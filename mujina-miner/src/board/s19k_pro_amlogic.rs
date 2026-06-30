//! S19k Pro support on a native Antminer Amlogic control board.
//!
//! This first implementation brings up one configured hashboard using the
//! native Linux interfaces proven in `amlogic-cb-tools`.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::Duration,
};

use amlogic_cb_tools::{
    eeprom_antminer::decode_antminer_eeprom,
    gpio::SysfsGpio,
    linux_i2c::LinuxI2cDevice,
    pic::{PicChain, pic_address_for_slot},
    protocol::{
        CMD_GET_VOLTAGE, CMD_MEASURE_VOLTAGE, CMD_SET_VOLTAGE, CMD_WATCHDOG, NAK_BYTE, build_frame,
        decode_dac_to_voltage, decode_measured_voltage, encode_voltage_to_dac, parse_frame,
    },
    pwm::SysfsPwm,
    tach::SysfsTachometer,
};
use async_trait::async_trait;
use tokio::sync::{Mutex, watch};
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::sync::CancellationToken;

use super::{Board, BoardError, BoardInfo, VirtualBoardDescriptor};
use crate::{
    api_client::types::{BoardState, Fan, PowerMeasurement, TemperatureSensor},
    asic::{
        bm13xx::{
            self,
            chain_config::{ChainConfig, ChainPeripherals, VoltageRegulator},
            chip_config, thread_v2,
            topology::TopologySpec,
        },
        hash_thread::{
            AsicEnable, HashTask, HashThread, HashThreadCapabilities, HashThreadError,
            HashThreadEvent, HashThreadStatus,
        },
    },
    config::{AmlogicControlBoardConfig, AmlogicHashboardConfig},
    error::Error,
    tracing::prelude::*,
    transport::serial::SerialStream,
};

/// Adapter that lets the BM13xx hash thread retune the controller-side
/// chip UART by closing+reopening `/dev/ttyS2`, matching the LuxOS
/// behaviour observed in `captures/luxos-bhb56902-chain-init.log`
/// (three separate `open64()` calls on that path during init). The
/// Amlogic `meson_uart` driver does not switch baud cleanly mid-stream
/// via `tcsetattr` at 3 Mbaud, so the reopen path is the only reliable
/// way to get the chain to stay synced.
struct SerialControlAdapter {
    /// Device node path so we can re-open after each baud switch.
    path: std::path::PathBuf,
    /// Control side of the staged stream from the last
    /// `prepare_new_stream` call. Held here so `finalize_baud_switch`
    /// can retune it, and so the new fd stays open across the actor's
    /// chip_tx swap (the kernel never sees `/dev/ttyS2` unclaimed,
    /// matching LuxOS's "open before drop" pattern observed in
    /// `captures/luxos-bhb56902-full-mining.log`).
    staged_control: Option<crate::transport::serial::SerialControl>,
    /// Keep-alive handle on the ORIGINAL `/dev/ttyS2` fd from board
    /// init. LuxOS leaves its initial fd open for the entire mining
    /// session — `captures/luxos-bhb56902-steady-state.log` shows
    /// `OPEN64 fd=25` near t=0 followed by `OPEN64 fd=16` for each
    /// later baud switch, but fd=25 itself never appears in a close
    /// sequence. With nothing keeping the original fd alive, mujina
    /// was hitting a brief no-fd window when the actor swapped
    /// readers/writers, and the meson_uart driver glitched chips off
    /// the chain.
    _original_keepalive: Option<crate::transport::serial::SerialControl>,
}

#[async_trait::async_trait]
impl bm13xx::chain_config::ChipUartBaudControl for SerialControlAdapter {
    async fn prepare_new_stream(
        &mut self,
        current_baud_rate: u32,
    ) -> anyhow::Result<(
        bm13xx::chain_config::ChipRxStream,
        bm13xx::chain_config::ChipTxSink,
    )> {
        let path = self.path.to_string_lossy().into_owned();
        // Open at the SAME baud the chain is currently using so the
        // kernel `tcsetattr` driven by `SerialStream::new` is a no-op
        // for the device — both the existing and the new fd stay at
        // the current bit rate, the in-flight broadcast can complete,
        // and chips actually receive it.
        let stream = SerialStream::new(&path, current_baud_rate).map_err(|e| {
            anyhow::anyhow!("SerialStream::new({path}, {current_baud_rate}): {e}")
        })?;
        let (reader, writer, control) = stream.split();
        control
            .flush_input()
            .map_err(|e| anyhow::anyhow!("SerialControl::flush_input: {e}"))?;
        self.staged_control = Some(control);
        let chip_rx = FramedRead::new(reader, bm13xx::FrameCodec);
        let chip_tx = FramedWrite::new(writer, bm13xx::FrameCodec);
        Ok((Box::pin(chip_rx), Box::pin(chip_tx)))
    }

    async fn finalize_baud_switch(
        &mut self,
        target_baud_rate: u32,
    ) -> anyhow::Result<()> {
        let control = self
            .staged_control
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("finalize_baud_switch called without prepare_new_stream"))?;
        control
            .set_baud_rate(target_baud_rate)
            .map_err(|e| anyhow::anyhow!("SerialControl::set_baud_rate({target_baud_rate}): {e}"))
    }
}

const BOARD_MODEL: &str = "S19k Pro (Amlogic control board)";
const DEFAULT_BOARD_NAME: &str = "s19kpro-amlogic";
const FAN_PWM_PERIOD_NS: u32 = 10_000;
const SERIAL_BAUD: u32 = 115_200;
const PSU_RESPONSE_DELAY_MS: u64 = 500;
const PSU_MAX_RESPONSE_ATTEMPTS: usize = 3;
const EEPROM_LEN: usize = 256;
const TMP75_TEMP_REG: u8 = 0x00;

static AMLOGIC_BOARD_CONFIG: OnceLock<AmlogicControlBoardConfig> = OnceLock::new();

/// Install the config used by the native Amlogic virtual board factory.
pub fn install_config(config: AmlogicControlBoardConfig) -> crate::error::Result<()> {
    AMLOGIC_BOARD_CONFIG
        .set(config)
        .map_err(|_| Error::Config("Amlogic control-board config already initialized".into()))
}

/// Derive a stable device identifier for the configured control board.
pub fn device_id(config: &AmlogicControlBoardConfig) -> String {
    config
        .board_name
        .clone()
        .unwrap_or_else(|| DEFAULT_BOARD_NAME.to_string())
}

/// One hashboard, after `select_present_hashboards` + `perform_health_gate`.
#[derive(Clone)]
struct SelectedHashboard {
    config: AmlogicHashboardConfig,
    board_serial: Option<String>,
}

/// Native Amlogic S19k Pro board.
///
/// Holds one or more `SelectedHashboard`s — the BHB56902 hardware
/// supports up to three hashboards on the same APW12 / Amlogic SoC,
/// and `select_present_hashboards` picks every slot whose detect GPIO
/// reads "present". `create_hash_threads` then spawns one
/// [`BoardStateHashThread`] per board so each chain mines
/// independently; the shared APW12 is coordinated through a
/// [`bm13xx::chain_config::ChainCoordinator`] so the per-step voltage
/// commands during the frequency ramp don't oscillate across chains.
pub struct S19kProAmlogic {
    config: AmlogicControlBoardConfig,
    selected_hashboards: Vec<SelectedHashboard>,
    psu: Arc<Mutex<NativeAmlogicPsu>>,
    state_tx: watch::Sender<BoardState>,
    thread_states: Arc<std::sync::Mutex<Vec<crate::api_client::types::ThreadState>>>,
    telemetry_shutdown: CancellationToken,
}

impl S19kProAmlogic {
    fn new(
        config: AmlogicControlBoardConfig,
        selected_hashboards: Vec<SelectedHashboard>,
        psu: Arc<Mutex<NativeAmlogicPsu>>,
        state_tx: watch::Sender<BoardState>,
    ) -> Self {
        Self {
            config,
            selected_hashboards,
            psu,
            state_tx,
            thread_states: Arc::new(std::sync::Mutex::new(Vec::new())),
            telemetry_shutdown: CancellationToken::new(),
        }
    }

    async fn initialize(
        config: &AmlogicControlBoardConfig,
        state_tx: &watch::Sender<BoardState>,
    ) -> Result<(Vec<SelectedHashboard>, Arc<Mutex<NativeAmlogicPsu>>), BoardError> {
        let board_name = device_id(config);
        let present = select_present_hashboards(config)?;

        info!(
            board = %board_name,
            slots = %present
                .iter()
                .map(|hb| hb.index.to_string())
                .collect::<Vec<_>>()
                .join(","),
            "Initializing native Amlogic S19k Pro board"
        );

        // Health-gate every present hashboard before energizing chips.
        // EEPROM + temperature reads can fail individually; a failure
        // here aborts board init for now (we don't want a "ghost"
        // hashboard with bad EEPROM coming up with the others).
        let mut selected: Vec<SelectedHashboard> = Vec::with_capacity(present.len());
        let mut all_temps: Vec<TemperatureSensor> = Vec::new();
        for hb in &present {
            let (board_serial, initial_temperatures) = perform_health_gate(config, hb)?;
            all_temps.extend(initial_temperatures);
            selected.push(SelectedHashboard {
                config: hb.clone(),
                board_serial,
            });
        }

        configure_fans(config, config.startup.default_fan_percent)?;
        assert_all_resets(config)?;

        let psu = Arc::new(Mutex::new(NativeAmlogicPsu::new(config)));
        let measured_voltage = {
            let mut psu_guard = psu.lock().await;
            psu_guard
                .set_enabled(true)
                .map_err(|e| BoardError::HardwareControl(format!("Failed to enable PSU: {e}")))?;
            if let Err(e) = psu_guard.config_watchdog(0x00) {
                warn!(
                    "PSU watchdog disable rejected (firmware variant?), continuing: {}",
                    e
                );
            }
            psu_guard
                .set_voltage(config.startup.initial_voltage)
                .await
                .map_err(|e| {
                    BoardError::HardwareControl(format!("Failed to set PSU voltage: {e}"))
                })?;

            tokio::time::sleep(Duration::from_millis(config.startup.psu_settle_ms)).await;
            psu_guard.measure_voltage().ok()
        };

        // PIC handshake — best-effort per present hashboard. On
        // PIC-variant boards (BHB42601 / BHB42611) the per-domain
        // DC-DC regulators are gated by an on-hashboard PIC16F1704
        // microcontroller and have to be unlocked here. On noPIC
        // variants (BHB56902 / S19k Pro family) the open or
        // handshake fails and we continue — the chain still comes up
        // via the existing path.
        //
        // See "PIC vs noPIC Bitmain Miners":
        //   https://braiins.com/blog/pic-vs-nopic-bitmain-miners-...
        // The protocol opcodes used by `PicChain` were lifted from
        // the decompiled S21 single_board_test in
        //   https://github.com/HashSource/bitmain_antminer_binaries
        // and confirmed against LuxOS ftrace captures on BHB42601.
        for sel in &selected {
            let pic_addr = pic_address_for_slot(sel.config.index);
            match PicChain::open(&sel.config.eeprom_i2c_device, pic_addr) {
                Ok(mut pic) => match pic.handshake() {
                    Ok(version) => {
                        info!(
                            hashboard = sel.config.index,
                            addr = format_args!("0x{:02x}", pic_addr),
                            version = format_args!("0x{:02x}", version),
                            "PIC handshake ok"
                        );
                        if let Err(e) = pic.enable_dc_dc() {
                            warn!(
                                hashboard = sel.config.index,
                                addr = format_args!("0x{:02x}", pic_addr),
                                error = %e,
                                "PIC enable_dc_dc failed; chips may not power up"
                            );
                        } else {
                            info!(
                                hashboard = sel.config.index,
                                addr = format_args!("0x{:02x}", pic_addr),
                                "PIC DC-DC enabled; chips powering up"
                            );
                        }
                    }
                    Err(e) => {
                        debug!(
                            hashboard = sel.config.index,
                            addr = format_args!("0x{:02x}", pic_addr),
                            error = %e,
                            "PIC handshake failed (noPIC variant?); continuing"
                        );
                    }
                },
                Err(e) => {
                    debug!(
                        hashboard = sel.config.index,
                        addr = format_args!("0x{:02x}", pic_addr),
                        error = %e,
                        "PIC i2c open failed (noPIC variant?); continuing"
                    );
                }
            }
        }

        // First board's serial gets reported as the board serial.
        // Could expose all serials in the future, but the API model
        // currently has one serial per BoardState.
        let primary_serial = selected
            .iter()
            .find_map(|s| s.board_serial.clone())
            .or_else(|| Some(board_name.clone()));

        let fan_states = build_fan_state(config, config.startup.default_fan_percent);
        let power_states = vec![PowerMeasurement {
            name: "apw12".into(),
            voltage_v: measured_voltage,
            current_a: None,
            power_w: None,
        }];

        state_tx.send_modify(|state| {
            state.name = board_name.clone();
            state.model = BOARD_MODEL.into();
            state.serial = primary_serial.clone();
            state.temperatures = all_temps.clone();
            state.fans = fan_states.clone();
            state.powers = power_states.clone();
        });

        Ok((selected, psu))
    }
}

#[async_trait]
impl Board for S19kProAmlogic {
    fn board_info(&self) -> BoardInfo {
        BoardInfo {
            model: BOARD_MODEL.into(),
            firmware_version: None,
            serial_number: self
                .selected_hashboards
                .iter()
                .find_map(|s| s.board_serial.clone())
                .or_else(|| Some(device_id(&self.config))),
        }
    }

    async fn shutdown(&mut self) -> Result<(), BoardError> {
        info!(board = %device_id(&self.config), "Shutting down native Amlogic board");

        self.telemetry_shutdown.cancel();

        assert_all_resets(&self.config)?;
        configure_fans(&self.config, 0)?;

        // Best-effort disable PIC DC-DC on every present hashboard
        // before cutting the rail. PSU output-off below still safes
        // the chips even if some PIC opens fail (noPIC variants).
        for sel in &self.selected_hashboards {
            let pic_addr = pic_address_for_slot(sel.config.index);
            if let Ok(mut pic) = PicChain::open(&sel.config.eeprom_i2c_device, pic_addr) {
                if let Err(e) = pic.disable_dc_dc() {
                    debug!(
                        hashboard = sel.config.index,
                        addr = format_args!("0x{:02x}", pic_addr),
                        error = %e,
                        "PIC disable_dc_dc on shutdown failed (non-fatal)"
                    );
                }
            }
        }

        self.psu
            .lock()
            .await
            .set_enabled(false)
            .map_err(|e| BoardError::HardwareControl(format!("Failed to disable PSU: {e}")))?;

        Ok(())
    }

    async fn create_hash_threads(&mut self) -> Result<Vec<Box<dyn HashThread>>, BoardError> {
        let n_chains = self.selected_hashboards.len();
        // Single APW12 across all hashboards. Lockstep voltage commands
        // through a shared coordinator when there's more than one chain,
        // otherwise leave the legacy path where each actor drives its
        // own regulator independently.
        let ramp_coordinator = if n_chains > 1 {
            Some(Arc::new(bm13xx::chain_config::ChainCoordinator::new(
                n_chains,
            )))
        } else {
            None
        };

        let mut threads: Vec<Box<dyn HashThread>> = Vec::with_capacity(n_chains);
        let mut thread_state_seed: Vec<crate::api_client::types::ThreadState> =
            Vec::with_capacity(n_chains);

        for selected in self.selected_hashboards.clone() {
            let hb = &selected.config;

            let data_stream = SerialStream::new(&hb.serial_path.to_string_lossy(), SERIAL_BAUD)
                .map_err(|e| {
                    BoardError::InitializationFailed(format!(
                        "Failed to open data port {}: {e}",
                        hb.serial_path.display()
                    ))
                })?;
            let (data_reader, data_writer, data_control) = data_stream.split();

            data_control.flush_input().map_err(|e| {
                BoardError::InitializationFailed(format!("Failed to flush serial buffer: {e}"))
            })?;

            let chip_rx = FramedRead::new(data_reader, bm13xx::FrameCodec);
            let chip_tx = FramedWrite::new(data_writer, bm13xx::FrameCodec);

            // Original SerialControl held alive in the adapter so the
            // `/dev/ttyS*` fd never closes mid-init when the actor
            // swaps writer/reader pairs across a baud bump. Per-
            // hashboard adapter so each chain owns its own keepalive.
            let chip_uart_baud = Arc::new(Mutex::new(SerialControlAdapter {
                path: hb.serial_path.clone(),
                staged_control: None,
                _original_keepalive: Some(data_control),
            }));

            let config = ChainConfig {
                name: format!("S19kProAmlogic-HB{}", hb.index),
                topology: TopologySpec::uniform_domains(11, 7, false),
                chip_config: chip_config::bm1366(),
                peripherals: ChainPeripherals {
                    asic_enable: Arc::new(Mutex::new(NativeResetControl {
                        gpio: SysfsGpio::new(hb.reset_gpio),
                        reset_release_ms: self.config.startup.reset_release_ms,
                    })),
                    voltage_regulator: Some(
                        Arc::clone(&self.psu) as Arc<Mutex<dyn VoltageRegulator + Send>>
                    ),
                    chip_uart_baud: Some(chip_uart_baud
                        as Arc<Mutex<dyn bm13xx::chain_config::ChipUartBaudControl + Send>>),
                    ramp_coordinator: ramp_coordinator.clone(),
                },
                // 3.125 Mbaud post-broadcast switch — see commit
                // history for the chip-side register / numeric-rate
                // and fd-keepalive fixes that made this reliable.
                post_broadcast_chip_baud: Some(bm13xx::protocol::BaudRate::Baud3M),
            };

            let inner_thread =
                thread_v2::BM13xxThread::new(chip_rx, chip_tx, config).map_err(|e| {
                    BoardError::InitializationFailed(format!(
                        "Failed to create hash thread for HB{}: {e}",
                        hb.index
                    ))
                })?;

            let thread_name = inner_thread.name().to_string();
            thread_state_seed.push(crate::api_client::types::ThreadState {
                name: thread_name.clone(),
                hashrate: 0,
                is_active: false,
            });

            let board_thread = BoardStateHashThread::new(
                Box::new(inner_thread),
                self.state_tx.clone(),
                Arc::clone(&self.thread_states),
                Arc::clone(&self.psu),
                self.config.startup.psu_settle_ms,
                hb.reset_gpio,
                self.config.startup.reset_assert_ms,
                self.config.startup.initial_voltage,
                hb.eeprom_i2c_device.clone(),
                hb.index,
            );

            threads.push(Box::new(board_thread));
        }

        // Seed both the BoardState.threads field (for the UI) and the
        // shared `thread_states` cache (read by the consolidated
        // telemetry task) with one slot per chain. Live hashrates
        // arrive via `sync_thread_state` as each actor mines.
        self.state_tx.send_modify(|state| {
            state.serial = self
                .selected_hashboards
                .iter()
                .find_map(|s| s.board_serial.clone())
                .or_else(|| Some(device_id(&self.config)));
            state.threads = thread_state_seed.clone();
        });
        {
            let mut thread_states = self
                .thread_states
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *thread_states = thread_state_seed.clone();
        }

        // ONE telemetry task across all selected hashboards. It walks
        // every present hashboard each tick, aggregates temperatures /
        // fans / threads / PSU voltage into a single BoardState
        // update, and acts as the overtemp gate (kills PSU if any
        // sensor exceeds the cutoff). Spawning N independent tasks
        // would have them race on `state_tx.send_modify` and clobber
        // each other's temperatures.
        let cfg_clone = self.config.clone();
        let hbs_clone: Vec<AmlogicHashboardConfig> = self
            .selected_hashboards
            .iter()
            .map(|s| s.config.clone())
            .collect();
        let psu = Arc::clone(&self.psu);
        let state_tx = self.state_tx.clone();
        let thread_states = Arc::clone(&self.thread_states);
        let shutdown = self.telemetry_shutdown.child_token();
        tokio::spawn(async move {
            native_telemetry_task(cfg_clone, hbs_clone, psu, state_tx, thread_states, shutdown)
                .await;
        });

        Ok(threads)
    }
}

struct BoardStateHashThread {
    inner: Box<dyn HashThread>,
    state_tx: watch::Sender<BoardState>,
    thread_states: Arc<std::sync::Mutex<Vec<crate::api_client::types::ThreadState>>>,
    /// PSU rail to drop on hard pause and re-energize on resume.
    /// Shared with `native_telemetry_task` (which reads voltage and is
    /// the other safety-cutoff lever); guarded by the same async mutex
    /// so a pause and an overtemp cutoff serialize cleanly.
    psu: Arc<Mutex<NativeAmlogicPsu>>,
    /// Wait between `psu.set_enabled(true)` and the inner thread's
    /// resume so the APW12 has time to stabilize its output before the
    /// next UpdateTask hits and triggers the cold init / freq ramp.
    psu_settle_ms: u64,
    /// Reset GPIO for the active hashboard. Driven low on resume
    /// *before* the PSU comes back up so chips power up while held in
    /// reset (matching the cold-boot `assert_all_resets()` step in
    /// `initialize()`), and released only when the actor's
    /// `initialize_chips()` runs.
    reset_gpio: u32,
    /// Hold time after asserting reset and before bringing the PSU
    /// back up. Without this, RST_N is asserted only nanoseconds
    /// before the rail comes alive and the daisy-chained signal does
    /// not propagate to all 77 chips in time, leaving a random ~30/77
    /// in indeterminate state. Mirrors the `reset_assert_ms` sleep
    /// inside cold-boot `assert_all_resets()`.
    reset_assert_ms: u64,
    /// Voltage to command on resume *before* PSU enable, so chips
    /// power up at the same level as a cold boot (12 V) rather than
    /// the last operating voltage (13.9 V) which the APW12 retains
    /// across enable cycles. The frequency ramp brings it back up.
    initial_voltage_v: f32,
    /// i2c device path and PIC address used to re-handshake the on-
    /// hashboard PIC16F1704 on resume. The PIC's LDO is fed from the
    /// same 12 V rail we drop on pause, so it loses state and the
    /// per-domain DC-DCs come back disabled. Without re-running
    /// handshake → enable_dc_dc, only chips on domains that happen to
    /// power up on their own respond on UART (scattered ~30/77).
    pic_i2c_device: PathBuf,
    pic_slot_index: u8,
}

impl BoardStateHashThread {
    fn new(
        inner: Box<dyn HashThread>,
        state_tx: watch::Sender<BoardState>,
        thread_states: Arc<std::sync::Mutex<Vec<crate::api_client::types::ThreadState>>>,
        psu: Arc<Mutex<NativeAmlogicPsu>>,
        psu_settle_ms: u64,
        reset_gpio: u32,
        reset_assert_ms: u64,
        initial_voltage_v: f32,
        pic_i2c_device: PathBuf,
        pic_slot_index: u8,
    ) -> Self {
        Self {
            inner,
            state_tx,
            thread_states,
            psu,
            psu_settle_ms,
            reset_gpio,
            reset_assert_ms,
            initial_voltage_v,
            pic_i2c_device,
            pic_slot_index,
        }
    }

    fn sync_thread_state(&self, is_active_override: Option<bool>) {
        let status = self.inner.status();
        let capabilities = self.inner.capabilities();
        let hashrate = thread_hashrate_value(&status, capabilities);
        let name = self.inner.name().to_string();
        let is_active = is_active_override.unwrap_or(status.is_active);
        let thread_state = crate::api_client::types::ThreadState {
            name: name.clone(),
            hashrate,
            is_active,
        };

        {
            let mut thread_states = self
                .thread_states
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(existing) = thread_states.iter_mut().find(|thread| thread.name == name) {
                *existing = thread_state.clone();
            } else {
                thread_states.push(thread_state.clone());
            }
        }

        self.state_tx.send_modify(|state| {
            if let Some(existing) = state.threads.iter_mut().find(|thread| thread.name == name) {
                *existing = thread_state.clone();
            } else {
                state.threads.push(thread_state);
            }
        });
    }
}

fn thread_hashrate_value(status: &HashThreadStatus, _capabilities: &HashThreadCapabilities) -> u64 {
    // Report the actual measured hashrate, including 0 when the thread
    // hasn't accepted any shares yet (init, frequency ramp, pause).
    // The previous fallback to `capabilities.hashrate_estimate` made
    // the per-board UI show a static 6.39 TH/s during ramp-up while
    // the chain-wide hashrate was still 0 — confusing.
    u64::from(status.hashrate)
}

#[async_trait]
impl HashThread for BoardStateHashThread {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn capabilities(&self) -> &HashThreadCapabilities {
        self.inner.capabilities()
    }

    async fn update_task(
        &mut self,
        new_task: HashTask,
    ) -> Result<Option<HashTask>, HashThreadError> {
        let result = self.inner.update_task(new_task).await;
        self.sync_thread_state(Some(result.is_ok()));
        result
    }

    async fn replace_task(
        &mut self,
        new_task: HashTask,
    ) -> Result<Option<HashTask>, HashThreadError> {
        let result = self.inner.replace_task(new_task).await;
        self.sync_thread_state(Some(result.is_ok()));
        result
    }

    async fn go_idle(&mut self) -> Result<Option<HashTask>, HashThreadError> {
        let result = self.inner.go_idle().await;
        self.sync_thread_state(Some(false));
        result
    }

    async fn shutdown(&mut self) -> Result<(), HashThreadError> {
        let result = self.inner.shutdown().await;
        self.sync_thread_state(Some(false));
        result
    }

    async fn set_frequency(&mut self, mhz: f32) -> Result<(), HashThreadError> {
        // Pure forward — the runtime re-ramp is PLL-only at fixed voltage,
        // so the board wrapper has nothing to coordinate (unlike pause, which
        // also cycles the PSU rail).
        self.inner.set_frequency(mhz).await
    }

    async fn set_paused(&mut self, paused: bool) -> Result<(), HashThreadError> {
        // Hard pause: drop the chip power rail. The chain comes back
        // cold-booted on resume, which is the same path `start_async`
        // uses at process startup and is known to work — unlike the
        // UART `disable_chips()` path that gets only ~37/77 chips
        // back on BHB56902 after re-enumeration.
        //
        // Order matters:
        //   - on pause:  zero the inner thread first (so the per-board
        //                UI publishes 0 TH/s before chips lose power
        //                and the chain goes quiet), THEN drop PSU.
        //   - on resume: bring PSU up + settle BEFORE the inner thread
        //                comes out of paused state, so the next
        //                UpdateTask that arrives can immediately drive
        //                `initialize_chips()` against a powered chain.
        if paused {
            let result = self.inner.set_paused(true).await;
            self.sync_thread_state(Some(false));
            // Drive RST_N LOW BEFORE dropping the rail. Chips were
            // running with reset released; if we kill power while
            // they're still in a free-running state, internal logic
            // can land in indeterminate flip-flop states that survive
            // a brief power dip — exactly the failure mode the legacy
            // disable_chips() path hit (~37/77 chips back, comment
            // at thread_v2.rs:1255-1263). Driving reset low first puts
            // chips into a known synchronous reset state before the
            // rail falls.
            if let Err(e) = SysfsGpio::new(self.reset_gpio).set_output_low() {
                warn!(error = %e, reset_gpio = self.reset_gpio, "Failed to assert reset before PSU drop; chain may not pause cleanly");
            }
            // Give the reset edge time to be sampled by every chip in
            // the daisy-chain before the rail collapses.
            tokio::time::sleep(Duration::from_millis(self.reset_assert_ms)).await;
            if let Err(e) = self.psu.lock().await.set_enabled(false) {
                warn!(error = %e, "set_paused(true) succeeded on chain but PSU disable failed; chips still powered");
            } else {
                info!(
                    thread = %self.inner.name(),
                    "Hard pause: reset asserted then PSU output disabled — chips drained, board will cool"
                );
            }
            result
        } else {
            // Hard resume — mirror the cold-boot sequence in
            // `S19kProAmlogic::initialize`:
            //
            //   1. Assert hashboard reset BEFORE the PSU comes on.
            //      Chips power up while held in reset. The previous
            //      version didn't do this; chips powered up with reset
            //      already released and ended up in indeterminate state.
            //      Verify_chain then saw a random ~30/77 each pass.
            //   2. Drop the PSU voltage back to the cold-boot setpoint
            //      (12 V). The APW12 retains the last commanded
            //      voltage across an enable/disable cycle, so without
            //      this re-enable comes back at 13.9 V — too hot a
            //      start for cold chips, plus the frequency ramp
            //      expects to begin at the low rail anyway.
            //   3. Enable the PSU. Chips now have power, are at 12 V,
            //      and are held in reset by step 1.
            //   4. Sleep psu_settle_ms so the APW12 stabilizes.
            //   5. Resume the inner thread. Its next UpdateTask
            //      triggers `initialize_chips()`, which calls
            //      `asic_enable.enable()` to release reset and then
            //      runs enumeration — exactly the cold-boot path.
            if let Err(e) = SysfsGpio::new(self.reset_gpio).set_output_low() {
                warn!(error = %e, reset_gpio = self.reset_gpio, "Failed to assert reset on resume; chain may enumerate poorly");
            }
            // Hold reset asserted before PSU comes up. Without this
            // the RST_N edge happens microseconds before the rail
            // appears and does not propagate to all 77 daisy-chained
            // chips, leaving a random ~30/77 in indeterminate state.
            tokio::time::sleep(Duration::from_millis(self.reset_assert_ms)).await;
            {
                let mut psu = self.psu.lock().await;
                if let Err(e) = psu.set_voltage(self.initial_voltage_v).await {
                    warn!(error = %e, "Failed to reset PSU voltage to initial setpoint on resume");
                }
                if let Err(e) = psu.set_enabled(true) {
                    warn!(error = %e, "PSU re-enable failed on resume; chain will not respond");
                    return Err(HashThreadError::InitializationFailed(format!(
                        "PSU re-enable failed: {e}"
                    )));
                }
            }
            tokio::time::sleep(Duration::from_millis(self.psu_settle_ms)).await;

            // PIC handshake — BHB56902 has a PIC16F1704 that gates the
            // per-domain DC-DCs and is fed from the same 12 V rail we
            // just dropped. Without re-running handshake →
            // enable_dc_dc, only chips on the handful of domains that
            // happen to come up power-on survive (verified: scattered
            // ~30/77). Mirror the cold-boot path in `initialize`.
            //
            // Best-effort: any failure here gets a WARN like cold boot;
            // the next initialize_chips() will still try to enumerate
            // (and will likely fail visibly, which is the right signal).
            let pic_addr = pic_address_for_slot(self.pic_slot_index);
            match PicChain::open(&self.pic_i2c_device, pic_addr) {
                Ok(mut pic) => match pic.handshake() {
                    Ok(version) => {
                        info!(
                            addr = format_args!("0x{:02x}", pic_addr),
                            version = format_args!("0x{:02x}", version),
                            "PIC handshake ok on resume"
                        );
                        if let Err(e) = pic.enable_dc_dc() {
                            warn!(
                                addr = format_args!("0x{:02x}", pic_addr),
                                error = %e,
                                "PIC enable_dc_dc failed on resume; chips may not power up"
                            );
                        }
                    }
                    Err(e) => {
                        warn!(
                            addr = format_args!("0x{:02x}", pic_addr),
                            error = %e,
                            "PIC handshake failed on resume; chain may not respond on UART"
                        );
                    }
                },
                Err(e) => {
                    debug!(
                        addr = format_args!("0x{:02x}", pic_addr),
                        error = %e,
                        "PIC absent on resume (noPIC variant?)"
                    );
                }
            }

            info!(
                thread = %self.inner.name(),
                settle_ms = self.psu_settle_ms,
                initial_voltage_v = self.initial_voltage_v,
                reset_gpio = self.reset_gpio,
                "Hard resume: reset asserted, PSU back on at initial voltage, PIC re-handshaked; next UpdateTask will release reset and enumerate"
            );
            self.inner.set_paused(false).await
        }
    }

    fn take_event_receiver(&mut self) -> Option<tokio::sync::mpsc::Receiver<HashThreadEvent>> {
        self.inner.take_event_receiver()
    }

    fn status(&self) -> HashThreadStatus {
        self.inner.status()
    }
}

#[derive(Clone)]
struct NativeResetControl {
    gpio: SysfsGpio,
    reset_release_ms: u64,
}

#[async_trait]
impl AsicEnable for NativeResetControl {
    async fn enable(&mut self) -> anyhow::Result<()> {
        self.gpio.set_output_high()?;
        tokio::time::sleep(Duration::from_millis(self.reset_release_ms)).await;
        Ok(())
    }

    async fn disable(&mut self) -> anyhow::Result<()> {
        self.gpio.set_output_low()?;
        Ok(())
    }
}

#[derive(Clone)]
struct NativeAmlogicPsu {
    i2c_device: PathBuf,
    address: u16,
    write_register: u8,
    enable_gpio: u32,
    enabled: bool,
}

impl NativeAmlogicPsu {
    fn new(config: &AmlogicControlBoardConfig) -> Self {
        Self {
            i2c_device: config.psu.i2c_device.clone(),
            address: config.psu.address,
            write_register: config.psu.write_register,
            enable_gpio: config.psu.enable_gpio,
            enabled: false,
        }
    }

    fn set_enabled(&mut self, enabled: bool) -> anyhow::Result<()> {
        let gpio = SysfsGpio::new(self.enable_gpio);
        if enabled {
            gpio.set_output_low()?;
        } else {
            gpio.set_output_high()?;
        }
        self.enabled = enabled;
        Ok(())
    }

    fn config_watchdog(&mut self, value: u8) -> anyhow::Result<()> {
        self.exchange(CMD_WATCHDOG, &[value, 0x00])?;
        Ok(())
    }

    fn measure_voltage(&mut self) -> anyhow::Result<f32> {
        let frame = self.exchange(CMD_MEASURE_VOLTAGE, &[])?;
        if frame.payload.len() < 2 {
            return Err(anyhow::anyhow!("missing ADC payload from PSU"));
        }
        Ok(decode_measured_voltage(frame.payload[0], frame.payload[1]))
    }

    fn read_target_voltage(&mut self) -> anyhow::Result<f32> {
        let frame = self.exchange(CMD_GET_VOLTAGE, &[])?;
        let dac = *frame
            .payload
            .first()
            .ok_or_else(|| anyhow::anyhow!("missing DAC payload from PSU"))?;
        Ok(decode_dac_to_voltage(dac))
    }

    fn exchange(
        &mut self,
        command: u8,
        payload: &[u8],
    ) -> anyhow::Result<amlogic_cb_tools::protocol::Frame> {
        let mut dev = LinuxI2cDevice::open(&self.i2c_device, self.address)?;
        let frame = build_frame(command, payload);
        for byte in frame {
            dev.write_byte_transaction(self.write_register, byte)?;
        }

        std::thread::sleep(Duration::from_millis(PSU_RESPONSE_DELAY_MS));

        let mut last_error = None;
        for _ in 0..PSU_MAX_RESPONSE_ATTEMPTS {
            match read_psu_response_frame(&mut dev) {
                Ok(response) if response == [NAK_BYTE] => {
                    last_error = Some(anyhow::anyhow!("PSU returned NAK"));
                }
                Ok(response) => match parse_frame(&response) {
                    Ok(frame) if frame.command == command => return Ok(frame),
                    Ok(frame) => {
                        last_error = Some(anyhow::anyhow!(
                            "unexpected PSU response command 0x{:02X} for request 0x{command:02X}",
                            frame.command
                        ));
                    }
                    Err(err) => {
                        last_error = Some(anyhow::Error::new(err));
                    }
                },
                Err(err) => {
                    last_error = Some(err);
                }
            }

            std::thread::sleep(Duration::from_millis(PSU_RESPONSE_DELAY_MS));
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("no valid PSU response received")))
    }
}

impl Drop for NativeAmlogicPsu {
    fn drop(&mut self) {
        if !self.enabled {
            return;
        }

        if let Err(error) = self.set_enabled(false) {
            error!(gpio = self.enable_gpio, error = %error, "Failed to disable PSU during drop");
        } else {
            warn!(gpio = self.enable_gpio, "Disabled PSU during drop");
        }
    }
}

#[async_trait]
impl VoltageRegulator for NativeAmlogicPsu {
    async fn set_voltage(&mut self, volts: f32) -> anyhow::Result<()> {
        let clamped = volts.clamp(12.0, 15.0);
        let dac = encode_voltage_to_dac(clamped);

        // The APW12 firmware on this control board (`get-fw` reports
        // 0x0010, `get-hw` 0x0071) intermittently NAKs response frames
        // even when the underlying DAC write succeeds — observed by doing
        // a `get-voltage` immediately after a NAK'd `set-voltage` and
        // seeing the requested DAC echoed back. Retry up to four times
        // and fall back to readback comparison; if the DAC reads back at
        // the requested voltage we treat the NAK as transient and accept.
        // Without this the chain ramp stalls on the first NAK because
        // bm13xx's chain init bubbles the error up and the scheduler
        // refuses to assign jobs.
        let mut last_err: Option<anyhow::Error> = None;
        for attempt in 0..4 {
            match self.exchange(CMD_SET_VOLTAGE, &[dac, 0x00]) {
                Ok(_) => {
                    if attempt > 0 {
                        warn!(
                            requested = clamped,
                            attempt,
                            "PSU set_voltage succeeded after retry"
                        );
                    }
                    return Ok(());
                }
                Err(err) => {
                    // Try a readback; if that succeeds and matches, the
                    // earlier write probably did land. If readback itself
                    // NAKs, retry the whole thing.
                    if let Ok(readback) = self.read_target_voltage() {
                        if (readback - clamped).abs() <= 0.15 {
                            warn!(
                                requested = clamped,
                                readback,
                                attempt,
                                error = %err,
                                "PSU accepted voltage by readback after transient response issue"
                            );
                            return Ok(());
                        }
                    }
                    last_err = Some(err);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("PSU set_voltage retries exhausted")))
    }

    fn voltage_range(&self) -> (f32, f32) {
        // BHB56902 factory ATE setpoint is 13.90 V / 645 MHz (Braiins'
        // `Detected hashboard #2: Voltage (Avg.) 13.90 V, Frequency
        // (Avg.) 645 MHz, Hashrate 44400.51 GH/s`).
        //
        // The min is set to the factory chain voltage. The shared
        // frequency-ramp voltage formula (`voltage_for_frequency_stacked`)
        // was tuned for BM1362 and returns ~0.3 V/chip; with 11
        // domains it would set 3.46 V across the chain, which is far
        // below what BM1366 needs to PLL-lock at 645 MHz. With a
        // 13.9 V floor the ramp clamps up to the factory setpoint
        // immediately, matching what LuxOS and Braiins do (they set
        // voltage to target *before* ramping frequency).
        //
        // verify_chain after ramp reports few chips alive at 13.9 V
        // (e.g. 16/77), but observed hashrate (~27 TH/s on the dummy
        // source vs ~14 TH/s at the 13.0 V floor with 67 reported)
        // shows that more chips are actually mining than the polled
        // verify can see at 3.125 Mbaud — the polled read is what's
        // flaky, not the chips themselves.
        (13.9, 14.5)
    }

    fn target_voltage(&self) -> f32 {
        // Factory-equivalent operating point, matching Braiins's
        // `Voltage(13.9)` for this hashboard. With 500 MHz frequency
        // mujina was at ~21 TH/s; at 645 MHz + 13.9 V Braiins gets
        // ~30 TH/s on the same chips.
        13.9
    }

    fn voltage_step(&self) -> f32 {
        0.1
    }
}

/// Walk every configured hashboard slot; return every one whose detect
/// GPIO reads "present". Missing required slots are fatal, missing
/// optional slots are skipped. The order matches the config order
/// (and therefore the slot index), so per-thread naming stays stable
/// across runs.
fn select_present_hashboards(
    config: &AmlogicControlBoardConfig,
) -> Result<Vec<AmlogicHashboardConfig>, BoardError> {
    if config.hashboards.is_empty() {
        return Err(BoardError::InitializationFailed(
            "Amlogic config has no configured hashboards".into(),
        ));
    }

    let mut present = Vec::new();
    for hashboard in &config.hashboards {
        let is_present = is_hashboard_present(hashboard)?;
        if !is_present {
            let missing_is_fatal = config
                .startup
                .health_gate
                .fail_on_missing_expected_hashboard
                || hashboard.required;
            if missing_is_fatal {
                return Err(BoardError::InitializationFailed(format!(
                    "Configured hashboard {} is missing",
                    hashboard.index
                )));
            }
            continue;
        }
        present.push(hashboard.clone());
    }

    if present.is_empty() {
        return Err(BoardError::InitializationFailed(
            "No configured hashboards are present".into(),
        ));
    }
    Ok(present)
}

fn is_hashboard_present(hashboard: &AmlogicHashboardConfig) -> Result<bool, BoardError> {
    let detect = SysfsGpio::new(hashboard.detect_gpio);
    detect.set_input_bias_disabled().map_err(|e| {
        BoardError::HardwareControl(format!("Failed to configure detect GPIO: {e}"))
    })?;
    let present = detect
        .read_value()
        .map_err(|e| BoardError::HardwareControl(format!("Failed to read detect GPIO: {e}")))?;
    Ok(present != 0)
}

fn perform_health_gate(
    config: &AmlogicControlBoardConfig,
    hashboard: &AmlogicHashboardConfig,
) -> Result<(Option<String>, Vec<TemperatureSensor>), BoardError> {
    let mut board_serial = None;

    if config.startup.health_gate.read_eeprom_before_mining {
        let eeprom = read_eeprom(hashboard)?;
        let decoded = decode_antminer_eeprom(&eeprom).map_err(|e| {
            BoardError::InitializationFailed(format!(
                "EEPROM health gate failed for hashboard {}: {e}",
                hashboard.index
            ))
        })?;
        board_serial = Some(decoded.board_serial);
    }

    let temperatures = if config.startup.health_gate.read_temperatures_before_mining {
        read_temperatures(hashboard)?
    } else {
        Vec::new()
    };

    if config.startup.health_gate.read_temperatures_before_mining {
        for sensor in &temperatures {
            if sensor.temperature_c.is_none() {
                return Err(BoardError::InitializationFailed(format!(
                    "Temperature health gate failed for {}",
                    sensor.name
                )));
            }
        }
    }

    Ok((board_serial, temperatures))
}

fn assert_all_resets(config: &AmlogicControlBoardConfig) -> Result<(), BoardError> {
    for hashboard in &config.hashboards {
        SysfsGpio::new(hashboard.reset_gpio)
            .set_output_low()
            .map_err(|e| {
                BoardError::HardwareControl(format!(
                    "Failed to assert reset for hashboard {}: {e}",
                    hashboard.index
                ))
            })?;
    }
    std::thread::sleep(Duration::from_millis(config.startup.reset_assert_ms));
    Ok(())
}

fn configure_fans(config: &AmlogicControlBoardConfig, percent: u8) -> Result<(), BoardError> {
    let mut configured_channels = HashSet::new();
    for fan in &config.fans {
        if configured_channels.insert((fan.pwm_chip, fan.pwm_channel)) {
            SysfsPwm::new(fan.pwm_chip, fan.pwm_channel)
                .configure_percent(FAN_PWM_PERIOD_NS, percent, true)
                .map_err(|e| {
                    BoardError::HardwareControl(format!(
                        "Failed to configure pwmchip{}/pwm{}: {e}",
                        fan.pwm_chip, fan.pwm_channel
                    ))
                })?;
        }
    }
    Ok(())
}

fn build_fan_state(config: &AmlogicControlBoardConfig, percent: u8) -> Vec<Fan> {
    config
        .fans
        .iter()
        .map(|fan| Fan {
            name: format!("fan{}", fan.index),
            rpm: None,
            percent: Some(percent),
            target_percent: Some(percent),
        })
        .collect()
}

async fn native_telemetry_task(
    config: AmlogicControlBoardConfig,
    hashboards: Vec<AmlogicHashboardConfig>,
    psu: Arc<Mutex<NativeAmlogicPsu>>,
    state_tx: watch::Sender<BoardState>,
    thread_states: Arc<std::sync::Mutex<Vec<crate::api_client::types::ThreadState>>>,
    shutdown: CancellationToken,
) {
    const TELEMETRY_INTERVAL: Duration = Duration::from_secs(2);

    // One PIC handle per present hashboard for the heartbeat path.
    // LuxOS sends a PIC heartbeat (opcode 0x16) roughly every 1.5 s on
    // PIC variants; without heartbeats the PIC disables DC-DC after a
    // watchdog timeout (chips drop mid-ramp). Probe each slot once at
    // task start; noPIC variants (BHB56902) silently disable the
    // heartbeat for that slot to avoid spamming warnings.
    let mut pics: Vec<(u16, AmlogicHashboardConfig, Option<PicChain>)> =
        Vec::with_capacity(hashboards.len());
    for hb in &hashboards {
        let pic_addr = pic_address_for_slot(hb.index);
        let pic = match PicChain::open(&hb.eeprom_i2c_device, pic_addr) {
            Ok(mut p) => match p.heartbeat() {
                Ok(()) => Some(p),
                Err(e) => {
                    info!(
                        hashboard = hb.index,
                        addr = format_args!("0x{:02x}", pic_addr),
                        error = %e,
                        "PIC absent on this hashboard (likely a noPIC variant); \
                         skipping heartbeat path"
                    );
                    None
                }
            },
            Err(e) => {
                warn!(
                    hashboard = hb.index,
                    addr = format_args!("0x{:02x}", pic_addr),
                    error = %e,
                    "could not open PIC for heartbeat task; chips may drop after watchdog timeout"
                );
                None
            }
        };
        pics.push((pic_addr, hb.clone(), pic));
    }

    // Tracks the last duty applied to the fans by the dynamic curve so we
    // only call configure_fans on actual changes. None means "never set
    // by this task yet" — the next iteration will issue the first write.
    let mut applied_fan_percent: Option<u8> = None;
    // Wall-clock of the last successful temperature read; the watchdog
    // below pins fans to 100 % once this gets stale. Initialize to NOW
    // so we tolerate the first read taking a little while before
    // declaring the sensor dead.
    let mut last_temp_at = std::time::Instant::now();

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        // Walk every present hashboard each tick: PIC heartbeat,
        // PIC-mediated temps, TMP75 fallback. Accumulate into one
        // sensor list keyed by `HB{index}-...` so the API surfaces
        // per-hashboard sensors in the same BoardState.
        let mut temperatures: Vec<TemperatureSensor> = Vec::new();
        for (pic_addr, hb, pic_opt) in pics.iter_mut() {
            let pic_addr = *pic_addr;
            let mut got_pic_temps = false;
            if let Some(pic) = pic_opt.as_mut() {
                if let Err(e) = pic.heartbeat() {
                    warn!(
                        hashboard = hb.index,
                        addr = format_args!("0x{:02x}", pic_addr),
                        error = %e,
                        "PIC heartbeat failed"
                    );
                }
                match pic.read_temperatures_celsius() {
                    Ok(temps) => {
                        for (i, t) in temps.iter().enumerate() {
                            temperatures.push(TemperatureSensor {
                                name: format!("HB{}-PIC{}", hb.index, i),
                                temperature_c: Some(*t),
                            });
                        }
                        got_pic_temps = !temps.is_empty();
                    }
                    Err(e) => {
                        debug!(
                            hashboard = hb.index,
                            addr = format_args!("0x{:02x}", pic_addr),
                            error = %e,
                            "PIC temperature read failed"
                        );
                    }
                }
            }
            if !got_pic_temps {
                match read_temperatures(hb) {
                    Ok(t) => temperatures.extend(t),
                    Err(error) => {
                        debug!(board = hb.index, error = %error, "TMP75 temperature read failed");
                    }
                }
            }
        }

        // ----- Fan + overtemp control --------------------------------
        //
        // The BHB56902 doesn't expose the BM1366's on-die thermal
        // diode the way a Bitaxe does (which routes it to an EMC2101
        // fan controller, then reads it over i2c — see
        // `bitaxeorg/ESP-Miner::main/thermal/EMC2101.{c,h}` and
        // `Thermal_get_chip_temp` in main/thermal/thermal.c). The only
        // signal we have on this hashboard is the pair of TMP75
        // sensors on the PCB, and they're poorly thermally coupled to
        // the chips — a TMP75 reading of 30 °C can sit alongside a
        // chip die at 80+ °C, which is exactly how two of our test
        // boards smoked before this code existed.
        //
        // Strategy until a better signal arrives:
        //
        //   1. Drive a dynamic fan curve off the hottest available
        //      sensor across every present hashboard — multi-board
        //      mode aggregates all TMP75 readings into `temperatures`,
        //      so the curve naturally tracks whichever board runs
        //      hottest. Floor at 60 % so the cooling can't drop to
        //      nothing while the curve is still cold.
        //   2. Hard cutoff at 65 °C TMP75 — sensor under-reads die by
        //      ~20 °C, so 65 here is ~85 °C actual die.
        //   3. Watchdog: if we haven't even READ a temperature in 30 s
        //      (i2c stuck, sensor died, telemetry task stalled and we
        //      just got back), pin fans to 100 % until we recover.
        //   4. The boot-time `default_fan_percent` from the toml is
        //      now 100 — the previous 50–60 % default was the
        //      open-loop value that ran during the entire pre-mining
        //      window with no temp signal at all.
        const TMP75_OVERTEMP_C: f32 = 65.0;
        const FAN_FLOOR_PERCENT: u8 = 60;
        const FAN_RAMP_START_C: f32 = 40.0;
        const FAN_RAMP_FULL_C: f32 = 60.0;
        const TEMP_STALE_AFTER: Duration = Duration::from_secs(30);

        let board_hottest = temperatures
            .iter()
            .filter_map(|t| t.temperature_c)
            .fold(None, |acc: Option<f32>, t| Some(acc.map_or(t, |a| a.max(t))));
        if board_hottest.is_some() {
            last_temp_at = std::time::Instant::now();
        }

        if let Some(t) = board_hottest
            && t >= TMP75_OVERTEMP_C
        {
            error!(
                hottest = t,
                cutoff = TMP75_OVERTEMP_C,
                "OVERTEMP — disabling all PIC DC-DC outputs and PSU output"
            );
            for (_, _, pic_opt) in pics.iter_mut() {
                if let Some(pic) = pic_opt.as_mut() {
                    let _ = pic.disable_dc_dc();
                }
            }
            let _ = psu.lock().await.set_enabled(false);
            shutdown.cancel();
            break;
        }

        let temp_stale = last_temp_at.elapsed() >= TEMP_STALE_AFTER;
        let target_fan_percent: u8 = if temp_stale {
            warn!(
                "No temperature sample in 30 s — pinning fans to 100 % as a safety fallback"
            );
            100
        } else if let Some(t) = board_hottest {
            if t <= FAN_RAMP_START_C {
                FAN_FLOOR_PERCENT
            } else if t >= FAN_RAMP_FULL_C {
                100
            } else {
                let span = FAN_RAMP_FULL_C - FAN_RAMP_START_C;
                let into_ramp = t - FAN_RAMP_START_C;
                let pct = FAN_FLOOR_PERCENT as f32
                    + ((100.0 - FAN_FLOOR_PERCENT as f32) * (into_ramp / span));
                pct.round().clamp(FAN_FLOOR_PERCENT as f32, 100.0) as u8
            }
        } else {
            // Pre-mining / first tick — sit at the boot value but
            // never below the floor.
            config.startup.default_fan_percent.max(FAN_FLOOR_PERCENT)
        };

        if Some(target_fan_percent) != applied_fan_percent {
            if let Err(e) = configure_fans(&config, target_fan_percent) {
                warn!(
                    target_percent = target_fan_percent,
                    error = %e,
                    "configure_fans failed; previous duty still applied"
                );
            } else {
                info!(
                    target_percent = target_fan_percent,
                    board_temp_c = board_hottest.unwrap_or(0.0),
                    "Adjusted fan PWM"
                );
                applied_fan_percent = Some(target_fan_percent);
            }
        }

        let fans = read_fan_states(&config, target_fan_percent).await;
        let voltage_v = match psu.lock().await.measure_voltage() {
            Ok(voltage_v) => Some(voltage_v),
            Err(error) => {
                debug!(error = %error, "Native telemetry PSU voltage read failed");
                None
            }
        };
        let powers = vec![PowerMeasurement {
            name: "apw12".into(),
            voltage_v,
            current_a: None,
            power_w: None,
        }];
        let threads = {
            thread_states
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        };

        state_tx.send_modify(|state| {
            state.temperatures = temperatures.clone();
            state.fans = fans.clone();
            state.powers = powers.clone();
            state.threads = threads.clone();
        });

        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(TELEMETRY_INTERVAL) => {}
        }
    }
}

async fn read_fan_states(config: &AmlogicControlBoardConfig, target_percent: u8) -> Vec<Fan> {
    const FAN_SAMPLE_WINDOW: Duration = Duration::from_millis(500);

    let mut fan_states = Vec::with_capacity(config.fans.len());
    for fan in &config.fans {
        let fan_name = format!("fan{}", fan.index);
        let tach_gpio = fan.tach_gpio;
        let pulses_per_rev = fan.pulses_per_rev;
        let rpm = match tokio::task::spawn_blocking(move || {
            SysfsTachometer::new(tach_gpio)
                .measure_rpm(FAN_SAMPLE_WINDOW, pulses_per_rev)
                .map(|reading| reading.rpm)
                .map_err(|error| error.to_string())
        })
        .await
        {
            Ok(Ok(rpm)) => Some(rpm),
            Ok(Err(error)) => {
                debug!(fan = %fan_name, gpio = tach_gpio, error = %error, "Native telemetry fan RPM read failed");
                None
            }
            Err(error) => {
                debug!(fan = %fan_name, gpio = tach_gpio, error = %error, "Native telemetry fan RPM task join failed");
                None
            }
        };

        fan_states.push(Fan {
            name: fan_name,
            rpm,
            percent: None,
            target_percent: Some(target_percent),
        });
    }

    fan_states
}

fn read_temperatures(
    hashboard: &AmlogicHashboardConfig,
) -> Result<Vec<TemperatureSensor>, BoardError> {
    // Try PIC-mediated temps first (PIC-variant boards: BHB56902 / S19k Pro
    // family, where asic_sensor_type=0 in EEPROM and pic_sensor_type != 0).
    // These can only be read while PSU output is ON because the PIC's LDO
    // is fed from the 12V rail; tolerate a failure here and fall back to
    // ASIC-bus TMP75.
    let mut sensors = Vec::new();
    let pic_addr = pic_address_for_slot(hashboard.index);
    if let Ok(mut pic) = PicChain::open(&hashboard.eeprom_i2c_device, pic_addr) {
        if let Ok(temps) = pic.read_temperatures_celsius() {
            for (i, t) in temps.iter().enumerate() {
                sensors.push(TemperatureSensor {
                    name: format!("HB{}-PIC{}", hashboard.index, i),
                    temperature_c: Some(*t),
                });
            }
            return Ok(sensors);
        }
    }

    // Fallback: TMP75 sensors on the ASIC bus (older / noPIC variants).
    let addresses = configured_tmp75_addresses(hashboard)?;
    for (sensor_index, address) in addresses.into_iter().enumerate() {
        let raw = read_tmp75_raw(&hashboard.temp_i2c_device, address).map_err(|e| {
            BoardError::InitializationFailed(format!(
                "Failed to read TMP75 sensor {} on hashboard {}: {e}",
                sensor_index, hashboard.index
            ))
        })?;
        sensors.push(TemperatureSensor {
            name: format!("HB{}-TMP75-{}", hashboard.index, sensor_index),
            temperature_c: Some(decode_tmp75_celsius(raw)),
        });
    }
    Ok(sensors)
}

fn read_eeprom(hashboard: &AmlogicHashboardConfig) -> Result<Vec<u8>, BoardError> {
    let address = configured_eeprom_address(hashboard)?;
    let mut device = LinuxI2cDevice::open(&hashboard.eeprom_i2c_device, address).map_err(|e| {
        BoardError::InitializationFailed(format!(
            "Failed to open EEPROM I2C device {}: {e}",
            hashboard.eeprom_i2c_device.display()
        ))
    })?;

    match device.read_at(0, EEPROM_LEN) {
        Ok(data) => Ok(data),
        Err(_) => {
            let mut data = Vec::with_capacity(EEPROM_LEN);
            for offset in 0..EEPROM_LEN {
                data.push(device.read_byte_data(offset as u8).map_err(|e| {
                    BoardError::InitializationFailed(format!(
                        "Failed to read EEPROM byte {} on hashboard {}: {e}",
                        offset, hashboard.index
                    ))
                })?);
            }
            Ok(data)
        }
    }
}

fn configured_tmp75_addresses(hashboard: &AmlogicHashboardConfig) -> Result<Vec<u16>, BoardError> {
    if !hashboard.temp_sensor_addresses.is_empty() {
        return hashboard
            .temp_sensor_addresses
            .iter()
            .copied()
            .map(validate_i2c_address)
            .collect();
    }

    Ok(tmp75_addresses(hashboard.index)?
        .into_iter()
        .map(u16::from)
        .collect())
}

fn configured_eeprom_address(hashboard: &AmlogicHashboardConfig) -> Result<u16, BoardError> {
    match hashboard.eeprom_address {
        Some(address) => validate_i2c_address(address),
        None => Ok(u16::from(eeprom_address(hashboard.index)?)),
    }
}

fn validate_i2c_address(address: u16) -> Result<u16, BoardError> {
    if address > 0x7F {
        return Err(BoardError::InitializationFailed(format!(
            "invalid 7-bit I2C address: 0x{address:02X}"
        )));
    }

    Ok(address)
}

fn tmp75_addresses(board_index: u8) -> Result<[u8; 2], BoardError> {
    // Per the Bitmain hardware mapping documented in
    // `skot/amlogic-cb-tools::bin/hashboard_s19jpro.rs` (function
    // `tmp75_addresses` and the `--help` banner):
    //   HB0 → 0x48, 0x4C
    //   HB1 → 0x4D, 0x49
    //   HB2 → 0x4E, 0x4A
    // mujina previously had HB0 and HB2 swapped, which silently sent
    // the overtemp cutoff to read whichever board was wired to the
    // OTHER slot's TMP75 pair — a real safety bug for any deployment
    // not running on HB1.
    match board_index {
        0 => Ok([0x48, 0x4C]),
        1 => Ok([0x4D, 0x49]),
        2 => Ok([0x4E, 0x4A]),
        _ => Err(BoardError::InitializationFailed(format!(
            "invalid hashboard index: {board_index}"
        ))),
    }
}

fn eeprom_address(board_index: u8) -> Result<u8, BoardError> {
    match board_index {
        0 => Ok(0x52),
        1 => Ok(0x51),
        2 => Ok(0x50),
        _ => Err(BoardError::InitializationFailed(format!(
            "invalid hashboard index: {board_index}"
        ))),
    }
}

fn read_tmp75_raw(i2c_device: &Path, address: u16) -> anyhow::Result<u16> {
    let mut device = LinuxI2cDevice::open(i2c_device, address)?;
    Ok(device.read_word_data(TMP75_TEMP_REG)?.swap_bytes())
}

fn decode_tmp75_celsius(raw: u16) -> f32 {
    let value = i16::from_be_bytes(raw.to_be_bytes()) >> 4;
    value as f32 * 0.0625
}

fn read_psu_response_frame(dev: &mut LinuxI2cDevice) -> anyhow::Result<Vec<u8>> {
    let mut first = dev.read_byte_transaction()?;
    while first != 0x55 && first != NAK_BYTE {
        first = dev.read_byte_transaction()?;
    }

    if first == NAK_BYTE {
        return Ok(vec![NAK_BYTE]);
    }

    let second = dev.read_byte_transaction()?;
    if second != 0xAA {
        return Err(anyhow::anyhow!(
            "invalid preamble continuation: 0x{second:02X}"
        ));
    }

    let length = dev.read_byte_transaction()?;
    let mut response = Vec::with_capacity(usize::from(length) + 2);
    response.push(first);
    response.push(second);
    response.push(length);

    let remaining = usize::from(length)
        .checked_sub(1)
        .ok_or_else(|| anyhow::anyhow!("response length underflow"))?;
    for _ in 0..remaining {
        response.push(dev.read_byte_transaction()?);
    }

    Ok(response)
}

async fn create_amlogic_board()
-> crate::error::Result<(Box<dyn Board + Send>, super::BoardRegistration)> {
    let config = AMLOGIC_BOARD_CONFIG
        .get()
        .cloned()
        .ok_or_else(|| Error::Config("Amlogic control-board config not installed".into()))?;

    let name = device_id(&config);
    let initial_state = BoardState {
        name: name.clone(),
        model: BOARD_MODEL.into(),
        serial: Some(name),
        ..Default::default()
    };
    let (state_tx, state_rx) = watch::channel(initial_state);

    let (selected_hashboards, psu) = S19kProAmlogic::initialize(&config, &state_tx)
        .await
        .map_err(|e| Error::Hardware(format!("Failed to initialize native Amlogic board: {e}")))?;

    let board = S19kProAmlogic::new(config, selected_hashboards, psu, state_tx);
    let registration = super::BoardRegistration { state_rx };
    Ok((Box::new(board), registration))
}

inventory::submit! {
    VirtualBoardDescriptor {
        device_type: "s19k_pro_amlogic",
        name: BOARD_MODEL,
        create_fn: || Box::pin(create_amlogic_board()),
    }
}
