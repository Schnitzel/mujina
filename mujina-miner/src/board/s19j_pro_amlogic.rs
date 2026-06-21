//! S19j Pro support on a native Antminer Amlogic control board.
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

const BOARD_MODEL: &str = "S19j Pro (Amlogic control board)";
const DEFAULT_BOARD_NAME: &str = "s19jpro-amlogic";
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

/// Native Amlogic S19j Pro board.
pub struct S19jProAmlogic {
    config: AmlogicControlBoardConfig,
    selected_hashboard: AmlogicHashboardConfig,
    board_serial: Option<String>,
    psu: Arc<Mutex<NativeAmlogicPsu>>,
    state_tx: watch::Sender<BoardState>,
    thread_states: Arc<std::sync::Mutex<Vec<crate::api_client::types::ThreadState>>>,
    telemetry_shutdown: CancellationToken,
}

impl S19jProAmlogic {
    fn new(
        config: AmlogicControlBoardConfig,
        selected_hashboard: AmlogicHashboardConfig,
        board_serial: Option<String>,
        psu: Arc<Mutex<NativeAmlogicPsu>>,
        state_tx: watch::Sender<BoardState>,
    ) -> Self {
        Self {
            config,
            selected_hashboard,
            board_serial,
            psu,
            state_tx,
            thread_states: Arc::new(std::sync::Mutex::new(Vec::new())),
            telemetry_shutdown: CancellationToken::new(),
        }
    }

    async fn initialize(
        config: &AmlogicControlBoardConfig,
        state_tx: &watch::Sender<BoardState>,
    ) -> Result<
        (
            AmlogicHashboardConfig,
            Option<String>,
            Arc<Mutex<NativeAmlogicPsu>>,
        ),
        BoardError,
    > {
        let selected_hashboard = select_hashboard(config)?;
        let board_name = device_id(config);

        info!(
            board = %board_name,
            hashboard = selected_hashboard.index,
            serial = %selected_hashboard.serial_path.display(),
            "Initializing native Amlogic S19j Pro board"
        );

        let (board_serial, initial_temperatures) =
            perform_health_gate(config, &selected_hashboard)?;

        configure_fans(config, config.startup.default_fan_percent)?;
        assert_all_resets(config)?;

        let psu = Arc::new(Mutex::new(NativeAmlogicPsu::new(config)));
        let measured_voltage = {
            let mut psu_guard = psu.lock().await;
            psu_guard
                .set_enabled(true)
                .map_err(|e| BoardError::HardwareControl(format!("Failed to enable PSU: {e}")))?;
            psu_guard.config_watchdog(0x00).map_err(|e| {
                BoardError::HardwareControl(format!("Failed to disable PSU watchdog: {e}"))
            })?;
            psu_guard
                .set_voltage(config.startup.initial_voltage)
                .await
                .map_err(|e| {
                    BoardError::HardwareControl(format!("Failed to set PSU voltage: {e}"))
                })?;

            tokio::time::sleep(Duration::from_millis(config.startup.psu_settle_ms)).await;
            psu_guard.measure_voltage().ok()
        };

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
            state.serial = board_serial.clone().or_else(|| Some(board_name.clone()));
            state.temperatures = initial_temperatures.clone();
            state.fans = fan_states.clone();
            state.powers = power_states.clone();
        });

        Ok((selected_hashboard, board_serial, psu))
    }
}

#[async_trait]
impl Board for S19jProAmlogic {
    fn board_info(&self) -> BoardInfo {
        BoardInfo {
            model: BOARD_MODEL.into(),
            firmware_version: None,
            serial_number: self
                .board_serial
                .clone()
                .or_else(|| Some(device_id(&self.config))),
        }
    }

    async fn shutdown(&mut self) -> Result<(), BoardError> {
        info!(board = %device_id(&self.config), "Shutting down native Amlogic board");

        self.telemetry_shutdown.cancel();

        assert_all_resets(&self.config)?;
        configure_fans(&self.config, 0)?;
        self.psu
            .lock()
            .await
            .set_enabled(false)
            .map_err(|e| BoardError::HardwareControl(format!("Failed to disable PSU: {e}")))?;

        Ok(())
    }

    async fn create_hash_threads(&mut self) -> Result<Vec<Box<dyn HashThread>>, BoardError> {
        let data_stream = SerialStream::new(
            &self.selected_hashboard.serial_path.to_string_lossy(),
            SERIAL_BAUD,
        )
        .map_err(|e| BoardError::InitializationFailed(format!("Failed to open data port: {e}")))?;
        let (data_reader, data_writer, data_control) = data_stream.split();

        data_control.flush_input().map_err(|e| {
            BoardError::InitializationFailed(format!("Failed to flush serial buffer: {e}"))
        })?;

        let chip_rx = FramedRead::new(data_reader, bm13xx::FrameCodec);
        let chip_tx = FramedWrite::new(data_writer, bm13xx::FrameCodec);

        let config = ChainConfig {
            name: format!("S19jProAmlogic-HB{}", self.selected_hashboard.index),
            topology: TopologySpec::uniform_domains(42, 3, false),
            chip_config: chip_config::bm1362(),
            peripherals: ChainPeripherals {
                asic_enable: Arc::new(Mutex::new(NativeResetControl {
                    gpio: SysfsGpio::new(self.selected_hashboard.reset_gpio),
                    reset_release_ms: self.config.startup.reset_release_ms,
                })),
                voltage_regulator: Some(
                    Arc::clone(&self.psu) as Arc<Mutex<dyn VoltageRegulator + Send>>
                ),
                chip_uart_baud: None,
            },
            post_broadcast_chip_baud: None,
        };

        let thread = thread_v2::BM13xxThread::new(chip_rx, chip_tx, config).map_err(|e| {
            BoardError::InitializationFailed(format!("Failed to create hash thread: {e}"))
        })?;

        let thread_name = thread.name().to_string();
        // Seed the initial hashrate at 0; the actor's HashrateEstimator
        // takes over once shares start flowing. See the matching change
        // in `thread_hashrate_value` below for why the static
        // `capabilities.hashrate_estimate` is no longer surfaced.
        let thread_hashrate = 0u64;

        self.state_tx.send_modify(|state| {
            state.serial = self
                .board_serial
                .clone()
                .or_else(|| Some(device_id(&self.config)));
            state.threads = vec![crate::api_client::types::ThreadState {
                name: thread_name.clone(),
                hashrate: thread_hashrate,
                is_active: false,
            }];
        });

        {
            let mut thread_states = self
                .thread_states
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *thread_states = vec![crate::api_client::types::ThreadState {
                name: thread_name.clone(),
                hashrate: thread_hashrate,
                is_active: false,
            }];
        }

        let thread = BoardStateHashThread::new(
            Box::new(thread),
            self.state_tx.clone(),
            Arc::clone(&self.thread_states),
        );

        let config = self.config.clone();
        let hashboard = self.selected_hashboard.clone();
        let psu = Arc::clone(&self.psu);
        let state_tx = self.state_tx.clone();
        let thread_states = Arc::clone(&self.thread_states);
        let shutdown = self.telemetry_shutdown.child_token();
        tokio::spawn(async move {
            native_telemetry_task(config, hashboard, psu, state_tx, thread_states, shutdown).await;
        });

        Ok(vec![Box::new(thread)])
    }
}

struct BoardStateHashThread {
    inner: Box<dyn HashThread>,
    state_tx: watch::Sender<BoardState>,
    thread_states: Arc<std::sync::Mutex<Vec<crate::api_client::types::ThreadState>>>,
}

impl BoardStateHashThread {
    fn new(
        inner: Box<dyn HashThread>,
        state_tx: watch::Sender<BoardState>,
        thread_states: Arc<std::sync::Mutex<Vec<crate::api_client::types::ThreadState>>>,
    ) -> Self {
        Self {
            inner,
            state_tx,
            thread_states,
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

        match self.exchange(CMD_SET_VOLTAGE, &[dac, 0x00]) {
            Ok(_) => Ok(()),
            Err(err) => {
                let readback = self.read_target_voltage()?;
                if (readback - clamped).abs() <= 0.15 {
                    warn!(requested = clamped, readback, error = %err, "PSU accepted voltage by readback after transient response issue");
                    Ok(())
                } else {
                    Err(err)
                }
            }
        }
    }

    fn voltage_range(&self) -> (f32, f32) {
        (12.0, 15.0)
    }

    fn target_voltage(&self) -> f32 {
        12.6
    }

    fn voltage_step(&self) -> f32 {
        0.1
    }
}

fn select_hashboard(
    config: &AmlogicControlBoardConfig,
) -> Result<AmlogicHashboardConfig, BoardError> {
    if config.hashboards.is_empty() {
        return Err(BoardError::InitializationFailed(
            "Amlogic config has no configured hashboards".into(),
        ));
    }

    let mut first_present = None;
    for hashboard in &config.hashboards {
        let present = is_hashboard_present(hashboard)?;
        if !present {
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

        if first_present.is_none() {
            first_present = Some(hashboard.clone());
        }
    }

    first_present.ok_or_else(|| {
        BoardError::InitializationFailed("No configured hashboards are present".into())
    })
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
    hashboard: AmlogicHashboardConfig,
    psu: Arc<Mutex<NativeAmlogicPsu>>,
    state_tx: watch::Sender<BoardState>,
    thread_states: Arc<std::sync::Mutex<Vec<crate::api_client::types::ThreadState>>>,
    shutdown: CancellationToken,
) {
    const TELEMETRY_INTERVAL: Duration = Duration::from_secs(2);

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        let temperatures = match read_temperatures(&hashboard) {
            Ok(temperatures) => temperatures,
            Err(error) => {
                debug!(board = %hashboard.index, error = %error, "Native telemetry temperature read failed");
                Vec::new()
            }
        };

        let fans = read_fan_states(&config, config.startup.default_fan_percent).await;
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
    let addresses = configured_tmp75_addresses(hashboard)?;
    let mut sensors = Vec::with_capacity(addresses.len());
    for (sensor_index, address) in addresses.into_iter().enumerate() {
        let raw = read_tmp75_raw(&hashboard.temp_i2c_device, address).map_err(|e| {
            BoardError::InitializationFailed(format!(
                "Failed to read TMP75 sensor {} on hashboard {}: {e}",
                sensor_index, hashboard.index
            ))
        })?;
        sensors.push(TemperatureSensor {
            name: format!("HB{}-Temp{}", hashboard.index, sensor_index),
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
    match board_index {
        0 => Ok([0x4E, 0x4A]),
        1 => Ok([0x4D, 0x49]),
        2 => Ok([0x48, 0x4C]),
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

    let (selected_hashboard, board_serial, psu) = S19jProAmlogic::initialize(&config, &state_tx)
        .await
        .map_err(|e| Error::Hardware(format!("Failed to initialize native Amlogic board: {e}")))?;

    let board = S19jProAmlogic::new(config, selected_hashboard, board_serial, psu, state_tx);
    let registration = super::BoardRegistration { state_rx };
    Ok((Box::new(board), registration))
}

inventory::submit! {
    VirtualBoardDescriptor {
        device_type: "s19j_pro_amlogic",
        name: BOARD_MODEL,
        create_fn: || Box::pin(create_amlogic_board()),
    }
}
