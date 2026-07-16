//! Antminer S19x Pro support on a native Amlogic (A113D) control board.
//!
//! One unified driver for every Amlogic-controlbboard S19x hashboard. The
//! S19j Pro (BM1362 / BHB42601·BHB42611) and S19k Pro (BM1366 / BHB56902)
//! share the same control-board machinery — APW12 PSU, fans, LEDs, GPIO,
//! reset sequencing, per-hashboard PIC handshake — and differ only in the
//! *chip family* on the hashboard. That per-model divergence (chip config,
//! chain topology, voltage envelope, thermal ceiling) is factored into
//! [`HashboardSpec`] / [`hashboard_spec`]; everything else is shared.
//!
//! The board picks every slot whose detect GPIO reads "present"
//! ([`select_present_hashboards`]) and spawns one hash thread per present
//! board so each chain mines independently on the shared APW12, which is
//! coordinated through a [`bm13xx::chain_config::ChainCoordinator`].

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};

use amlogic_cb_tools::{
    eeprom_antminer::{DecodedAntminerEeprom, decode_antminer_eeprom},
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
    config::{AmlogicControlBoardConfig, AmlogicHashboardConfig, HashboardModel},
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

/// Descriptor-level model label (the inventory `name` is `&'static str`,
/// so it can't be per-instance). The *reported* board model surfaced in
/// `BoardState` is resolved per-model via [`HashboardModel::board_model_label`].
const UNIFIED_BOARD_MODEL: &str = "S19x Pro (Amlogic control board)";
const FAN_PWM_PERIOD_NS: u32 = 10_000;
const SERIAL_BAUD: u32 = 115_200;
const PSU_RESPONSE_DELAY_MS: u64 = 500;
const PSU_MAX_RESPONSE_ATTEMPTS: usize = 3;
/// Resume-time PIC handshake retries. Covers a transient "No such device or
/// address" on a *known* PIC-variant hashboard right after its 12 V rail
/// comes back up on resume — observed on .222 (hashboard 1 failed once, then
/// handshaked cleanly on the very next full cold-init with no hardware
/// change). Bounded and short: this is a best-effort ride-over-a-blip, not a
/// substitute for the chain's own health checks.
const PIC_RESUME_HANDSHAKE_ATTEMPTS: usize = 3;
const PIC_RESUME_RETRY_DELAY_MS: u64 = 250;

/// How many times the telemetry task probes a hashboard's PIC heartbeat at
/// startup before giving up. `PicChain::open` only opens the i2c bus (no
/// device probe), so the heartbeat IS the presence test — and the bus can
/// hand back a single noisy frame (observed on .222: `[0x56; 6]`) that must
/// not be mistaken for a noPIC board.
const PIC_HEARTBEAT_PROBE_ATTEMPTS: usize = 3;

/// Per-op timeout for the APW12 i2c calls during board bring-up. The exchange
/// runs on a raw, timeout-less `LinuxI2cDevice` ioctl (on `spawn_blocking`),
/// so a dead/unpowered PSU makes the syscall block forever with no error — the
/// board just stalls silently. Bounding each bring-up op turns that hang into
/// a clear error. The blocked `spawn_blocking` thread leaks, but the caller
/// (and the daemon) recover.
const PSU_BRINGUP_OP_TIMEOUT: Duration = Duration::from_secs(5);
const EEPROM_LEN: usize = 256;
const TMP75_TEMP_REG: u8 = 0x00;

static AMLOGIC_BOARD_CONFIG: OnceLock<AmlogicControlBoardConfig> = OnceLock::new();

/// Per-model (chip-family) hashboard specifics factored out of the
/// otherwise-identical Amlogic control-board driver. Everything that
/// differs between an S19j Pro (BM1362) and an S19k Pro (BM1366)
/// hashboard lives here; the control-board machinery is shared.
///
/// The constants below are hardware-critical (they gate chip PLL-lock and
/// stability) and are copied verbatim, per model, from the two original
/// drivers — do NOT average or "tidy" them.
#[derive(Clone)]
struct HashboardSpec {
    /// Thread-name prefix, kept per-model so existing S19j/S19k thread
    /// names stay stable in the API/UI (`{label}-HB{index}`).
    chain_label: &'static str,
    /// BM13xx chip-family configuration for this hashboard.
    chip_config: chip_config::ChipConfig,
    /// Chain topology: voltage domains × chips-per-domain.
    topology: TopologySpec,
    /// Cold-init voltage clamp `(min, max)` reported by
    /// [`VoltageRegulator::voltage_range`].
    voltage_range: (f32, f32),
    /// Factory/operating setpoint reported by
    /// [`VoltageRegulator::target_voltage`].
    target_voltage: f32,
    /// Per-step voltage granularity ([`VoltageRegulator::voltage_step`]).
    voltage_step: f32,
    /// Runtime clamp `(min, max)` applied inside
    /// [`NativeAmlogicPsu::set_voltage`]. Wider than `voltage_range`; it
    /// only governs the *runtime* voltage bands, not cold init.
    psu_clamp: (f32, f32),
    /// Operating-frequency ceiling (MHz) for the M4 thermal cap — the
    /// shared frequency cap never exceeds this.
    thermal_cap_max_mhz: u32,
    /// Useful runtime operating-frequency floor (MHz) reported to controllers
    /// (Nova) as the low end of the dial band. Sits above the hard
    /// `MIN_RUNTIME_FREQ_MHZ` safety clamp: below this, per-chip efficiency and
    /// PLL stability degrade, so a power dial shouldn't operate here. The band
    /// ceiling is the model's cold-init target
    /// (`ChipConfig::target_frequency_mhz`, which is also the runtime clamp),
    /// so it isn't duplicated on the spec.
    min_operating_mhz: u32,
    /// Post-broadcast chip-UART baud. `Some(Baud3M)` (BM1366/S19k) reopens the
    /// chip UART at 3.125 Mbaud after the broadcast phase and needs the
    /// fd-keepalive adapter. `None` (BM1362/S19j) keeps the fixed `SERIAL_BAUD`
    /// the original single-board S19j driver used — no reopen, no keepalive.
    /// Kept per-model on purpose: the S19j baud switch is untested on BM1362,
    /// and the shipped S19j behaviour is the fixed-baud path.
    post_broadcast_baud: Option<bm13xx::protocol::BaudRate>,
}

/// Resolve the [`HashboardSpec`] for a hashboard model. This is the single
/// place chip-family constants live.
fn hashboard_spec(model: HashboardModel) -> HashboardSpec {
    match model {
        HashboardModel::S19kPro => HashboardSpec {
            chain_label: "S19kProAmlogic",
            chip_config: chip_config::bm1366(),
            topology: TopologySpec::uniform_domains(11, 7, false),
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
            voltage_range: (13.9, 14.5),
            // Factory-equivalent operating point, matching Braiins's
            // `Voltage(13.9)` for this hashboard. With 500 MHz frequency
            // mujina was at ~21 TH/s; at 645 MHz + 13.9 V Braiins gets
            // ~30 TH/s on the same chips.
            target_voltage: 13.9,
            voltage_step: 0.1,
            // 11.7 V floor lets the runtime voltage bands (M1.5) reach the
            // APW12's hardware minimum (~11.78 V, DAC=255). Cold-init
            // voltages clamp higher upstream (voltage_range), so this only
            // widens the *runtime* range.
            psu_clamp: (11.7, 15.0),
            // The BM1366 operating max (MHz). The cap never exceeds this;
            // 575 MHz matches LuxOS's actual sustained operating point on
            // BHB56902 (see chip_config::bm1366 target_frequency_mhz).
            thermal_cap_max_mhz: 575,
            // Dial floor for the BM1366/S19k. Ceiling is 575 MHz
            // (target_frequency_mhz). 150 MHz keeps the chain in its stable
            // range while still giving the power dial a wide span to shed to.
            min_operating_mhz: 150,
            // BM1366 switches the chip UART to 3.125 Mbaud after the broadcast
            // phase (the deployed S19k behaviour).
            post_broadcast_baud: Some(bm13xx::protocol::BaudRate::Baud3M),
        },
        HashboardModel::S19jPro => HashboardSpec {
            chain_label: "S19jProAmlogic",
            chip_config: chip_config::bm1362(),
            topology: TopologySpec::uniform_domains(42, 3, false),
            // BHB42601 EEPROM specifies 13.20 V at 525 MHz (factory test
            // setpoint, decoded from EEPROM `voltage_v` field). The generic
            // `bm13xx::thread_v2::voltage_for_frequency_stacked()` formula is
            // calibrated for emberone-style stacked regulators (per-chip
            // 0.3 V at 500 MHz, multiplied by 12 chips = 3.6 V total) and
            // returns ~12.6 V when applied to the 42-domain series chain on
            // S19j Pro (0.3 V × 42). That's 0.6 V under spec and causes chips
            // to fall off the chain mid-ramp under load.
            //
            // Clamping the min here (`applied.clamp(min_v, max_v)` in
            // thread_v2) forces the ramp to program at least 13.2 V from the
            // first step. Above-spec at low frequencies is harmless; the chips
            // just have headroom they don't use.
            //
            // Long-term fix is per-chip-family voltage-frequency tables in
            // chip_config.rs but that's a wider refactor.
            voltage_range: (13.2, 15.0),
            // Match EEPROM-specified operating voltage for BHB42601.
            target_voltage: 13.2,
            voltage_step: 0.1,
            // 12.0 V runtime floor — the original S19j Pro driver's
            // `set_voltage` clamp. Cold-init clamps higher via voltage_range.
            psu_clamp: (12.0, 15.0),
            // BM1362 operating max (MHz). The S19j Pro driver had no thermal
            // cap before this unification; routing it through the shared
            // multi-board path adds the protective throttle. The BM1362
            // factory/operating point is 525 MHz (chip_config::bm1362
            // max_freq), so a 525 MHz ceiling never bites under normal
            // temps — it only sheds frequency as the board heats.
            thermal_cap_max_mhz: 525,
            // Dial floor for the BM1362/S19j. Ceiling is 500 MHz
            // (target_frequency_mhz = None → the unwrap_or(500) default in
            // thread_v2, which is also the runtime clamp). 150 MHz floor.
            min_operating_mhz: 150,
            // BM1362 keeps the fixed SERIAL_BAUD — the original single-board
            // S19j driver left the 3.125 Mbaud switch OFF (`None`), and that's
            // the path currently mining on real S19j hardware. Do not enable
            // the switch here without validating it on a BM1362 board first.
            post_broadcast_baud: None,
        },
    }
}

/// Auto-detect a hashboard's chip family from its decoded EEPROM so the
/// board self-identifies rather than trusting the configured `model`.
///
/// Matching is case-insensitive `contains`:
///   - `board_name` "BHB42601" / "BHB42611" OR `chip_marking` "BM1362"
///     → [`HashboardModel::S19jPro`]
///   - `board_name` "BHB56902"             OR `chip_marking` "BM1366"
///     → [`HashboardModel::S19kPro`]
///   - neither → `None` (caller falls back to the configured model).
fn detect_hashboard_model(decoded: &DecodedAntminerEeprom) -> Option<HashboardModel> {
    let board_name = decoded.board_name.to_ascii_uppercase();
    let chip_marking = decoded.chip_marking.to_ascii_uppercase();

    if board_name.contains("BHB42601")
        || board_name.contains("BHB42611")
        || chip_marking.contains("BM1362")
    {
        Some(HashboardModel::S19jPro)
    } else if board_name.contains("BHB56902") || chip_marking.contains("BM1366") {
        Some(HashboardModel::S19kPro)
    } else {
        None
    }
}

/// Effective PSU voltage envelope for the *set* of present hashboard
/// families. The single APW12 powers every board, so one rail must suit
/// EVERY present family at once. Safety-critical: the resolved envelope is
/// never allowed to exceed any present board's max or drop below its min.
#[derive(Clone, Copy, Debug, PartialEq)]
struct EffectiveVoltageSpec {
    /// Cold-init clamp `(min, max)` — intersection `(max-of-mins, min-of-maxs)`
    /// of every present model's [`HashboardSpec::voltage_range`].
    voltage_range: (f32, f32),
    /// Operating setpoint — MAX of every present model's
    /// [`HashboardSpec::target_voltage`], clamped into the intersections
    /// so it can never sit outside a present board's envelope.
    target_voltage: f32,
    /// Per-step granularity — MIN of every present model's
    /// [`HashboardSpec::voltage_step`].
    voltage_step: f32,
    /// Runtime clamp `(min, max)` — intersection `(max-of-mins, min-of-maxs)`
    /// of every present model's [`HashboardSpec::psu_clamp`].
    psu_clamp: (f32, f32),
}

/// Compute the shared-rail voltage envelope from the present *detected*
/// models.
///
/// Policy (all safety-driven — the shared APW12 must satisfy every board):
///   - `target_voltage` = MAX of targets (the highest-voltage family sets
///     the operating point; lower families get harmless headroom their
///     config comments explicitly call out as safe).
///   - `voltage_range` / `psu_clamp` = intersection `(MAX of mins, MIN of
///     maxs)` — the band every present board tolerates.
///   - `voltage_step` = MIN of steps (finest granularity any board needs).
///   - The chosen `target_voltage` is clamped into both intersections so it
///     never exceeds any board's max or drops below any board's min.
///   - If either intersection is empty (`min >= max`), the mix is genuinely
///     incompatible: FAIL rather than pick a voltage outside some board's
///     range.
///
/// For a homogeneous chassis (all one model) the folds and clamps collapse
/// to exactly that single model's spec — byte-for-byte identical to the
/// pre-mixed-support behaviour.
fn effective_voltage_spec(
    models: &[HashboardModel],
) -> Result<EffectiveVoltageSpec, BoardError> {
    if models.is_empty() {
        return Err(BoardError::InitializationFailed(
            "no present hashboards to resolve an effective PSU voltage envelope".into(),
        ));
    }
    let specs: Vec<HashboardSpec> = models.iter().map(|m| hashboard_spec(*m)).collect();

    // MAX of targets; intersection (max-of-mins, min-of-maxs) of ranges and
    // clamps; MIN of steps. Seeding the folds from ±infinity keeps a
    // single-model chassis byte-identical (max(-inf, v) == v exactly).
    let target_voltage = specs
        .iter()
        .map(|s| s.target_voltage)
        .fold(f32::NEG_INFINITY, f32::max);

    let range_min = specs
        .iter()
        .map(|s| s.voltage_range.0)
        .fold(f32::NEG_INFINITY, f32::max);
    let range_max = specs
        .iter()
        .map(|s| s.voltage_range.1)
        .fold(f32::INFINITY, f32::min);

    let clamp_min = specs
        .iter()
        .map(|s| s.psu_clamp.0)
        .fold(f32::NEG_INFINITY, f32::max);
    let clamp_max = specs
        .iter()
        .map(|s| s.psu_clamp.1)
        .fold(f32::INFINITY, f32::min);

    let voltage_step = specs
        .iter()
        .map(|s| s.voltage_step)
        .fold(f32::INFINITY, f32::min);

    // Safety assertions: an empty intersection means no single voltage is
    // safe for every present board. Refuse to power on rather than pick a
    // voltage outside some board's tolerated band.
    if !(range_min < range_max) {
        return Err(BoardError::InitializationFailed(format!(
            "incompatible hashboard mix: cold-init voltage ranges do not overlap \
             (max-of-mins {range_min:.3} V >= min-of-maxs {range_max:.3} V) — refusing to power on"
        )));
    }
    if !(clamp_min < clamp_max) {
        return Err(BoardError::InitializationFailed(format!(
            "incompatible hashboard mix: runtime voltage clamps do not overlap \
             (max-of-mins {clamp_min:.3} V >= min-of-maxs {clamp_max:.3} V) — refusing to power on"
        )));
    }

    // Clamp the chosen operating point into BOTH intersections. For the
    // homogeneous case the target already sits inside its own range/clamp,
    // so this is a no-op and preserves the exact original value.
    let target_voltage = target_voltage
        .clamp(range_min, range_max)
        .clamp(clamp_min, clamp_max);

    Ok(EffectiveVoltageSpec {
        voltage_range: (range_min, range_max),
        target_voltage,
        voltage_step,
        psu_clamp: (clamp_min, clamp_max),
    })
}

/// Useful runtime operating-frequency band `(floor, ceiling)` in MHz for the
/// set of present detected models — reported to a power controller (Nova) so
/// it dials/calibrates inside the band every present board tolerates rather
/// than guessing per model.
///
/// Shared rail → one operating point, so intersect:
///   - floor  = MAX of per-model `min_operating_mhz` (the highest of the
///     useful floors — safe for all).
///   - ceiling = MIN of per-model operating maxima. The max is the model's
///     cold-init target (`ChipConfig::target_frequency_mhz`, defaulting to
///     500 MHz to match the `unwrap_or(500.0)` in `thread_v2`), which is
///     exactly the runtime `SetFrequency` clamp — so a controller staying at
///     or below `ceiling` is never clamped.
///
/// A homogeneous chassis collapses to that single model's band (e.g. all
/// S19j → (150, 500); all S19k → (150, 575); mixed j+k → (150, 500)).
fn effective_freq_band(models: &[HashboardModel]) -> (f32, f32) {
    let mut floor = f32::NEG_INFINITY;
    let mut ceiling = f32::INFINITY;
    for model in models {
        let spec = hashboard_spec(*model);
        floor = floor.max(spec.min_operating_mhz as f32);
        let model_max = spec.chip_config.target_frequency_mhz.unwrap_or(500.0);
        ceiling = ceiling.min(model_max);
    }
    if !floor.is_finite() || !ceiling.is_finite() {
        // No present models — shouldn't happen post-init; conservative default.
        return (150.0, 500.0);
    }
    (floor, ceiling)
}

/// Short family label used only inside the mixed-chassis summary
/// (`board_model_label` carries the "(Amlogic control board)" suffix which
/// reads badly in a count list).
fn short_model_name(model: HashboardModel) -> &'static str {
    match model {
        HashboardModel::S19jPro => "S19j Pro",
        HashboardModel::S19kPro => "S19k Pro",
    }
}

/// Reported board-model label from the present *detected* models.
///
/// Homogeneous → that single model's `board_model_label()` (unchanged from
/// the single-family behaviour). Mixed → a count summary, e.g.
/// `"Mixed Amlogic (2x S19j Pro, 1x S19k Pro)"` (families listed in a
/// stable S19j-then-S19k order).
fn reported_model_label(models: &[HashboardModel]) -> String {
    let Some(first) = models.first() else {
        // No present boards — shouldn't happen (init guarantees >= 1); fall
        // back to the board default label.
        return HashboardModel::S19kPro.board_model_label().to_string();
    };
    if models.iter().all(|m| m == first) {
        return first.board_model_label().to_string();
    }
    let mut parts = Vec::new();
    for family in [HashboardModel::S19jPro, HashboardModel::S19kPro] {
        let count = models.iter().filter(|m| **m == family).count();
        if count > 0 {
            parts.push(format!("{}x {}", count, short_model_name(family)));
        }
    }
    format!("Mixed Amlogic ({})", parts.join(", "))
}

/// Board-level model, resolved from the first configured hashboard. A
/// mixed-model chassis isn't supported; every hashboard shares one model.
fn board_model(config: &AmlogicControlBoardConfig) -> HashboardModel {
    config
        .hashboards
        .first()
        .map(|hb| hb.model)
        .unwrap_or(HashboardModel::S19kPro)
}

/// Per-model default board name used when the config leaves `board_name`
/// unset. Kept per-model so existing device IDs stay stable.
fn default_board_name(model: HashboardModel) -> &'static str {
    match model {
        HashboardModel::S19jPro => "s19jpro-amlogic",
        HashboardModel::S19kPro => "s19kpro-amlogic",
    }
}

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
        .unwrap_or_else(|| default_board_name(board_model(config)).to_string())
}

/// One hashboard, after `select_present_hashboards` + `perform_health_gate`.
#[derive(Clone)]
struct SelectedHashboard {
    config: AmlogicHashboardConfig,
    board_serial: Option<String>,
    /// Chip family DETECTED from this board's EEPROM (falls back to the
    /// configured `config.model` only when detection is unavailable). This
    /// is the model that drives the chain — chip config, topology, chain
    /// label, and this board's contribution to the shared-rail voltage
    /// envelope and its per-chain thermal cap.
    model: HashboardModel,
}

/// Native Amlogic S19x Pro board.
///
/// Holds one or more `SelectedHashboard`s — the Amlogic control board
/// supports up to three hashboards on the same APW12 / A113D SoC, and
/// `select_present_hashboards` picks every slot whose detect GPIO reads
/// "present". `create_hash_threads` then spawns one
/// [`BoardStateHashThread`] per board so each chain mines independently;
/// the shared APW12 is coordinated through a
/// [`bm13xx::chain_config::ChainCoordinator`] so the per-step voltage
/// commands during the frequency ramp don't oscillate across chains.
///
/// Each present hashboard is configured from its own model's
/// [`HashboardSpec`] (chip family + topology + voltage), so a homogeneous
/// S19j *or* S19k chassis both run every present board through the same
/// code path.
pub struct S19xAmlogic {
    config: AmlogicControlBoardConfig,
    selected_hashboards: Vec<SelectedHashboard>,
    /// False when the APW12 never answered on i2c during bring-up (almost
    /// always: its AC feed is cut). The board still runs fans/telemetry, but
    /// the chains are skipped — the chips have no rail. Nothing polls for the
    /// PSU coming back; mining requires a restart, by design.
    psu_present: bool,
    psu: Arc<Mutex<NativeAmlogicPsu>>,
    state_tx: watch::Sender<BoardState>,
    thread_states: Arc<std::sync::Mutex<Vec<crate::api_client::types::ThreadState>>>,
    telemetry_shutdown: CancellationToken,
}

impl S19xAmlogic {
    fn new(
        config: AmlogicControlBoardConfig,
        selected_hashboards: Vec<SelectedHashboard>,
        psu: Arc<Mutex<NativeAmlogicPsu>>,
        psu_present: bool,
        state_tx: watch::Sender<BoardState>,
    ) -> Self {
        Self {
            config,
            selected_hashboards,
            psu_present,
            psu,
            state_tx,
            thread_states: Arc::new(std::sync::Mutex::new(Vec::new())),
            telemetry_shutdown: CancellationToken::new(),
        }
    }

    async fn initialize(
        config: &AmlogicControlBoardConfig,
        state_tx: &watch::Sender<BoardState>,
    ) -> Result<(Vec<SelectedHashboard>, Arc<Mutex<NativeAmlogicPsu>>, bool), BoardError> {
        let board_name = device_id(config);
        // Configured/expected model — used only for the pre-detection log
        // banner and as a per-slot fallback if EEPROM detection is
        // unavailable. The DETECTED model (below) is what actually drives
        // each chain and the shared PSU envelope.
        let configured_model = board_model(config);
        let present = select_present_hashboards(config)?;

        info!(
            board = %board_name,
            configured_model = %configured_model.board_model_label(),
            slots = %present
                .iter()
                .map(|hb| hb.index.to_string())
                .collect::<Vec<_>>()
                .join(","),
            "Initializing native Amlogic S19x Pro board"
        );

        // Health-gate every present hashboard before energizing chips.
        // EEPROM + temperature reads can fail individually; a failure
        // here aborts board init for now (we don't want a "ghost"
        // hashboard with bad EEPROM coming up with the others). The
        // health gate also self-identifies each board's chip family from
        // its EEPROM (see `perform_health_gate`).
        let mut selected: Vec<SelectedHashboard> = Vec::with_capacity(present.len());
        let mut all_temps: Vec<TemperatureSensor> = Vec::new();
        for hb in &present {
            let (board_serial, detected_model, initial_temperatures) =
                perform_health_gate(config, hb)?;
            all_temps.extend(initial_temperatures);
            // Fall back to the configured model only when detection was
            // unavailable (EEPROM read disabled or unrecognized board —
            // `perform_health_gate` has already warned in the latter case).
            let model = detected_model.unwrap_or(hb.model);
            selected.push(SelectedHashboard {
                config: hb.clone(),
                board_serial,
                model,
            });
        }

        // Resolve the shared-rail voltage envelope and the reported label
        // from the SET of present detected models. The single APW12 must
        // suit every present family at once (see `effective_voltage_spec`).
        let present_models: Vec<HashboardModel> =
            selected.iter().map(|s| s.model).collect();
        let effective_voltage = effective_voltage_spec(&present_models)?;
        let reported_label = reported_model_label(&present_models);
        info!(
            board = %board_name,
            reported_model = %reported_label,
            target_v = effective_voltage.target_voltage,
            cold_init_range = ?effective_voltage.voltage_range,
            runtime_clamp = ?effective_voltage.psu_clamp,
            step_v = effective_voltage.voltage_step,
            "Resolved effective shared-PSU voltage envelope for present hashboard families"
        );

        configure_fans(config, config.startup.default_fan_percent)?;

        let psu = Arc::new(Mutex::new(NativeAmlogicPsu::new(config, effective_voltage)));

        // Force a genuine power-on-reset of the chips before touching them.
        //
        // `set_enabled` is a pure GPIO write on the active-low enable line — no
        // i2c — so this works even when the APW12 is unpowered or its bus is
        // dead. That matters: mujina is SIGKILLed on stop (see the init
        // script), which leaves the enable line ASSERTED. So on a restart the
        // rail is still up and the chips are still clocked from the previous
        // run, and `assert_all_resets()` alone does not recover them — the
        // chips have to come up *into* reset from an unpowered rail, which is
        // exactly the sequence the resume path already relies on. Cutting AC
        // upstream (a Shelly relay) doesn't help either: restoring it just
        // brings the rail back with the enable line still asserted, so the
        // chips free-run before mujina ever asserts reset.
        //
        // Dropping the rail unconditionally makes cold-init idempotent whatever
        // state we inherited (fresh boot, restart into hot clocked chips, or a
        // half-configured chain).
        {
            let mut psu_guard = psu.lock().await;
            psu_guard.set_enabled(false).map_err(|e| {
                BoardError::HardwareControl(format!(
                    "Failed to disable PSU for the cold-init reset cycle: {e}"
                ))
            })?;
        }
        info!(
            board = %board_name,
            off_ms = config.startup.psu_off_settle_ms,
            "Dropping the PSU rail to force a chip power-on-reset"
        );
        tokio::time::sleep(Duration::from_millis(config.startup.psu_off_settle_ms)).await;

        // Hold every chain in reset BEFORE the rail comes back, so the chips
        // power up already held in reset instead of free-running.
        assert_all_resets(config)?;

        // Whether the APW12 answered on i2c. When it didn't, the board still
        // comes up — see the comment on the timeout arms below.
        let mut psu_present = true;
        let measured_voltage = {
            let mut psu_guard = psu.lock().await;
            psu_guard
                .set_enabled(true)
                .map_err(|e| BoardError::HardwareControl(format!("Failed to enable PSU: {e}")))?;

            // Each APW12 op below is a raw, timeout-less i2c ioctl (on
            // spawn_blocking). If the PSU is unpowered or the bus is
            // disconnected the ioctl blocks forever with no error, so the
            // board used to stall here SILENTLY (no log line, never inits).
            // Bound each op so a dead PSU surfaces a clear message instead.
            //
            // A TIMEOUT means the APW12 isn't answering at all — usually its AC
            // feed is simply cut (a Shelly relay), not a fault. That must NOT
            // fail the whole board: the APW12 only powers the ASICs, while the
            // EEPROMs, temperature sensors and fans all run off the
            // control-board rail and are already up (enumeration happened
            // above). So carry on in telemetry-only mode — fans and temps keep
            // working and only the chains are skipped. Mining then requires a
            // restart once the PSU is back, which is deliberate: nothing here
            // polls for its return.
            //
            // `config_watchdog` is best-effort (some APW12 firmware NAKs it),
            // so a returned error is only a warning.
            match tokio::time::timeout(PSU_BRINGUP_OP_TIMEOUT, psu_guard.config_watchdog(0x00)).await
            {
                Err(_elapsed) => {
                    warn!(
                        board = %board_name,
                        timeout = ?PSU_BRINGUP_OP_TIMEOUT,
                        "APW12 did not respond on i2c-1 during bring-up (config_watchdog) — the \
                         PSU is unpowered or its i2c bus is disconnected. Coming up WITHOUT the \
                         chains: fans, temperatures and EEPROM still work. Restore PSU power and \
                         restart mujina to mine."
                    );
                    psu_present = false;
                }
                Ok(Err(e)) => {
                    warn!("PSU watchdog disable rejected (firmware variant?), continuing: {e}");
                }
                Ok(Ok(())) => {}
            }

            if psu_present {
                match tokio::time::timeout(
                    PSU_BRINGUP_OP_TIMEOUT,
                    psu_guard.set_voltage(config.startup.initial_voltage),
                )
                .await
                {
                    Err(_elapsed) => {
                        warn!(
                            board = %board_name,
                            timeout = ?PSU_BRINGUP_OP_TIMEOUT,
                            "APW12 did not respond on i2c-1 during bring-up (set_voltage) — \
                             coming up WITHOUT the chains; restore PSU power and restart to mine."
                        );
                        psu_present = false;
                    }
                    Ok(Err(e)) => {
                        // The PSU *is* answering but rejected the voltage —
                        // a real fault, not a missing rail. Keep that fatal.
                        return Err(BoardError::HardwareControl(format!(
                            "Failed to set PSU voltage: {e}"
                        )));
                    }
                    Ok(Ok(())) => {}
                }
            }

            if psu_present {
                tokio::time::sleep(Duration::from_millis(config.startup.psu_settle_ms)).await;
                match tokio::time::timeout(PSU_BRINGUP_OP_TIMEOUT, psu_guard.measure_voltage()).await
                {
                    Err(_elapsed) => {
                        warn!(
                            "APW12 measure_voltage timed out after {PSU_BRINGUP_OP_TIMEOUT:?} \
                             (i2c-1 unresponsive); continuing without a measured voltage"
                        );
                        None
                    }
                    Ok(result) => result.ok(),
                }
            } else {
                // Leave the enable line asserted but the rail is dead anyway;
                // nothing to measure.
                None
            }
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
        // Bring up each PIC-variant board's DC-DC (retried), and note any that
        // can't be started. On PIC variants (S19j Pro) the DC-DC is gated by
        // the on-hashboard PIC: if it won't handshake / enable (after retries)
        // the chips can never power, so the board cannot start. noPIC variants
        // (S19k Pro) bring their DC-DC up directly — no PIC gate.
        // Skipped entirely without a rail: the PIC's LDO is fed from the same
        // 12 V the APW12 supplies, so with the PSU down every handshake would
        // fail and mark every PIC-variant board "unstartable" — turning a
        // simply-unpowered chassis into a hard init failure.
        let mut unstartable: Vec<u8> = Vec::new();
        for sel in &selected {
            if !psu_present {
                break;
            }
            if !sel.model.expects_pic() {
                continue; // noPIC board — DC-DC comes up directly.
            }
            let pic_addr = pic_address_for_slot(sel.config.index);
            match pic_handshake_and_enable_dc_dc(&sel.config.eeprom_i2c_device, pic_addr).await {
                Ok(Some(version)) => info!(
                    hashboard = sel.config.index,
                    addr = format_args!("0x{:02x}", pic_addr),
                    version = format_args!("0x{:02x}", version),
                    "PIC handshake ok; DC-DC enabled, chips powering up"
                ),
                Ok(None) => {
                    warn!(
                        hashboard = sel.config.index,
                        addr = format_args!("0x{:02x}", pic_addr),
                        "PIC-variant board: no PIC responded at its address; cannot enable DC-DC"
                    );
                    unstartable.push(sel.config.index);
                }
                Err(e) => {
                    warn!(
                        hashboard = sel.config.index,
                        addr = format_args!("0x{:02x}", pic_addr),
                        error = %e,
                        "PIC handshake / DC-DC enable failed; hashboard cannot be started"
                    );
                    unstartable.push(sel.config.index);
                }
            }
        }

        // Startability policy. By default an unstartable hashboard is NOT
        // silently ignored: fail loudly so the operator fixes it, instead of
        // quietly mining at reduced capacity — and, crucially, without leaving
        // a dead chain wired into the shared-rail ramp coordinator, which would
        // wedge the whole cold-init. Set `skip_unstartable_hashboards = true`
        // to drop the bad board(s) and bring the rest of the chassis up.
        // (The resolved PSU envelope covers every present family, so it stays
        // safe for the remaining boards after a skip.)
        if !unstartable.is_empty() {
            if config.startup.health_gate.skip_unstartable_hashboards {
                warn!(
                    board = %board_name,
                    unstartable = ?unstartable,
                    "Skipping hashboard(s) that could not be started; mining on the remaining \
                     boards (skip_unstartable_hashboards = true)"
                );
                selected.retain(|s| !unstartable.contains(&s.config.index));
                if selected.is_empty() {
                    return Err(BoardError::InitializationFailed(
                        "no present hashboards could be started".into(),
                    ));
                }
            } else {
                return Err(BoardError::InitializationFailed(format!(
                    "hashboard(s) {unstartable:?} could not be started (PIC handshake / DC-DC \
                     enable failed) — check the PIC / i2c wiring on those boards. To mine on the \
                     remaining boards, set skip_unstartable_hashboards = true under \
                     [hardware.amlogic_control_board]."
                )));
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

        let (freq_floor, freq_ceiling) = effective_freq_band(&present_models);
        state_tx.send_modify(|state| {
            state.name = board_name.clone();
            state.model = reported_label.clone();
            state.serial = primary_serial.clone();
            state.temperatures = all_temps.clone();
            state.fans = fan_states.clone();
            state.powers = power_states.clone();
            // Report the useful operating band + rail voltage resolved from the
            // present detected model(s) so a controller can dial/calibrate
            // within it instead of hardcoding per-model bounds.
            state.min_freq_mhz = Some(freq_floor);
            state.max_freq_mhz = Some(freq_ceiling);
            state.target_voltage_v = Some(effective_voltage.target_voltage);
        });

        if !psu_present {
            warn!(
                board = %board_name,
                hashboards = selected.len(),
                "Board is up in TELEMETRY-ONLY mode (no PSU on i2c): fans, temperatures and \
                 EEPROM are live, but no chains were started and the miner will not hash. \
                 Restore PSU power and restart mujina to mine."
            );
        }

        Ok((selected, psu, psu_present))
    }
}

#[async_trait]
impl Board for S19xAmlogic {
    fn board_info(&self) -> BoardInfo {
        let present_models: Vec<HashboardModel> =
            self.selected_hashboards.iter().map(|s| s.model).collect();
        BoardInfo {
            model: reported_model_label(&present_models),
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
        // Board-wide rail arbitration, shared by every chain on this APW12 so
        // pause/resume touch the rail exactly once per cycle (see `SharedRail`).
        let rail = Arc::new(Mutex::new(SharedRail::new(n_chains)));

        let ramp_coordinator = if n_chains > 1 {
            Some(Arc::new(bm13xx::chain_config::ChainCoordinator::new(
                n_chains,
            )))
        } else {
            None
        };

        // M4 thermal supervisor: one frequency cap (MHz) PER CHAIN so a
        // mixed chassis doesn't cap one family at the other's ceiling. Each
        // cap starts at its own detected model's operating max (uncapped)
        // and the telemetry task lowers/raises every cap off the hottest
        // sensor (the chains share one rail and airflow path, so they
        // throttle together — but each stays bounded by its own family's
        // ceiling). Each actor enforces `min(requested, cap)` on its tick.
        // Collected as `(cap, per-chain max)` pairs for the telemetry task.
        let mut thermal_caps: Vec<(Arc<AtomicU32>, u32)> = Vec::with_capacity(n_chains);

        let mut threads: Vec<Box<dyn HashThread>> = Vec::with_capacity(n_chains);
        let mut thread_state_seed: Vec<crate::api_client::types::ThreadState> =
            Vec::with_capacity(n_chains);

        // With no rail the chips are unpowered, so opening their UARTs and
        // running enumerate/ramp would only fail or hang. Skip chain creation
        // and come up telemetry-only: the task spawned at the end of this
        // function still drives the fans and reads temperatures, which is the
        // entire point of staying up without a PSU. Leaves `thermal_caps`
        // empty, which the telemetry task handles (nothing to throttle).
        let chains_to_start: Vec<SelectedHashboard> = if self.psu_present {
            self.selected_hashboards.clone()
        } else {
            warn!(
                board = %device_id(&self.config),
                hashboards = self.selected_hashboards.len(),
                "No PSU on i2c — skipping chain creation; running fans/telemetry only. \
                 Restore PSU power and restart mujina to mine."
            );
            Vec::new()
        };

        for selected in chains_to_start {
            let hb = &selected.config;
            // Per-hashboard chip-family spec from the DETECTED model (the
            // config's per-slot `model` is only a fallback). This drives the
            // chip config, topology, chain label, baud policy, and this
            // chain's thermal ceiling.
            let spec = hashboard_spec(selected.model);

            // Per-chain thermal cap seeded at this chain's own ceiling.
            let chain_thermal_cap = Arc::new(AtomicU32::new(spec.thermal_cap_max_mhz));
            thermal_caps.push((Arc::clone(&chain_thermal_cap), spec.thermal_cap_max_mhz));

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

            // Chip-UART baud is per-model (see `HashboardSpec.post_broadcast_baud`).
            // BM1366 (S19k) switches to 3.125 Mbaud after the broadcast phase and
            // needs the adapter to hold the original SerialControl alive so the
            // `/dev/ttyS*` fd never closes mid-init when the actor swaps
            // writer/reader pairs across the baud bump (per-hashboard so each
            // chain owns its own keepalive). BM1362 (S19j) keeps the fixed
            // SERIAL_BAUD — no reopen, so no adapter and `data_control` is just
            // dropped after the flush above.
            let chip_uart_baud: Option<
                Arc<Mutex<dyn bm13xx::chain_config::ChipUartBaudControl + Send>>,
            > = if spec.post_broadcast_baud.is_some() {
                Some(Arc::new(Mutex::new(SerialControlAdapter {
                    path: hb.serial_path.clone(),
                    staged_control: None,
                    _original_keepalive: Some(data_control),
                })))
            } else {
                drop(data_control);
                None
            };

            let config = ChainConfig {
                name: format!("{}-HB{}", spec.chain_label, hb.index),
                topology: spec.topology.clone(),
                chip_config: spec.chip_config.clone(),
                peripherals: ChainPeripherals {
                    asic_enable: Arc::new(Mutex::new(NativeResetControl {
                        gpio: SysfsGpio::new(hb.reset_gpio),
                        reset_release_ms: self.config.startup.reset_release_ms,
                    })),
                    voltage_regulator: Some(
                        Arc::clone(&self.psu) as Arc<Mutex<dyn VoltageRegulator + Send>>
                    ),
                    chip_uart_baud,
                    ramp_coordinator: ramp_coordinator.clone(),
                    // Slot index keys this chain's request in the shared-rail
                    // max-voltage aggregator (see `ChainCoordinator`).
                    chain_index: hb.index as usize,
                    thermal_cap_mhz: Some(Arc::clone(&chain_thermal_cap)),
                },
                // Per-model post-broadcast baud (see `HashboardSpec`): BM1366
                // switches to 3.125 Mbaud after the broadcast phase; BM1362
                // stays at the fixed SERIAL_BAUD.
                post_broadcast_chip_baud: spec.post_broadcast_baud,
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
                hashrate_1min: 0,
                is_active: false,
                active_chips: 0,
                expected_chips: 0,
                frequency_mhz: 0.0,
            });

            let board_thread = BoardStateHashThread::new(
                Box::new(inner_thread),
                self.state_tx.clone(),
                Arc::clone(&self.thread_states),
                Arc::clone(&self.psu),
                hb.reset_gpio,
                self.config.startup.reset_assert_ms,
                hb.eeprom_i2c_device.clone(),
                hb.index,
                self.config.clone(),
                Arc::clone(&rail),
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
        // Carry the EEPROM-DETECTED chip family into the telemetry task, not
        // the configured one. The task decides whether to run the PIC
        // heartbeat from `hb.model.expects_pic()`; on a mixed chassis a slot
        // configured `s19j_pro` may actually hold a noPIC S19k board
        // (`SelectedHashboard.model` already reflects the detected family and
        // drives the chain, voltage, and thermal everywhere else). Without
        // this override the heartbeat path keeps poking an absent PIC on every
        // detected-S19k slot and spams "PIC heartbeat failed / os error 6"
        // each tick. Overriding `model` with the detected family makes the
        // heartbeat decision consistent with the rest of the board.
        let hbs_clone: Vec<AmlogicHashboardConfig> = self
            .selected_hashboards
            .iter()
            .map(|s| {
                let mut cfg = s.config.clone();
                cfg.model = s.model;
                cfg
            })
            .collect();
        let psu = Arc::clone(&self.psu);
        let psu_present = self.psu_present;
        let state_tx = self.state_tx.clone();
        let thread_states = Arc::clone(&self.thread_states);
        let shutdown = self.telemetry_shutdown.child_token();
        tokio::spawn(async move {
            native_telemetry_task(
                cfg_clone,
                hbs_clone,
                psu,
                psu_present,
                state_tx,
                thread_states,
                thermal_caps,
                shutdown,
            )
            .await;
        });

        Ok(threads)
    }
}

/// Coordinates the single APW12 rail shared by every chain on this board.
///
/// Pause/resume arrive PER CHAIN (the scheduler calls `set_paused` on each
/// thread in turn), but the rail and the power-on-reset are BOARD-WIDE. Doing
/// the bring-up per chain is what made resume diverge from cold-init: the first
/// chain to resume asserted only ITS OWN reset and then energized the rail, so
/// every other chain's chips powered up free-running and enumerated as a random
/// subset (or hung, and then tripped INIT_TIMEOUT into Unstartable). Meanwhile
/// the first chain to PAUSE dropped the rail out from under chains that were
/// still mining.
///
/// Gate the board-wide steps here so they run exactly once per cycle, and only
/// when every chain agrees: the rail drops only after the LAST chain pauses,
/// and the power-on-reset runs on the FIRST chain to resume.
struct SharedRail {
    n_chains: usize,
    /// Chains currently paused. The rail drops only when all of them are.
    paused_chains: std::collections::HashSet<u8>,
    /// Whether the board-level power-on-reset has already run for the current
    /// resume cycle. Cleared when the rail drops.
    bringup_done: bool,
}

impl SharedRail {
    fn new(n_chains: usize) -> Self {
        Self {
            n_chains,
            paused_chains: std::collections::HashSet::new(),
            bringup_done: true,
        }
    }
}

/// THE board-level power-on-reset. Cold-init and resume both call this, so the
/// chips are brought up identically either way — which is the whole point: a
/// resume that does something subtly different from a restart is a resume that
/// fails in ways a restart doesn't.
///
/// Order matters and is not negotiable:
///   1. Drop the rail and hold it down long enough to discharge below the
///      chips' power-on-reset threshold.
///   2. Assert RST_N on EVERY chain — not just one. The rail is shared, so
///      energizing it releases power to all of them at once; any chain whose
///      reset isn't asserted powers up free-running and lands in an
///      indeterminate state that a later reset does not reliably clear.
///   3. Re-set the cold setpoint. The APW12 retains its last commanded voltage
///      across an enable cycle, so without this the chips get the operating
///      voltage on a cold start.
///   4. Enable, then let the APW12 stabilize.
/// Reset is released later, per chain, by `initialize_chips()`.
async fn board_power_on_reset(
    config: &AmlogicControlBoardConfig,
    psu: &Arc<Mutex<NativeAmlogicPsu>>,
) -> Result<(), BoardError> {
    {
        let mut guard = psu.lock().await;
        guard.set_enabled(false).map_err(|e| {
            BoardError::HardwareControl(format!("Failed to disable PSU for power-on-reset: {e}"))
        })?;
    }
    tokio::time::sleep(Duration::from_millis(config.startup.psu_off_settle_ms)).await;

    assert_all_resets(config)?;
    tokio::time::sleep(Duration::from_millis(config.startup.reset_assert_ms)).await;

    {
        let mut guard = psu.lock().await;
        if let Err(e) = guard.set_voltage(config.startup.initial_voltage).await {
            warn!(error = %e, "Failed to set the cold-start voltage during power-on-reset");
        }
        guard.set_enabled(true).map_err(|e| {
            BoardError::HardwareControl(format!("Failed to enable PSU after power-on-reset: {e}"))
        })?;
    }
    tokio::time::sleep(Duration::from_millis(config.startup.psu_settle_ms)).await;
    Ok(())
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
    /// Reset GPIO for the active hashboard. Driven low on resume
    /// *before* the PSU comes back up so chips power up while held in
    /// reset (matching the cold-boot `assert_all_resets()` step in
    /// `initialize()`), and released only when the actor's
    /// `initialize_chips()` runs.
    reset_gpio: u32,
    /// Hold time after asserting reset and before bringing the PSU
    /// back up. Without this, RST_N is asserted only nanoseconds
    /// before the rail comes alive and the daisy-chained signal does
    /// not propagate to all chips in time, leaving a random subset
    /// in indeterminate state. Mirrors the `reset_assert_ms` sleep
    /// inside cold-boot `assert_all_resets()`.
    reset_assert_ms: u64,
    /// i2c device path and PIC address used to re-handshake the on-
    /// hashboard PIC16F1704 on resume. The PIC's LDO is fed from the
    /// same 12 V rail we drop on pause, so it loses state and the
    /// per-domain DC-DCs come back disabled. Without re-running
    /// handshake → enable_dc_dc, only chips on domains that happen to
    /// power up on their own respond on UART.
    pic_i2c_device: PathBuf,
    pic_slot_index: u8,
    /// Full board config — needed so pause/resume can assert EVERY chain's
    /// reset, not just this one's (the rail they share is board-wide).
    config: AmlogicControlBoardConfig,
    /// Board-wide rail arbitration shared by all chains on this APW12.
    rail: Arc<Mutex<SharedRail>>,
}

impl BoardStateHashThread {
    fn new(
        inner: Box<dyn HashThread>,
        state_tx: watch::Sender<BoardState>,
        thread_states: Arc<std::sync::Mutex<Vec<crate::api_client::types::ThreadState>>>,
        psu: Arc<Mutex<NativeAmlogicPsu>>,
        reset_gpio: u32,
        reset_assert_ms: u64,
        pic_i2c_device: PathBuf,
        pic_slot_index: u8,
        config: AmlogicControlBoardConfig,
        rail: Arc<Mutex<SharedRail>>,
    ) -> Self {
        Self {
            inner,
            state_tx,
            thread_states,
            psu,
            reset_gpio,
            reset_assert_ms,
            pic_i2c_device,
            pic_slot_index,
            config,
            rail,
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
            hashrate_1min: u64::from(status.hashrate_1min),
            is_active,
            active_chips: status.active_chips,
            expected_chips: status.expected_chips,
            frequency_mhz: status.frequency_mhz,
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

    async fn set_voltage(&mut self, volts: f32) -> Result<(), HashThreadError> {
        // Pure forward — the rail is shared, so the inner actor sets the one
        // APW12; the scheduler orders this relative to set_frequency.
        self.inner.set_voltage(volts).await
    }

    async fn set_paused(&mut self, paused: bool) -> Result<(), HashThreadError> {
        // Hard pause: drop the chip power rail. The chain comes back
        // cold-booted on resume, which is the same path `start_async`
        // uses at process startup and is known to work — unlike the
        // UART `disable_chips()` path that gets only a subset of chips
        // back after re-enumeration.
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

            // Drive THIS chain's RST_N low before the rail can fall. Chips were
            // free-running; putting them into a known synchronous reset state
            // first avoids the indeterminate flip-flop states that survive a
            // brief power dip.
            if let Err(e) = SysfsGpio::new(self.reset_gpio).set_output_low() {
                warn!(error = %e, reset_gpio = self.reset_gpio, "Failed to assert reset on pause");
            }
            tokio::time::sleep(Duration::from_millis(self.reset_assert_ms)).await;

            // The rail is shared: dropping it on the FIRST chain's pause would
            // cut power to chains that are still mining. Only the LAST chain to
            // pause takes it down.
            let mut rail = self.rail.lock().await;
            rail.paused_chains.insert(self.pic_slot_index);
            if rail.paused_chains.len() >= rail.n_chains {
                if let Err(e) = self.psu.lock().await.set_enabled(false) {
                    warn!(error = %e, "All chains paused but PSU disable failed; chips still powered");
                } else {
                    info!(
                        thread = %self.inner.name(),
                        "Hard pause: all chains paused — PSU output disabled, chips drained"
                    );
                }
                // Next resume must redo the board-level power-on-reset.
                rail.bringup_done = false;
            } else {
                info!(
                    thread = %self.inner.name(),
                    paused = rail.paused_chains.len(),
                    of = rail.n_chains,
                    "Chain paused; leaving the shared rail up for the chains still mining"
                );
            }
            result
        } else {
            // Hard resume — run THE SAME board-level power-on-reset as cold
            // boot, exactly once per cycle, before any chain enumerates.
            //
            // This used to be a per-chain reimplementation of the cold-boot
            // sequence, and that is precisely why resume behaved differently
            // from a restart: each chain asserted only its OWN reset and then
            // energized the shared rail, so whichever chain resumed first
            // powered up every other chain's chips with reset released. Those
            // chips came up indeterminate, enumerated as a random subset, and
            // (post INIT_TIMEOUT) got marked Unstartable — while a full restart
            // recovered them, because cold-init asserts every reset before the
            // rail comes up. One sequence, one code path, both callers.
            {
                let mut rail = self.rail.lock().await;
                rail.paused_chains.remove(&self.pic_slot_index);
                if !rail.bringup_done {
                    if let Err(e) = board_power_on_reset(&self.config, &self.psu).await {
                        warn!(error = %e, "Board power-on-reset failed on resume; chain may not enumerate");
                        return Err(HashThreadError::InitializationFailed(format!(
                            "board power-on-reset failed on resume: {e}"
                        )));
                    }
                    rail.bringup_done = true;
                    info!(
                        thread = %self.inner.name(),
                        chains = rail.n_chains,
                        "Hard resume: board power-on-reset done (rail cycled, every chain held in reset, cold setpoint restored)"
                    );
                }
            }

            // PIC handshake — per hashboard, so it stays here rather than in
            // the shared sequence. The PIC16F1704 gates the per-domain DC-DCs
            // and is fed from the 12 V rail we just cycled, so without
            // re-running handshake -> enable_dc_dc only the domains that happen
            // to come up power-on survive. Best-effort: exhausting the retries
            // only WARNs and lets initialize_chips() surface the real failure.
            let pic_addr = pic_address_for_slot(self.pic_slot_index);
            match pic_handshake_and_enable_dc_dc(&self.pic_i2c_device, pic_addr).await {
                Ok(Some(version)) => {
                    info!(
                        addr = format_args!("0x{:02x}", pic_addr),
                        version = format_args!("0x{:02x}", version),
                        "PIC handshake ok on resume"
                    );
                }
                Ok(None) => {
                    debug!(
                        addr = format_args!("0x{:02x}", pic_addr),
                        "PIC absent on resume (noPIC variant?)"
                    );
                }
                Err(e) => {
                    warn!(
                        addr = format_args!("0x{:02x}", pic_addr),
                        attempts = PIC_RESUME_HANDSHAKE_ATTEMPTS,
                        error = %e,
                        "PIC handshake failed on resume after retries; chain may not respond on UART"
                    );
                }
            }

            info!(
                thread = %self.inner.name(),
                "Hard resume: chain armed; next UpdateTask releases reset and enumerates"
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
    /// Effective shared-rail voltage envelope. The APW12 powers EVERY
    /// hashboard, so its `voltage_range` / `target_voltage` / `voltage_step`
    /// / runtime clamp are resolved from the SET of present detected models
    /// (see [`effective_voltage_spec`]) — never from a single model — so the
    /// one rail always suits every present family. For a homogeneous chassis
    /// this equals that single model's spec exactly.
    voltage: EffectiveVoltageSpec,
}

impl NativeAmlogicPsu {
    fn new(config: &AmlogicControlBoardConfig, voltage: EffectiveVoltageSpec) -> Self {
        Self {
            i2c_device: config.psu.i2c_device.clone(),
            address: config.psu.address,
            write_register: config.psu.write_register,
            enable_gpio: config.psu.enable_gpio,
            enabled: false,
            voltage,
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

    async fn config_watchdog(&self, value: u8) -> anyhow::Result<()> {
        self.exchange(CMD_WATCHDOG, vec![value, 0x00]).await?;
        Ok(())
    }

    async fn measure_voltage(&self) -> anyhow::Result<f32> {
        let frame = self.exchange(CMD_MEASURE_VOLTAGE, Vec::new()).await?;
        if frame.payload.len() < 2 {
            return Err(anyhow::anyhow!("missing ADC payload from PSU"));
        }
        Ok(decode_measured_voltage(frame.payload[0], frame.payload[1]))
    }

    async fn read_target_voltage(&self) -> anyhow::Result<f32> {
        let frame = self.exchange(CMD_GET_VOLTAGE, Vec::new()).await?;
        let dac = *frame
            .payload
            .first()
            .ok_or_else(|| anyhow::anyhow!("missing DAC payload from PSU"))?;
        Ok(decode_dac_to_voltage(dac))
    }

    /// Run a PSU protocol exchange on tokio's blocking-thread-pool.
    ///
    /// The underlying i2c-dev SMBUS ioctl (and the retry sleeps around it, in
    /// [`exchange_blocking`]) are synchronous kernel calls with NO
    /// userspace-enforced timeout: if the APW12 or the i2c-1 bus wedges, the
    /// ioctl can block the calling OS thread forever. Running that inline on
    /// a tokio async worker thread would starve the whole runtime — including
    /// its own timer driver, which is what `tokio::time::timeout` needs to
    /// fire — if every worker thread ends up parked in one of these blocking
    /// calls. That is exactly what took mujina's scheduler task down on .222
    /// after a burst of dial commands: GET requests (served by unaffected
    /// worker threads) kept working, but the scheduler's own periodic tick
    /// and every subsequent command stopped forever, recoverable only by a
    /// PSU power-cycle + cold-init.
    ///
    /// `spawn_blocking` moves the exchange onto tokio's dedicated
    /// blocking-thread-pool (grown on demand, separate from the async worker
    /// pool), so a wedged bus can leak at most one blocking-pool thread —
    /// never the reactor. `NativeAmlogicPsu` doesn't hold an open file handle
    /// (each exchange opens its own `LinuxI2cDevice`), so cloning the small
    /// set of fields the blocking body needs into a `move` closure is cheap
    /// and doesn't require `Self: 'static` or any lock across the await.
    async fn exchange(
        &self,
        command: u8,
        payload: Vec<u8>,
    ) -> anyhow::Result<amlogic_cb_tools::protocol::Frame> {
        let i2c_device = self.i2c_device.clone();
        let address = self.address;
        let write_register = self.write_register;
        tokio::task::spawn_blocking(move || {
            exchange_blocking(&i2c_device, address, write_register, command, &payload)
        })
        .await
        .map_err(|e| anyhow::anyhow!("PSU exchange task panicked: {e}"))?
    }
}

/// Blocking body of [`NativeAmlogicPsu::exchange`]. MUST run via
/// `spawn_blocking` — see the doc comment there for why calling this inline
/// from async code is a real hang risk, not just a style nit.
fn exchange_blocking(
    i2c_device: &Path,
    address: u16,
    write_register: u8,
    command: u8,
    payload: &[u8],
) -> anyhow::Result<amlogic_cb_tools::protocol::Frame> {
    let mut dev = LinuxI2cDevice::open(i2c_device, address)?;
    let frame = build_frame(command, payload);
    for byte in frame {
        dev.write_byte_transaction(write_register, byte)?;
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

/// Open the on-hashboard PIC, handshake, and enable its DC-DCs — one
/// atomic blocking unit, retried as a whole by
/// [`pic_handshake_and_enable_dc_dc`]. Distinct outcomes so the retry loop
/// can tell "no PIC at this address" (don't bother retrying — likely a
/// noPIC variant) from "PIC present but didn't answer this time" (worth
/// retrying).
///
/// MUST run via `spawn_blocking` — `PicChain` opens the same kind of raw
/// `LinuxI2cDevice` as [`exchange_blocking`], with the same no-userspace-
/// timeout risk if called inline from async code.
fn pic_handshake_and_enable_dc_dc_blocking(
    pic_i2c_device: &Path,
    pic_addr: u16,
) -> Result<u8, PicHandshakeError> {
    let mut pic =
        PicChain::open(pic_i2c_device, pic_addr).map_err(PicHandshakeError::Absent)?;
    let version = pic.handshake().map_err(PicHandshakeError::Handshake)?;
    pic.enable_dc_dc().map_err(PicHandshakeError::EnableDcDc)?;
    Ok(version)
}

/// Outcome of [`pic_handshake_and_enable_dc_dc_blocking`] — kept distinct
/// (rather than collapsed into one `anyhow::Error`) so callers can decide
/// whether a failure is worth retrying.
enum PicHandshakeError {
    /// Opening the i2c device itself failed (not a bus/protocol error) —
    /// this hashboard likely has no PIC (noPIC variant); retrying won't help.
    Absent(amlogic_cb_tools::pic::PicError),
    /// The device opened but didn't complete the handshake — the
    /// transient case worth retrying (e.g. the PIC's LDO hasn't finished
    /// stabilizing yet right after the rail came back up on resume).
    Handshake(amlogic_cb_tools::pic::PicError),
    /// Handshake succeeded but enabling the per-domain DC-DCs failed.
    /// Distinct from `Handshake` mainly for logging; retrying re-runs the
    /// full open+handshake+enable sequence either way.
    EnableDcDc(amlogic_cb_tools::pic::PicError),
}

impl std::fmt::Display for PicHandshakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Absent(e) => write!(f, "PIC i2c open failed: {e}"),
            Self::Handshake(e) => write!(f, "PIC handshake failed: {e}"),
            Self::EnableDcDc(e) => write!(f, "PIC enable_dc_dc failed: {e}"),
        }
    }
}

/// Resume-time PIC handshake + DC-DC enable, retried up to
/// [`PIC_RESUME_HANDSHAKE_ATTEMPTS`] times on the transient "present but
/// didn't answer" case (see [`PicHandshakeError`]). Runs each attempt on
/// `spawn_blocking` (never inline — see [`pic_handshake_and_enable_dc_dc_blocking`]).
///
/// Returns `Ok(Some(version))` on success, `Ok(None)` if the PIC is absent
/// (no point retrying — likely a noPIC hashboard), or `Err` after
/// exhausting retries on a present-but-unresponsive PIC.
async fn pic_handshake_and_enable_dc_dc(
    pic_i2c_device: &Path,
    pic_addr: u16,
) -> Result<Option<u8>, String> {
    let mut last_err: Option<PicHandshakeError> = None;
    for attempt in 0..PIC_RESUME_HANDSHAKE_ATTEMPTS {
        let device = pic_i2c_device.to_path_buf();
        let result = tokio::task::spawn_blocking(move || {
            pic_handshake_and_enable_dc_dc_blocking(&device, pic_addr)
        })
        .await
        .map_err(|e| PicHandshakeError::Handshake(amlogic_cb_tools::pic::PicError::Io {
            addr: pic_addr,
            source: std::io::Error::other(format!("PIC task panicked: {e}")),
        }));

        match result {
            Ok(Ok(version)) => {
                if attempt > 0 {
                    warn!(
                        addr = format_args!("0x{:02x}", pic_addr),
                        attempt,
                        "PIC handshake succeeded after retry"
                    );
                }
                return Ok(Some(version));
            }
            Ok(Err(PicHandshakeError::Absent(_))) => return Ok(None),
            Ok(Err(e)) => last_err = Some(e),
            Err(e) => last_err = Some(e),
        }

        if attempt + 1 < PIC_RESUME_HANDSHAKE_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(PIC_RESUME_RETRY_DELAY_MS)).await;
        }
    }
    Err(last_err
        .map(|e| e.to_string())
        .unwrap_or_else(|| "PIC handshake retries exhausted".into()))
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
        // Runtime voltage clamp is the SHARED-rail intersection clamp (see
        // `EffectiveVoltageSpec::psu_clamp`); it is wider than the cold-init
        // `voltage_range` and only governs the runtime voltage bands. Resolved
        // from the effective spec so the clamp always honours every present
        // family's runtime floor/ceiling — never a single model's.
        let (clamp_min, clamp_max) = self.voltage.psu_clamp;
        let clamped = volts.clamp(clamp_min, clamp_max);
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
            match self.exchange(CMD_SET_VOLTAGE, vec![dac, 0x00]).await {
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
                    if let Ok(readback) = self.read_target_voltage().await {
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
        // Shared-rail cold-init clamp — intersection of every present
        // family's range (see `EffectiveVoltageSpec::voltage_range`).
        self.voltage.voltage_range
    }

    fn target_voltage(&self) -> f32 {
        // Shared-rail operating setpoint — MAX of present targets, clamped
        // into the intersections (see `EffectiveVoltageSpec::target_voltage`).
        self.voltage.target_voltage
    }

    fn voltage_step(&self) -> f32 {
        // MIN of present families' steps (see `EffectiveVoltageSpec::voltage_step`).
        self.voltage.voltage_step
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
) -> Result<(Option<String>, Option<HashboardModel>, Vec<TemperatureSensor>), BoardError> {
    let mut board_serial = None;
    // Chip family detected from the EEPROM. `None` when EEPROM reading is
    // disabled or the board is unrecognized; the caller falls back to the
    // configured model.
    let mut detected_model = None;

    if config.startup.health_gate.read_eeprom_before_mining {
        let eeprom = read_eeprom(hashboard)?;
        let decoded = decode_antminer_eeprom(&eeprom).map_err(|e| {
            BoardError::InitializationFailed(format!(
                "EEPROM health gate failed for hashboard {}: {e}",
                hashboard.index
            ))
        })?;
        match detect_hashboard_model(&decoded) {
            Some(model) => {
                info!(
                    hashboard = hashboard.index,
                    detected = %model.board_model_label(),
                    board_name = %decoded.board_name,
                    chip_marking = %decoded.chip_marking,
                    "hashboard family detected from EEPROM"
                );
                detected_model = Some(model);
            }
            None => {
                warn!(
                    hashboard = hashboard.index,
                    board_name = %decoded.board_name,
                    chip_marking = %decoded.chip_marking,
                    configured = %hashboard.model.board_model_label(),
                    "hashboard family detection from EEPROM failed (unrecognized board_name/chip_marking); \
                     falling back to configured model"
                );
            }
        }
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

    Ok((board_serial, detected_model, temperatures))
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

// --- M4 thermal throttle (graduated frequency reduction below the hard cutoff) ---
// The upper bound (no-throttle ceiling) is per-CHAIN (each chain's detected
// `HashboardSpec::thermal_cap_max_mhz`) and passed into the telemetry task as
// a `(cap, per-chain max)` list — a mixed chassis caps each family at its own
// ceiling, not the other family's.
/// Frequency floor (MHz) the throttle won't go below — if the board is still
/// too hot here, the 65 °C TMP75 hard cutoff takes over.
const THERMAL_CAP_MIN_MHZ: u32 = 200;
/// TMP75 °C at which to begin lowering the cap. The hard cutoff is 65 °C
/// TMP75 (≈85 °C die); start shedding ~7 °C below it.
const THERMAL_THROTTLE_START_C: f32 = 58.0;
/// TMP75 °C at which to slam the cap to the floor (just below the 65 °C cutoff).
const THERMAL_THROTTLE_HARD_C: f32 = 64.0;
/// TMP75 °C below which to restore the cap toward max (hysteresis band 54–58).
const THERMAL_THROTTLE_RELEASE_C: f32 = 54.0;
/// Cap step per telemetry tick (~2 s): throttle down fast, restore slowly.
const THERMAL_STEP_DOWN_MHZ: u32 = 25;
const THERMAL_STEP_UP_MHZ: u32 = 6;

/// Probe one hashboard's PIC for the heartbeat path and return the handle to
/// keep (or `None` for a genuine noPIC board). Logs its own outcome.
///
/// `PicChain::open` only opens the i2c bus — it does NOT probe the device — so
/// the heartbeat is the real presence test. That means a single noisy read
/// (observed on .222: `[0x56; 6]`, i.e. `resp[1] != 0x16`) must not be taken
/// as "no PIC": retry first. A genuine noPIC board (BHB56902) fails every
/// attempt; a present PIC that glitched once recovers on a retry. If every
/// attempt fails but the board is a known PIC variant, KEEP the handle anyway
/// — dropping it leaves the PIC's DC-DC watchdog unfed and the chips fall off
/// the chain a few seconds later (exactly the failure this guards against).
///
/// MUST run via `spawn_blocking` — `PicChain` uses the same raw, timeout-less
/// `LinuxI2cDevice` ioctl path as [`exchange_blocking`].
fn probe_pic_for_heartbeat_blocking(
    pic_i2c_device: &Path,
    pic_addr: u16,
    hb_index: u8,
    expects_pic: bool,
) -> Option<PicChain> {
    let mut pic = match PicChain::open(pic_i2c_device, pic_addr) {
        Ok(p) => p,
        Err(e) => {
            warn!(
                hashboard = hb_index,
                addr = format_args!("0x{:02x}", pic_addr),
                error = %e,
                "could not open PIC for heartbeat task; chips may drop after watchdog timeout"
            );
            return None;
        }
    };

    let mut last_err: Option<amlogic_cb_tools::pic::PicError> = None;
    for attempt in 0..PIC_HEARTBEAT_PROBE_ATTEMPTS {
        match pic.heartbeat() {
            Ok(()) => {
                if attempt > 0 {
                    info!(
                        hashboard = hb_index,
                        addr = format_args!("0x{:02x}", pic_addr),
                        attempt = attempt + 1,
                        "PIC heartbeat probe recovered after a retry (initial read was noise)"
                    );
                }
                return Some(pic);
            }
            Err(e) => last_err = Some(e),
        }
        if attempt + 1 < PIC_HEARTBEAT_PROBE_ATTEMPTS {
            std::thread::sleep(Duration::from_millis(PIC_RESUME_RETRY_DELAY_MS));
        }
    }

    let err = last_err
        .map(|e| e.to_string())
        .unwrap_or_else(|| "no response".into());
    if expects_pic {
        warn!(
            hashboard = hb_index,
            addr = format_args!("0x{:02x}", pic_addr),
            error = %err,
            attempts = PIC_HEARTBEAT_PROBE_ATTEMPTS,
            "PIC-variant board did not answer the heartbeat probe; keeping the handle \
             and retrying each tick so the DC-DC watchdog stays fed"
        );
        Some(pic)
    } else {
        info!(
            hashboard = hb_index,
            addr = format_args!("0x{:02x}", pic_addr),
            error = %err,
            "PIC absent (noPIC variant); skipping heartbeat path"
        );
        None
    }
}

#[allow(clippy::too_many_arguments)]
async fn native_telemetry_task(
    config: AmlogicControlBoardConfig,
    hashboards: Vec<AmlogicHashboardConfig>,
    psu: Arc<Mutex<NativeAmlogicPsu>>,
    // False when the APW12 never answered at bring-up. The chips then have no
    // rail, so there is nothing to measure and nothing generating heat.
    psu_present: bool,
    state_tx: watch::Sender<BoardState>,
    thread_states: Arc<std::sync::Mutex<Vec<crate::api_client::types::ThreadState>>>,
    // Per-chain frequency caps as `(cap, per-chain max)`. Each is stepped off
    // the board's hottest sensor but stays bounded by its own family ceiling.
    thermal_caps: Vec<(Arc<AtomicU32>, u32)>,
    shutdown: CancellationToken,
) {
    const TELEMETRY_INTERVAL: Duration = Duration::from_secs(2);

    // One PIC handle per present hashboard for the heartbeat path.
    // LuxOS sends a PIC heartbeat (opcode 0x16) roughly every 1.5 s on
    // PIC variants; without heartbeats the PIC disables DC-DC after a
    // watchdog timeout (chips drop mid-ramp). Probe each slot once at task
    // start on the blocking pool (raw i2c). `probe_pic_for_heartbeat_blocking`
    // retries so a single noisy i2c frame can't misclassify a PIC-variant
    // board as "noPIC", and only genuine noPIC variants (BHB56902) end up with
    // the heartbeat disabled.
    let probe_hbs = hashboards.clone();
    let mut pics: Vec<(u16, AmlogicHashboardConfig, Option<PicChain>)> =
        tokio::task::spawn_blocking(move || {
            probe_hbs
                .iter()
                .map(|hb| {
                    let pic_addr = pic_address_for_slot(hb.index);
                    let pic = probe_pic_for_heartbeat_blocking(
                        &hb.eeprom_i2c_device,
                        pic_addr,
                        hb.index,
                        hb.model.expects_pic(),
                    );
                    (pic_addr, hb.clone(), pic)
                })
                .collect()
        })
        .await
        .unwrap_or_else(|e| {
            error!(error = %e, "PIC heartbeat probe task panicked");
            Vec::new()
        });

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
        //
        // Runs on spawn_blocking: `pic.heartbeat()` / `read_temperatures_celsius()`
        // / `read_temperatures()` are all synchronous i2c calls (the same raw,
        // timeout-less `LinuxI2cDevice` ioctl path as `NativeAmlogicPsu::exchange` --
        // see its doc comment for why running these inline on the async runtime is
        // a real hang/starvation risk, not just a style nit). This loop, run inline,
        // was confirmed as the source of a real, periodic ~5-6s GET-latency spike on
        // .222 recurring roughly every telemetry tick: 3 boards x (heartbeat + up to
        // 4 temperature reads), all blocking with no yield point in between, was
        // enough to intermittently starve the axum GET handler's worker thread --
        // Nova reported the miner "unreachable" / flapping even though it was
        // mining fine the whole time. `pics` (holding the open per-board `PicChain`
        // handles) moves into the closure and back out; the individual `PicChain`
        // objects don't need to survive a panic in this closure (astronomically
        // unlikely, since none of these calls panic on ordinary I/O failure --
        // they return `Result`), so on that edge case telemetry for this board set
        // is simply lost rather than the process crashing.
        let (returned_pics, blocking_temperatures) = tokio::task::spawn_blocking(move || {
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
            (pics, temperatures)
        })
        .await
        .unwrap_or_else(|e| {
            error!(error = %e, "Telemetry PIC/temperature blocking task panicked");
            (Vec::new(), Vec::new())
        });
        pics = returned_pics;
        let temperatures = blocking_temperatures;

        // ----- Fan + overtemp control --------------------------------
        //
        // The Amlogic hashboards don't expose the BM13xx on-die thermal
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
        // Configurable floor (toml `startup.fan_floor_percent`, default 30),
        // clamped above the fan's stall point. Governs noise at low power/paused;
        // the curve still ramps to 100 % as the board heats.
        //
        // The lower bound is 10: the S19j/S19k chassis fans were measured still
        // spinning (~3100 RPM) at 20 % on a cool board, so the previous bound of
        // 20 was well above the actual stall point. 10 is kept as a floor because
        // a duty low enough to stall the fans reads as "fans configured" while
        // moving no air, which the tach-less code path cannot detect.
        let idle_floor_percent: u8 = config.startup.fan_floor_percent.clamp(10, 100);
        // While mining, floor at `fan_floor_mining_percent` instead. The curve
        // below keys off BOARD temperature, which lags the die by tens of
        // seconds during a frequency ramp: a floor picked to keep an idle board
        // quiet leaves the chips underserved exactly while they heat fastest
        // (observed on .187 — chips ran hot through the ramp at a 10 % floor).
        // The chains publish `is_active` as soon as the first job is dispatched,
        // which is *before* the ramp completes, so this covers the whole ramp.
        // Clamped to >= the idle floor so a lower mining value can never reduce
        // airflow while hashing.
        let mining = {
            let states = thread_states
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            states.iter().any(|t| t.is_active)
        };
        let fan_floor_percent: u8 = if mining {
            config
                .startup
                .fan_floor_mining_percent
                .clamp(10, 100)
                .max(idle_floor_percent)
        } else {
            idle_floor_percent
        };
        // Ramp window (toml `startup.fan_ramp_start_c` / `fan_ramp_full_c`,
        // default 40 → 60). Below start the fans sit at the floor; from start
        // they rise linearly to 100 % at full.
        //
        // Two invariants, both enforced here rather than trusted to the toml:
        //   - `full` never exceeds TMP75_OVERTEMP_C. The fans MUST be at 100 %
        //     before the over-temp gate cuts the PSU, or the board shuts itself
        //     down with cooling headroom still unspent.
        //   - `full` stays at least 1 °C above `start`, so the span can never be
        //     zero (which would divide to a NaN duty).
        // `start` is capped first so the two can't cross and invert the ramp.
        let fan_ramp_start_c: f32 = config.startup.fan_ramp_start_c.min(TMP75_OVERTEMP_C - 1.0);
        let fan_ramp_full_c: f32 = config
            .startup
            .fan_ramp_full_c
            .max(fan_ramp_start_c + 1.0)
            .min(TMP75_OVERTEMP_C);
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

        // ----- M4 thermal throttle ------------------------------------
        // Below the hard cutoff above, step each chain's frequency cap down
        // as the board heats toward it and back up as it cools. Every chain
        // reacts to the same hottest sensor (shared rail + airflow) but each
        // stays bounded by its own family's ceiling, so a mixed chassis never
        // caps one family at the other's max. Each actor enforces
        // min(requested, cap) on its 1 s tick, so the board sheds heat by
        // slowing down — staying alive at lower power instead of tripping the
        // full DC-DC/PSU cutoff. Runs with no dependency on Nova.
        for (chain_idx, (thermal_cap_mhz, thermal_cap_max_mhz)) in
            thermal_caps.iter().enumerate()
        {
            let thermal_cap_max_mhz = *thermal_cap_max_mhz;
            let cap = thermal_cap_mhz.load(Ordering::Relaxed);
            let new_cap = if temp_stale {
                // No temperature signal — be conservative (fans already 100 %).
                THERMAL_CAP_MIN_MHZ
            } else if let Some(t) = board_hottest {
                if t >= THERMAL_THROTTLE_HARD_C {
                    THERMAL_CAP_MIN_MHZ
                } else if t >= THERMAL_THROTTLE_START_C {
                    cap.saturating_sub(THERMAL_STEP_DOWN_MHZ)
                        .max(THERMAL_CAP_MIN_MHZ)
                } else if t <= THERMAL_THROTTLE_RELEASE_C {
                    (cap + THERMAL_STEP_UP_MHZ).min(thermal_cap_max_mhz)
                } else {
                    cap // hysteresis band — hold
                }
            } else {
                cap
            };
            if new_cap != cap {
                if new_cap < cap {
                    warn!(
                        chain = chain_idx,
                        hottest_c = board_hottest,
                        cap_mhz = new_cap,
                        "Thermal throttle: lowering frequency cap"
                    );
                } else {
                    info!(
                        chain = chain_idx,
                        hottest_c = board_hottest,
                        cap_mhz = new_cap,
                        "Thermal throttle: raising frequency cap"
                    );
                }
                thermal_cap_mhz.store(new_cap, Ordering::Relaxed);
            }
        }

        let target_fan_percent: u8 = if temp_stale && psu_present {
            warn!(
                "No temperature sample in 30 s — pinning fans to 100 % as a safety fallback"
            );
            100
        } else if temp_stale {
            // Same missing-sensor case, but with no rail the chips are
            // unpowered and generating nothing to cool — the sensors are simply
            // dead along with the PSU. Screaming at 100 % here is pure noise
            // (it is exactly what a powered-off miner used to do), so sit at the
            // idle floor instead.
            idle_floor_percent
        } else if let Some(t) = board_hottest {
            if t <= fan_ramp_start_c {
                fan_floor_percent
            } else if t >= fan_ramp_full_c {
                100
            } else {
                let span = fan_ramp_full_c - fan_ramp_start_c;
                let into_ramp = t - fan_ramp_start_c;
                let pct = fan_floor_percent as f32
                    + ((100.0 - fan_floor_percent as f32) * (into_ramp / span));
                pct.round().clamp(fan_floor_percent as f32, 100.0) as u8
            }
        } else {
            // Pre-mining / first tick — sit at the boot value but
            // never below the floor.
            config.startup.default_fan_percent.max(fan_floor_percent)
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
                    mining,
                    floor_percent = fan_floor_percent,
                    "Adjusted fan PWM"
                );
                applied_fan_percent = Some(target_fan_percent);
            }
        }

        let fans = read_fan_states(&config, target_fan_percent).await;
        // Read the rail voltage — but NEVER hold the `psu` mutex across an
        // unbounded i2c op.
        //
        // `measure_voltage` is a raw, timeout-less ioctl: on a dead or wedged
        // APW12 it blocks forever. Holding the lock across it wedged this task
        // *inside* the mutex, which deadlocked everything else that needs the
        // rail — `set_paused` (pause hung forever on `psu.lock()`), resume, and
        // most seriously the over-temp cutoff above, which simply stopped
        // running because this loop never came back around. Bounding it means a
        // dead PSU costs one skipped reading instead of a stuck miner. Skipped
        // entirely when the PSU never answered at bring-up — there's nothing to
        // measure and no reason to burn the timeout every tick.
        let voltage_v = if psu_present {
            match tokio::time::timeout(PSU_BRINGUP_OP_TIMEOUT, async {
                psu.lock().await.measure_voltage().await
            })
            .await
            {
                Ok(Ok(voltage_v)) => Some(voltage_v),
                Ok(Err(error)) => {
                    debug!(error = %error, "Native telemetry PSU voltage read failed");
                    None
                }
                Err(_elapsed) => {
                    warn!(
                        timeout = ?PSU_BRINGUP_OP_TIMEOUT,
                        "APW12 voltage read timed out (i2c-1 unresponsive); skipping this sample. \
                         The PSU rail may be wedged — pause/resume and the over-temp cutoff stay \
                         responsive regardless."
                    );
                    None
                }
            }
        } else {
            None
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
        model: board_model(&config).board_model_label().into(),
        serial: Some(name),
        ..Default::default()
    };
    let (state_tx, state_rx) = watch::channel(initial_state);

    let (selected_hashboards, psu, psu_present) = S19xAmlogic::initialize(&config, &state_tx)
        .await
        .map_err(|e| Error::Hardware(format!("Failed to initialize native Amlogic board: {e}")))?;

    let board = S19xAmlogic::new(config, selected_hashboards, psu, psu_present, state_tx);
    let registration = super::BoardRegistration { state_rx };
    Ok((Box::new(board), registration))
}

inventory::submit! {
    VirtualBoardDescriptor {
        device_type: "s19x_amlogic",
        name: UNIFIED_BOARD_MODEL,
        create_fn: || Box::pin(create_amlogic_board()),
    }
}
