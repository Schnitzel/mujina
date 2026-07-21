//! Configuration management for mujina-miner.
//!
//! This module handles loading and validating configuration from TOML files,
//! environment variables, and command-line arguments. It supports hot-reload
//! via file watching.

use anyhow::{Context, anyhow};
use serde::{Deserialize, Serialize};
use std::{
    env, fs,
    path::{Path, PathBuf},
};

/// Main configuration structure for the miner.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    /// Daemon configuration
    pub daemon: DaemonConfig,

    /// Pool configuration
    pub pools: Vec<PoolConfig>,

    /// Hardware configuration
    pub hardware: HardwareConfig,

    /// API server configuration
    pub api: ApiConfig,
}

/// Daemon process configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct DaemonConfig {
    /// PID file location
    pub pid_file: Option<PathBuf>,

    /// Log level
    pub log_level: String,

    /// Use systemd notification
    #[serde(default)]
    pub systemd: bool,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            pid_file: None,
            log_level: "info".to_string(),
            systemd: false,
        }
    }
}

/// Pool connection configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PoolConfig {
    /// Pool URL (stratum+tcp://...)
    pub url: String,

    /// Worker name
    pub worker: String,

    /// Password (if required)
    pub password: Option<String>,

    /// Priority (lower is higher priority)
    #[serde(default)]
    pub priority: u32,
}

/// Hardware configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct HardwareConfig {
    /// Temperature limits
    pub temp_limit: f32,

    /// Fan control settings
    pub fan_min_rpm: u32,
    pub fan_max_rpm: u32,

    /// Power limits
    pub power_limit: Option<f32>,

    /// Native Antminer Amlogic control-board configuration.
    ///
    /// This is optional so non-Amlogic builds and existing development flows
    /// can continue to use the generic hardware settings until the runtime
    /// wiring is implemented.
    #[serde(default)]
    pub amlogic_control_board: Option<AmlogicControlBoardConfig>,
}

impl Default for HardwareConfig {
    fn default() -> Self {
        Self {
            temp_limit: 85.0,
            fan_min_rpm: 0,
            fan_max_rpm: 10_000,
            power_limit: None,
            amlogic_control_board: None,
        }
    }
}

/// Native Antminer Amlogic control-board configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AmlogicControlBoardConfig {
    /// Enables the native Amlogic board path.
    pub enabled: bool,

    /// Stable API-visible board name override.
    pub board_name: Option<String>,

    /// PSU configuration for the shared APW12 power supply.
    pub psu: AmlogicPsuConfig,

    /// Startup timings and defaults.
    pub startup: AmlogicStartupConfig,

    /// Configured fan/tach endpoints.
    pub fans: Vec<AmlogicFanConfig>,

    /// Configured control-board LEDs.
    pub leds: Option<AmlogicLedConfig>,

    /// Expected hashboards connected to the control board.
    pub hashboards: Vec<AmlogicHashboardConfig>,

    /// Optional GT Touch USB CDC display integration.
    #[serde(default)]
    pub gt_touch_display: Option<GtTouchDisplayConfig>,
}

/// GT Touch USB CDC display configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct GtTouchDisplayConfig {
    /// Enable GT Touch integration.
    pub enabled: bool,

    /// Explicit CDC serial device path (for example `/dev/ttyACM0`).
    ///
    /// When omitted on Linux, Mujina attempts to auto-detect a GT Touch CDC
    /// device by walking `/sys/class/tty`.
    pub serial_path: Option<PathBuf>,

    /// Baud rate used when opening the CDC ACM port.
    ///
    /// USB CDC ACM ignores this electrically, but the host stack still
    /// requires a line coding value when opening the port.
    pub baud_rate: u32,

    /// How often to publish subscribed metrics to the display.
    pub update_interval_ms: u64,

    /// Delay before reconnecting after a disconnect or open failure.
    pub reconnect_delay_ms: u64,
}

impl Default for GtTouchDisplayConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            serial_path: None,
            baud_rate: 115_200,
            update_interval_ms: 2_000,
            reconnect_delay_ms: 2_000,
        }
    }
}

/// APW12 PSU configuration for the Amlogic control board.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AmlogicPsuConfig {
    /// Linux I2C device used for the PSU bus.
    pub i2c_device: PathBuf,

    /// APW12 I2C address.
    pub address: u16,

    /// Write register used by the board's APW12 bridge.
    pub write_register: u8,

    /// Active-low GPIO controlling PSU enable.
    pub enable_gpio: u32,
}

/// Startup behavior and safety defaults for native Amlogic bring-up.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AmlogicStartupConfig {
    /// Default fan duty cycle applied before ASIC bring-up. Defaults to 100 —
    /// full airflow for the pre-mining window where there's no temperature
    /// signal yet to drive the curve (a lower open-loop default here was the
    /// original mechanism behind boards running hot through early bring-up).
    #[serde(default = "default_default_fan_percent")]
    pub default_fan_percent: u8,

    /// Minimum fan duty (%) the temperature curve floors at when the board is
    /// cool (below the ramp-start temperature) — i.e. when mining at low power
    /// or paused. Lower is quieter; the board layer clamps it to a safe range
    /// above the fan's stall point (currently >= 10). Defaults to 30 (the curve
    /// still ramps to 100 % as the board heats, with a hard over-temp cutoff,
    /// regardless). Verify your chassis fans still turn at the duty you pick —
    /// below their stall point they stop entirely while still reading as "set".
    #[serde(default = "default_fan_floor_percent")]
    pub fan_floor_percent: u8,

    /// Minimum fan duty (%) used in place of [`Self::fan_floor_percent`] while
    /// any chain is actively mining.
    ///
    /// The fan curve keys off BOARD temperature, which lags the die by tens of
    /// seconds during a frequency ramp — so a floor chosen to keep an idle
    /// board quiet leaves the chips underserved exactly while they heat
    /// fastest. This floor applies from the moment the first job is dispatched
    /// (so it covers the whole ramp) until mining stops, letting you run a
    /// quiet idle floor without starving the ramp of airflow.
    ///
    /// Never drops below `fan_floor_percent`. Defaults to 30, matching the idle
    /// default — i.e. no behaviour change unless you lower one of them.
    #[serde(default = "default_fan_floor_mining_percent")]
    pub fan_floor_mining_percent: u8,

    /// Board temperature (°C) at which the fan curve starts rising above the
    /// floor. Below this the fans sit at the floor; from here they ramp
    /// linearly to 100 % at [`Self::fan_ramp_full_c`].
    ///
    /// Note this is the BOARD sensor (TMP75/PIC), which lags the chip die by
    /// tens of seconds — so the die is already meaningfully hotter than this
    /// number. Lower it to start cooling earlier. Defaults to 30.
    #[serde(default = "default_fan_ramp_start_c")]
    pub fan_ramp_start_c: f32,

    /// Board temperature (°C) at which the fan curve reaches 100 %. Defaults to
    /// 60.
    ///
    /// Held to at least `fan_ramp_start_c + 1` so the ramp always has a span,
    /// and capped at the hard over-temp cutoff (65) — the fans must be at 100 %
    /// *before* the gate cuts the PSU, otherwise the board shuts itself down
    /// with cooling still in reserve. Raise this to stay quieter at cruise: it
    /// stretches the ramp, so any given temperature maps to a lower duty.
    #[serde(default = "default_fan_ramp_full_c")]
    pub fan_ramp_full_c: f32,

    /// Initial PSU output voltage used for first BM1362 enumeration.
    ///
    /// This should be a low bring-up voltage. The BM13xx thread ramps the PSU
    /// from this starting point up to the operating voltage as frequency is
    /// increased.
    pub initial_voltage: f32,

    /// Delay after enabling the PSU before dependent operations begin.
    pub psu_settle_ms: u64,

    /// How long to hold the PSU rail OFF at the start of cold-init, before
    /// asserting reset and bringing it back up.
    ///
    /// mujina is SIGKILLed on stop, which leaves the PSU enable line asserted —
    /// so on a restart the rail is still up and the chips are still clocked from
    /// the previous run. Asserting RST_N alone does not recover that; the chips
    /// have to come up *into* reset from an unpowered rail. Dropping the rail
    /// here makes cold-init idempotent whatever state was inherited.
    ///
    /// Must be long enough for the rail to discharge below the chips'
    /// power-on-reset threshold. Defaults to 2000 ms.
    #[serde(default = "default_psu_off_settle_ms")]
    pub psu_off_settle_ms: u64,

    /// Time to hold hashboard reset active during initialization.
    pub reset_assert_ms: u64,

    /// Delay after releasing reset before enumeration starts.
    pub reset_release_ms: u64,

    /// Health-gate policy applied before mining starts.
    pub health_gate: AmlogicHealthGateConfig,
}

fn default_fan_floor_percent() -> u8 {
    30
}

fn default_fan_floor_mining_percent() -> u8 {
    30
}

fn default_psu_off_settle_ms() -> u64 {
    2000
}

fn default_default_fan_percent() -> u8 {
    100
}

fn default_fan_ramp_start_c() -> f32 {
    30.0
}

fn default_fan_ramp_full_c() -> f32 {
    60.0
}

/// Pre-mining validation policy.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AmlogicHealthGateConfig {
    /// Require EEPROM read success before mining starts.
    pub read_eeprom_before_mining: bool,

    /// Require temperature sensor read success before mining starts.
    pub read_temperatures_before_mining: bool,

    /// Whether a configured-but-missing hashboard is fatal.
    pub fail_on_missing_expected_hashboard: bool,

    /// When a *present* hashboard cannot be started — e.g. a PIC-variant board
    /// (S19j Pro) whose on-hashboard PIC won't handshake or enable its DC-DC,
    /// so its chips can never power up — whether to skip it and mine on the
    /// remaining boards.
    ///
    /// Defaults to `false`: an unstartable hashboard is NOT silently ignored.
    /// Board init fails with a clear error naming the bad slot, so the operator
    /// notices and fixes the hardware instead of the miner quietly running at
    /// reduced capacity. Set to `true` to drop the unstartable board(s) and
    /// bring the rest of the chassis up in a degraded mode.
    #[serde(default)]
    pub skip_unstartable_hashboards: bool,
}

/// Configured fan endpoint.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AmlogicFanConfig {
    /// Logical fan index exposed by the API.
    pub index: u8,

    /// PWM chip number.
    pub pwm_chip: u32,

    /// PWM channel driving this fan or fan group.
    pub pwm_channel: u32,

    /// Tachometer GPIO for RPM measurement.
    pub tach_gpio: u32,

    /// Pulses per revolution for RPM conversion.
    pub pulses_per_rev: u32,
}

/// LED GPIO configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AmlogicLedConfig {
    /// Green status LED GPIO.
    pub green_gpio: u32,

    /// Red status LED GPIO.
    pub red_gpio: u32,
}

/// Configured hashboard connection.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AmlogicHashboardConfig {
    /// Logical hashboard slot index.
    pub index: u8,

    /// Hashboard model expected in this slot.
    pub model: HashboardModel,

    /// UART device used for the ASIC chain.
    pub serial_path: PathBuf,

    /// Reset GPIO for the slot.
    pub reset_gpio: u32,

    /// Presence detect GPIO for the slot.
    pub detect_gpio: u32,

    /// Linux I2C device used for TMP75 sensors.
    pub temp_i2c_device: PathBuf,

    /// Explicit TMP75 sensor addresses for this hashboard's temperature path.
    ///
    /// When empty, Mujina falls back to the legacy address mapping derived from
    /// `index`. This allows early bring-up configs to decouple the UART/reset
    /// slot from the sensor address map on boards where those are not aligned.
    #[serde(default)]
    pub temp_sensor_addresses: Vec<u16>,

    /// Linux I2C device used for the hashboard EEPROM.
    pub eeprom_i2c_device: PathBuf,

    /// Explicit EEPROM I2C address for this hashboard's identity path.
    ///
    /// When omitted, Mujina falls back to the legacy address mapping derived
    /// from `index`.
    #[serde(default)]
    pub eeprom_address: Option<u16>,

    /// Whether absence of this configured board should be treated as fatal.
    #[serde(default)]
    pub required: bool,
}

/// Supported hashboard types for config-driven native Amlogic bring-up.
///
/// The variant of the *first* configured hashboard selects which board
/// driver the daemon dispatches to. A mixed-model chassis isn't
/// supported today; all hashboards in the config should share the
/// same `model`.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HashboardModel {
    /// BHB42601 / BHB42611 (S19j Pro, S19j Pro+) — BM1362 chips, 126
    /// per board across 42 voltage domains.
    S19jPro,
    /// BHB56902 (S19k Pro) — BM1366 / BM1366BS chips, ~77 per board
    /// across 11 voltage domains.
    S19kPro,
}

impl HashboardModel {
    /// Friendly board-model name surfaced via the API (matches the
    /// `BOARD_MODEL` constants in the corresponding board impls).
    pub fn board_model_label(self) -> &'static str {
        match self {
            HashboardModel::S19jPro => "S19j Pro (Amlogic control board)",
            HashboardModel::S19kPro => "S19k Pro (Amlogic control board)",
        }
    }

    /// Chip family identifier shown on the GT Touch display.
    pub fn asic_model_label(self) -> &'static str {
        match self {
            HashboardModel::S19jPro => "BM1362",
            HashboardModel::S19kPro => "BM1366",
        }
    }

    /// Whether this board family carries an on-hashboard PIC microcontroller
    /// gating the per-domain DC-DCs. PIC variants (S19j Pro — BHB42601 /
    /// BHB42611) must be handshaked and heartbeated; noPIC variants (S19k Pro
    /// — BHB56902) bring their DC-DCs up directly. Used so a PIC-variant board
    /// is never mistaken for a noPIC one just because its PIC returned a noisy
    /// i2c frame on a single probe.
    pub fn expects_pic(self) -> bool {
        matches!(self, HashboardModel::S19jPro)
    }
}

/// API server configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ApiConfig {
    /// Listen address
    pub listen: String,

    /// Enable TLS
    #[serde(default)]
    pub tls: bool,

    /// TLS certificate path
    pub cert_path: Option<PathBuf>,

    /// TLS key path
    pub key_path: Option<PathBuf>,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:7785".to_string(),
            tls: false,
            cert_path: None,
            key_path: None,
        }
    }
}

impl Config {
    /// Load configuration from the default location.
    pub fn load() -> anyhow::Result<Self> {
        Self::load_optional()?.ok_or_else(|| {
            anyhow!(
                "No Mujina config file found. Set MUJINA_CONFIG or place mujina.toml in ~/.config/mujina/ or /etc/mujina/."
            )
        })
    }

    /// Load configuration if a config file is present.
    pub fn load_optional() -> anyhow::Result<Option<Self>> {
        let Some(path) = Self::find_default_path()? else {
            return Ok(None);
        };

        Self::load_from(&path).map(Some)
    }

    /// Load configuration from a specific file.
    pub fn load_from(path: &Path) -> anyhow::Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file {}", path.display()))?;

        toml::from_str(&raw)
            .with_context(|| format!("Failed to parse TOML config {}", path.display()))
    }

    /// Return the enabled Amlogic control-board config, if configured.
    pub fn enabled_amlogic_control_board(&self) -> Option<&AmlogicControlBoardConfig> {
        self.hardware
            .amlogic_control_board
            .as_ref()
            .filter(|config| config.enabled)
    }

    fn find_default_path() -> anyhow::Result<Option<PathBuf>> {
        if let Some(path) = env::var_os("MUJINA_CONFIG") {
            let path = PathBuf::from(path);
            if !path.exists() {
                return Err(anyhow!(
                    "MUJINA_CONFIG points to missing file {}",
                    path.display()
                ));
            }
            return Ok(Some(path));
        }

        for path in Self::default_search_paths()? {
            if path.exists() {
                return Ok(Some(path));
            }
        }

        Ok(None)
    }

    fn default_search_paths() -> anyhow::Result<Vec<PathBuf>> {
        let mut paths = Vec::new();

        if let Some(home) = env::var_os("HOME") {
            paths.push(PathBuf::from(home).join(".config/mujina/mujina.toml"));
        }

        paths.push(PathBuf::from("/etc/mujina/mujina.toml"));
        Ok(paths)
    }
}
