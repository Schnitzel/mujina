//! API data transfer objects.
//!
//! These types define the API contract shared between the server and
//! clients (CLI, TUI). See `docs/api.md` (at the repository root)
//! for the full API contract documentation, including conventions
//! for null values and units.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Full miner state snapshot.
#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
pub struct MinerState {
    pub uptime_secs: u64,
    /// Aggregate hashrate in hashes per second (5-minute window).
    pub hashrate: u64,
    /// Aggregate hashrate over the responsive 1-minute window (hashes per
    /// second) — settles ~5× faster than `hashrate` after a power dial.
    #[serde(default)]
    pub hashrate_1min: u64,
    pub shares_submitted: u64,
    /// Best share difficulty observed since startup, if any shares were found.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub best_difficulty: Option<u64>,
    pub paused: bool,
    pub boards: Vec<BoardState>,
    pub sources: Vec<SourceState>,
}

/// Board status.
#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
pub struct BoardState {
    /// URL-friendly identifier (e.g. "bitaxe-e2f56f9b").
    pub name: String,
    pub model: String,
    pub serial: Option<String>,
    pub fans: Vec<Fan>,
    pub temperatures: Vec<TemperatureSensor>,
    pub powers: Vec<PowerMeasurement>,
    pub threads: Vec<ThreadState>,
    /// Useful runtime operating-frequency floor (MHz) for this board's present
    /// hashboard model(s). A controller (e.g. Nova) should dial/calibrate no
    /// lower than this — it's the efficient/stable floor, above the hard clamp.
    /// `None` on boards/builds that don't report a band.
    #[serde(default)]
    pub min_freq_mhz: Option<f32>,
    /// Useful runtime operating-frequency ceiling (MHz) — the shared-rail
    /// intersection of every present model's operating max (each model's
    /// cold-init target, matching the runtime `SetFrequency` clamp). A single
    /// operating point commanded above this is clamped by the miner. `None` if
    /// not reported.
    #[serde(default)]
    pub max_freq_mhz: Option<f32>,
    /// Effective operating (rail) voltage (V) mujina holds for the present
    /// model(s) — resolved from their shared-PSU envelope. A controller should
    /// command this as the operating-point voltage rather than guessing a
    /// per-frequency band. `None` if not reported.
    #[serde(default)]
    pub target_voltage_v: Option<f32>,
}

/// Fan status.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct Fan {
    pub name: String,
    /// Measured RPM, or null if the tachometer read failed.
    pub rpm: Option<u32>,
    /// Measured duty cycle, or null if the read failed.
    pub percent: Option<u8>,
    /// Target duty cycle, or null if the fan is in automatic mode.
    pub target_percent: Option<u8>,
}

/// Temperature sensor reading.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct TemperatureSensor {
    pub name: String,
    pub temperature_c: Option<f32>,
}

/// Voltage, current, and power from a single measurement point.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct PowerMeasurement {
    pub name: String,
    pub voltage_v: Option<f32>,
    pub current_a: Option<f32>,
    pub power_w: Option<f32>,
}

/// Per-thread runtime status.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ThreadState {
    pub name: String,
    /// Hashrate in hashes per second (5-minute window).
    pub hashrate: u64,
    /// Responsive hashrate over a 1-minute window (hashes per second). Reflects
    /// an operating-point change ~5× faster than `hashrate`. 0 on older builds.
    #[serde(default)]
    pub hashrate_1min: u64,
    pub is_active: bool,
    /// Distinct chips that have produced a nonce within the recent census
    /// window (passive, derived from the nonce stream). Pair with
    /// `expected_chips` to distinguish "some chips gone silent" from a uniform
    /// per-chip throughput drop.
    #[serde(default)]
    pub active_chips: u16,
    /// Total chips expected on this chain.
    #[serde(default)]
    pub expected_chips: u16,
    /// Current chip frequency (MHz) applied to this chain — the live operating
    /// point of the power dial. 0 when idle/paused.
    #[serde(default)]
    pub frequency_mhz: f32,
}

/// Writable fields for `PATCH /api/v0/miner`.
///
/// All fields are optional; only those present in the request body are
/// applied. Read-only fields like `uptime_secs` and `hashrate` are not
/// included and cannot be set.
#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
pub struct MinerPatchRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paused: Option<bool>,

    /// Target chip frequency in MHz — the V1 power dial. Clamped to the
    /// miner's safe runtime range and applied to every chain by re-ramping
    /// the PLL at the existing voltage. `None` leaves frequency unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_freq_mhz: Option<f32>,

    /// Target chain voltage in volts (M1.5). When set, the request is applied
    /// as an operating-point change (`target_freq_mhz` must also be set): the
    /// miner sequences frequency and voltage in the V/f-safe order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_voltage_v: Option<f32>,
}

/// Request body for setting a fan's target duty cycle.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct SetFanTargetRequest {
    /// Target duty cycle percentage (0--100), or null for automatic control.
    pub target_percent: Option<u8>,
}

/// Job source status.
#[derive(Clone, Debug, Default, Deserialize, Serialize, ToSchema)]
pub struct SourceState {
    pub name: String,
    /// Connection URL (e.g. "stratum+tcp://pool:3333"), if applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Current share difficulty set by the source.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub difficulty: Option<u64>,
}
