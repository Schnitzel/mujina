//! Command types sent from API handlers to backend components.
//!
//! Each command carries a oneshot reply channel so the handler can
//! await the result and translate it into an HTTP response.

use anyhow::Result;
use tokio::sync::oneshot;

/// Commands from the API to the scheduler.
pub enum SchedulerCommand {
    /// Pause job distribution to all threads.
    PauseMining { reply: oneshot::Sender<Result<()>> },

    /// Resume job distribution after a pause.
    ResumeMining { reply: oneshot::Sender<Result<()>> },

    /// Set the chip frequency (MHz) on every chain — the V1 power dial.
    /// Each thread clamps to its safe runtime range and re-ramps its PLL.
    SetFrequency {
        mhz: f32,
        reply: oneshot::Sender<Result<()>>,
    },
}

/// Commands from the API to board management.
pub enum BoardCommand {
    /// Set a fan's target duty cycle on a specific board.
    SetFanTarget {
        board: String,
        fan: String,
        /// Target duty cycle (0--100), or None for automatic control.
        percent: Option<u8>,
        reply: oneshot::Sender<Result<()>>,
    },
}
