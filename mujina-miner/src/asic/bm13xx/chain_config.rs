//! Chain configuration for BM13xx hash thread.
//!
//! This module defines the configuration that boards provide to the hash thread
//! implementation. A board may have multiple chains, each with its own config.
//!
//! # Example
//!
//! ```ignore
//! // Board creates configuration
//! let config = ChainConfig {
//!     name: "EmberOne".to_string(),
//!     topology: TopologySpec::individual_domains(12, false),
//!     chip_config: bm13xx::bm1362(),
//!     peripherals: ChainPeripherals {
//!         asic_enable: Arc::new(Mutex::new(gpio_enable)),
//!         voltage_regulator: Some(Arc::new(Mutex::new(tps546))),
//!     },
//! };
//!
//! // Hash thread uses configuration
//! let mut chain = Chain::from_topology(&config.topology);
//! chain.assign_addresses()?;
//! let sequencer = Sequencer::new(config.chip_config.clone());
//! ```

use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use tokio::sync::Mutex;

use super::chip_config::ChipConfig;
use super::topology::TopologySpec;

// Re-export from parent module
pub use crate::asic::hash_thread::{AsicEnable, VoltageRegulator};

/// Configuration for a BM13xx ASIC chain.
///
/// Provided by the board, used by the hash thread implementation. Contains
/// all information needed to initialize and operate the chain.
pub struct ChainConfig {
    /// Human-readable name for logging.
    pub name: String,

    /// Physical topology (chain routing, domains).
    pub topology: TopologySpec,

    /// Chip configuration with board-specific overrides.
    pub chip_config: ChipConfig,

    /// Hardware control interfaces for this chain.
    pub peripherals: ChainPeripherals,

    /// After the broadcast register-config phase completes, the hash
    /// thread sends a UartBaud broadcast to switch the chips to this
    /// baud rate, then calls [`ChainPeripherals::set_chip_uart_baud`]
    /// to switch the controller side. Per-chip writes and steady-state
    /// mining traffic then run at this rate.
    ///
    /// `None` keeps the link at the rate the SerialStream was opened
    /// with (default 115200). Set this to a higher rate (e.g.
    /// `BaudRate::Baud3M`) on boards with long chains where 115200 is
    /// the chain throughput bottleneck — confirmed for BHB56902 via the
    /// LuxOS capture (see `captures/luxos-bhb56902-findings.md`).
    pub post_broadcast_chip_baud: Option<super::protocol::BaudRate>,
}

/// Hardware interfaces for a chain.
///
/// These are trait objects with Arc because:
/// - The board may retain control over enable (shared with hash thread)
/// - Voltage regulators may be shared among multiple chains/threads
/// - Shared ownership naturally uses Arc<dyn Trait> with type erasure
///
/// Uses `tokio::sync::Mutex` rather than `std::sync::Mutex` to avoid
/// blocking worker threads. While peripheral I/O is fast, the async mutex
/// ensures we never accidentally block other tasks on the same worker.
pub struct ChainPeripherals {
    /// Enable/disable the ASIC chain.
    ///
    /// This abstraction covers different mechanisms:
    /// - Reset GPIO (assert = inactive/low-power, deassert = active)
    /// - Power enable (cut power = inactive, power on = active)
    /// - Board-specific implementations
    ///
    /// When disabled, chips are in low-power state. When enabled, chips
    /// need full re-initialization (all configuration is lost).
    pub asic_enable: Arc<Mutex<dyn AsicEnable + Send>>,

    /// Voltage regulator control (optional, may be shared across chains).
    pub voltage_regulator: Option<Arc<Mutex<dyn VoltageRegulator + Send>>>,

    /// Optional handle for retuning the controller-side chip UART baud
    /// rate mid-init. Boards that own the [`SerialStream`] for the chip
    /// channel can wrap its `SerialControl` here so the hash thread can
    /// switch to a higher rate after sending the chip-side
    /// [`UartBaud`](super::protocol::Register::UartBaud) broadcast.
    pub chip_uart_baud: Option<Arc<Mutex<dyn ChipUartBaudControl + Send>>>,

    /// Optional cross-chain ramp coordinator. When multiple chains
    /// share a single voltage rail (e.g. all three hashboards on an
    /// S19k Pro share the APW12), independent per-actor voltage
    /// commands would race on the shared mutex and cause the rail to
    /// oscillate between chains' step values. Setting this on every
    /// chain in the cohort makes them rendezvous at each ramp step
    /// and elect one leader per step to drive the shared PSU.
    /// `None` keeps the legacy single-chain behaviour where each
    /// actor commands its own voltage every step.
    pub ramp_coordinator: Option<Arc<ChainCoordinator>>,

    /// Local thermal frequency cap (MHz), written by the board's telemetry
    /// task and enforced by every actor on its 1 s tick: the effective
    /// frequency is `min(requested, cap)`. This is the M4 safety supervisor —
    /// a graduated throttle that reduces frequency as the board heats toward
    /// the hard cutoff, independent of any external controller. `None` (e.g.
    /// CPU threads) means no thermal throttle.
    pub thermal_cap_mhz: Option<Arc<AtomicU32>>,
}

/// Cross-chain coordinator for shared-rail boards.
///
/// All chains in the cohort call [`ChainCoordinator::sync_voltage_step`]
/// at the top of every frequency-ramp step. The call rendezvous on a
/// barrier (so the slowest chain holds the rest), the elected leader
/// drives the shared voltage regulator on behalf of everyone, and all
/// chains then wait the same `voltage_settle` before returning. After
/// the call returns each chain sends its per-chain PLL command at the
/// same effective voltage.
pub struct ChainCoordinator {
    barrier: tokio::sync::Barrier,
}

impl ChainCoordinator {
    /// Create a coordinator for a cohort of `chain_count` chains.
    pub fn new(chain_count: usize) -> Self {
        Self {
            barrier: tokio::sync::Barrier::new(chain_count),
        }
    }

    /// Rendezvous all chains at this step, command the shared voltage
    /// from the elected leader, then settle.
    ///
    /// `voltage_v`/`voltage_settle` are computed identically by every
    /// chain (same step index, same chip target, same domain count),
    /// so it's safe for only the leader to actually drive the rail.
    /// Returns the wait result so callers can short-circuit logging
    /// if only the leader should log.
    pub async fn sync_voltage_step(
        &self,
        regulator: &Arc<Mutex<dyn VoltageRegulator + Send>>,
        voltage_v: f32,
        voltage_settle: std::time::Duration,
    ) -> Result<bool, anyhow::Error> {
        let result = self.barrier.wait().await;
        let is_leader = result.is_leader();
        if is_leader {
            regulator
                .lock()
                .await
                .set_voltage(voltage_v)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("ramp-coord leader set_voltage({voltage_v}V): {e}")
                })?;
        }
        tokio::time::sleep(voltage_settle).await;
        Ok(is_leader)
    }
}

/// Boxed type for the chip → controller response stream.
pub type ChipRxStream = std::pin::Pin<
    Box<
        dyn futures::Stream<Item = Result<super::protocol::Response, std::io::Error>>
            + Send
            + 'static,
    >,
>;

/// Boxed type for the controller → chip command sink.
pub type ChipTxSink = std::pin::Pin<
    Box<
        dyn futures::Sink<super::protocol::Command, Error = std::io::Error>
            + Send
            + 'static,
    >,
>;

/// Controller-side handle for changing the chip-channel UART baud rate.
///
/// Implementations wrap a [`SerialControl`](crate::transport::serial::SerialControl)
/// or equivalent so the hash thread can drive a mid-init baud bump.
///
/// The recommended pattern on the Amlogic `meson_uart` driver is the
/// "close + reopen" approach (matching LuxOS — see
/// `captures/luxos-bhb56902-findings.md` for the 3 separate `open64()`
/// calls observed during init): drop the current `SerialStream`, open
/// a fresh one at the new baud rate, and return a new pair of typed
/// channels. The `tcsetattr(Drain)` baud-switch path drops chips at
/// 3 Mbaud on BHB56902.
#[async_trait::async_trait]
pub trait ChipUartBaudControl {
    /// Pre-open a second `/dev/ttyS2` handle at `current_baud_rate`
    /// (matching the current kernel termios so the hardware baud
    /// doesn't change yet) and return fresh I/O channels backed by
    /// that handle. The implementation must keep the new handle's
    /// control side alive internally so it can be retuned in
    /// `finalize_baud_switch`.
    ///
    /// This is the open-without-bumping side of the two-phase LuxOS
    /// baud-switch pattern: two fds end up open on the same device,
    /// the old writer carries the chip-side `UartBaud` broadcast at
    /// the current rate, and only afterward does the new fd get
    /// retuned to the target rate.
    async fn prepare_new_stream(
        &mut self,
        current_baud_rate: u32,
    ) -> anyhow::Result<(ChipRxStream, ChipTxSink)>;

    /// Apply `target_baud_rate` to the stream prepared by the last
    /// `prepare_new_stream` call. Per Linux `termios` semantics, this
    /// also retunes the underlying device — so the hash thread must
    /// have already finished its broadcast on the OLD writer and
    /// waited for chips to apply their internal baud change before
    /// calling this.
    async fn finalize_baud_switch(
        &mut self,
        target_baud_rate: u32,
    ) -> anyhow::Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    /// Mock ASIC enable for testing.
    struct MockAsicEnable {
        enabled: bool,
    }

    #[async_trait]
    impl AsicEnable for MockAsicEnable {
        async fn enable(&mut self) -> anyhow::Result<()> {
            self.enabled = true;
            Ok(())
        }

        async fn disable(&mut self) -> anyhow::Result<()> {
            self.enabled = false;
            Ok(())
        }
    }

    #[tokio::test]
    async fn peripherals_can_be_shared() {
        let enable = Arc::new(Mutex::new(MockAsicEnable { enabled: false }));

        // Simulate board keeping a reference
        let board_ref = Arc::clone(&enable);

        // Hash thread gets its reference via ChainPeripherals
        let peripherals = ChainPeripherals {
            asic_enable: enable,
            voltage_regulator: None,
            chip_uart_baud: None,
            ramp_coordinator: None,
            thermal_cap_mhz: None,
        };

        // Hash thread enables
        peripherals.asic_enable.lock().await.enable().await.unwrap();

        // Board can observe the state change
        assert!(board_ref.lock().await.enabled);
    }
}
