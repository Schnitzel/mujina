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
    /// S19k Pro — or a mixed S19j+S19k chassis — share one APW12),
    /// independent per-actor voltage commands would race on the shared
    /// mutex and could drop the rail below what a chain at a higher
    /// operating point needs. Setting this on every chain in the cohort
    /// routes their per-step voltage requests through the coordinator,
    /// which always drives the rail to the MAXIMUM any chain currently
    /// requests — so a chain with a longer/faster ramp is never starved
    /// by a chain that commanded less. `None` keeps the legacy
    /// single-chain behaviour where each actor commands its own voltage.
    pub ramp_coordinator: Option<Arc<ChainCoordinator>>,

    /// This chain's stable slot index within the shared-rail cohort
    /// (0-based). Used only to key this chain's request in the
    /// [`ChainCoordinator`] max-voltage aggregator; ignored on
    /// single-chain boards (`ramp_coordinator == None`). Boards with one
    /// chain, or that don't share a rail, may leave this `0`.
    pub chain_index: usize,

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
/// Every chain in the cohort calls [`ChainCoordinator::sync_voltage_step`]
/// at the top of each frequency-ramp step, passing the voltage IT needs
/// for the operating point it is about to command. The coordinator records
/// that per-chain request and drives the shared regulator to the
/// **maximum** voltage any chain currently requests, then settles. Because
/// the rail is always at the max of every chain's need, no chain is ever
/// under-volted by another chain's lower command.
///
/// Crucially there is **no barrier**: chains ramp fully independently. A
/// mixed chassis (e.g. two S19j Pro at 500 MHz over 72 steps + one S19k
/// Pro at 575 MHz over 84 steps) has chains with *different ramp lengths*;
/// a fixed-party barrier would deadlock the moment the shorter ramps
/// finished and stopped arriving. The max-aggregator sidesteps that
/// entirely — a finished chain simply leaves its last request standing
/// (it keeps mining at that voltage), and the still-ramping chains keep
/// updating the max. Requests persist for the life of the cohort, so the
/// rail never dips below any active chain's floor.
pub struct ChainCoordinator {
    /// Latest rail voltage each chain (keyed by its `chain_index`) wants.
    /// The commanded rail is the max over all entries.
    requests: Mutex<std::collections::HashMap<usize, f32>>,
}

impl ChainCoordinator {
    /// Create a coordinator for a cohort of `chain_count` chains.
    pub fn new(chain_count: usize) -> Self {
        Self {
            requests: Mutex::new(std::collections::HashMap::with_capacity(chain_count)),
        }
    }

    /// Record `chain_index`'s requested rail voltage, drive the shared
    /// regulator to the maximum requested across all chains, then wait
    /// `voltage_settle`. Safe to call from every chain independently and
    /// at any cadence — there is no cross-chain rendezvous.
    pub async fn sync_voltage_step(
        &self,
        chain_index: usize,
        regulator: &Arc<Mutex<dyn VoltageRegulator + Send>>,
        voltage_v: f32,
        voltage_settle: std::time::Duration,
    ) -> Result<(), anyhow::Error> {
        // Record this chain's request and compute the cohort max while
        // holding only the small requests lock (never the regulator's).
        let target = {
            let mut requests = self.requests.lock().await;
            requests.insert(chain_index, voltage_v);
            requests
                .values()
                .copied()
                .fold(f32::NEG_INFINITY, f32::max)
        };
        regulator
            .lock()
            .await
            .set_voltage(target)
            .await
            .map_err(|e| anyhow::anyhow!("ramp-coord set_voltage({target:.2}V): {e}"))?;
        tokio::time::sleep(voltage_settle).await;
        Ok(())
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
            chain_index: 0,
            thermal_cap_mhz: None,
        };

        // Hash thread enables
        peripherals.asic_enable.lock().await.enable().await.unwrap();

        // Board can observe the state change
        assert!(board_ref.lock().await.enabled);
    }
}
