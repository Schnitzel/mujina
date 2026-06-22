//! BM13xx HashThread implementation (v2).
//!
//! Uses the domain model from [`super::chain`] and [`super::sequencer`] for
//! topology-driven initialization. The board provides a [`ChainConfig`] that
//! describes expected chip count and layout; enumeration *verifies* this
//! topology rather than discovering it.
//!
//! # TODO
//!
//! Stream lifecycle needs rethinking for baud rate changes. Currently streams
//! are passed to the constructor separately from config. A future design might
//! use a factory or allow stream replacement.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bitcoin::block::Header as BlockHeader;
use futures::{Sink, SinkExt, Stream};
use parking_lot::RwLock;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tokio_stream::StreamExt;

use super::chain::Chain;
use super::chain_config::ChainConfig;
use super::protocol::{self, Command, Register, RegisterAddress};
use super::sequencer::Sequencer;
use crate::{
    asic::hash_thread::{
        HashTask, HashThread, HashThreadCapabilities, HashThreadError, HashThreadEvent,
        HashThreadStatus, Share,
    },
    job_source::MerkleRootKind,
    tracing::prelude::*,
    types::{Difficulty, HashRate, HashrateEstimator},
};

/// Window used by the per-thread hashrate estimator that drives
/// `status.hashrate`. 5 minutes matches the scheduler-side estimator
/// so the per-board and the chain-wide hashrate views agree.
const ACTOR_HASHRATE_WINDOW: Duration = Duration::from_secs(5 * 60);

/// Minimum chip count required for initialization to succeed.
///
/// Returns the minimum number of responding chips given an expected count.
/// If fewer chips respond, initialization fails.
fn min_viable_chip_count(expected: usize) -> usize {
    expected / 2
}

/// [`HashThread`] implementation for BM13xx ASIC chains.
///
/// The scheduler uses this to dispatch mining work to BM13xx chips.
/// Internally, a facade/actor pattern keeps I/O off the caller's task:
/// methods send commands to a spawned [`BM13xxActor`] that owns the serial
/// streams.
pub struct BM13xxThread {
    /// Human-readable name for logging
    name: String,
    /// Channel for sending commands to the actor
    command_tx: mpsc::Sender<ThreadCommand>,
    capabilities: HashThreadCapabilities,
    status: Arc<RwLock<HashThreadStatus>>,
    event_rx: Option<mpsc::Receiver<HashThreadEvent>>,
}

impl BM13xxThread {
    /// Create a new BM13xx hash thread.
    ///
    /// Spawns an internal actor task that handles chip communication, and a
    /// reader task that continuously reads from the serial port. The actor
    /// starts with chips disabled and will initialize them lazily when work
    /// is first assigned.
    ///
    /// # Arguments
    /// * `chip_rx` - Stream of decoded responses from chips
    /// * `chip_tx` - Sink for sending encoded commands to chips
    /// * `config` - Chain configuration from board (topology, chip config, peripherals)
    ///
    /// # Errors
    /// Returns error if `config.peripherals.asic_enable` is missing.
    pub fn new<R, W>(chip_rx: R, chip_tx: W, config: ChainConfig) -> Result<Self, HashThreadError>
    where
        R: Stream<Item = Result<protocol::Response, std::io::Error>> + Send + 'static,
        W: Sink<protocol::Command, Error = std::io::Error> + Send + 'static,
    {
        let chip_rx: Pin<
            Box<dyn Stream<Item = Result<protocol::Response, std::io::Error>> + Send + 'static>,
        > = Box::pin(chip_rx);
        let chip_tx: Pin<
            Box<dyn Sink<protocol::Command, Error = std::io::Error> + Send + 'static>,
        > = Box::pin(chip_tx);
        // Build chain model from topology.
        //
        // Address interval=2 outperforms the ESP-Miner BM1366 default
        // of 256/chip_count (=3 for 77 chips): live S19k Pro tests
        // showed interval=2 enumerates 34/77 chips while interval=3
        // enumerates only 22/77, both with the BM1366 register
        // decoder fully patched. The reason isn't clear yet — likely
        // a bus-integrity quirk on this hashboard layout — so retain
        // interval=2 as the empirically-better default.
        let mut chain = Chain::from_topology(&config.topology);
        chain.assign_addresses().map_err(|e| {
            HashThreadError::InitializationFailed(format!("Address assignment failed: {}", e))
        })?;

        // Create sequencer for command generation
        let sequencer = Sequencer::new(config.chip_config.clone());

        let (cmd_tx, cmd_rx) = mpsc::channel(10);
        let (evt_tx, evt_rx) = mpsc::channel(100);
        let status = Arc::new(RwLock::new(HashThreadStatus::default()));
        let status_clone = Arc::clone(&status);
        let name = config.name.clone();

        // Channel for forwarding responses from the reader task to the actor.
        // Buffer size allows the reader to run ahead during bursts (e.g., chain
        // verification where all chips respond at once).
        let (response_tx, response_rx) = mpsc::channel(128);

        // Initial hashrate estimate: 83 GH/s per chip (~1 TH/s for 12-chip EmberOne).
        // Must match TicketMask scaling in sequencer.rs so scheduler computes
        // share_target >= TicketMask difficulty. The HashrateEstimator will
        // refine this once shares start flowing.
        let initial_hashrate = HashRate::from_gigahashes(83.0 * chain.chip_count() as f64);

        // Spawn reader task - continuously reads from serial to prevent USB
        // CDC-ACM flow control from blocking TX. Runs until chip_rx closes.
        let reader_handle =
            tokio::spawn(serial_reader_task(chip_rx, response_tx.clone()));

        // Spawn actor - it will initialize chips lazily on first work assignment
        tokio::spawn(async move {
            let mut actor = BM13xxActor {
                cmd_rx,
                evt_tx,
                status: status_clone,
                hashrate_estimator: HashrateEstimator::new(ACTOR_HASHRATE_WINDOW),
                paused: false,
                estimated_hashrate: initial_hashrate,
                response_rx,
                chip_tx,
                reader_handle: Some(reader_handle),
                response_tx,
                peripherals: config.peripherals,
                post_broadcast_chip_baud: config.post_broadcast_chip_baud,
                chain,
                sequencer,
                chip_state: ChipState::Disabled,
                current_task: None,
                chip_jobs: ChipJobs::new(),
            };
            actor.run().await;
        });

        Ok(Self {
            name,
            command_tx: cmd_tx,
            capabilities: HashThreadCapabilities {
                hashrate_estimate: initial_hashrate,
            },
            status,
            event_rx: Some(evt_rx),
        })
    }
}

/// Reader task that continuously reads from the serial port.
///
/// USB CDC-ACM serial requires the RX side to be read for TX to proceed.
/// Without a continuous reader, TX blocks after ~7-8 commands. This task
/// ensures RX is always being serviced, forwarding decoded responses to
/// the actor via channel.
async fn serial_reader_task(
    mut chip_rx: Pin<
        Box<dyn Stream<Item = Result<protocol::Response, std::io::Error>> + Send + 'static>,
    >,
    response_tx: mpsc::Sender<Result<protocol::Response, std::io::Error>>,
) {
    while let Some(response) = chip_rx.next().await {
        if response_tx.send(response).await.is_err() {
            // Actor dropped, exit reader
            break;
        }
    }
}

#[async_trait]
impl HashThread for BM13xxThread {
    fn name(&self) -> &str {
        &self.name
    }

    fn capabilities(&self) -> &HashThreadCapabilities {
        &self.capabilities
    }

    async fn update_task(
        &mut self,
        new_task: HashTask,
    ) -> Result<Option<HashTask>, HashThreadError> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(ThreadCommand::UpdateTask {
                new_task,
                response_tx: tx,
            })
            .await
            .map_err(|_| HashThreadError::ThreadOffline)?;
        rx.await.map_err(|_| HashThreadError::ThreadOffline)?
    }

    async fn replace_task(
        &mut self,
        new_task: HashTask,
    ) -> Result<Option<HashTask>, HashThreadError> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(ThreadCommand::ReplaceTask {
                new_task,
                response_tx: tx,
            })
            .await
            .map_err(|_| HashThreadError::ThreadOffline)?;
        rx.await.map_err(|_| HashThreadError::ThreadOffline)?
    }

    async fn go_idle(&mut self) -> Result<Option<HashTask>, HashThreadError> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(ThreadCommand::GoIdle { response_tx: tx })
            .await
            .map_err(|_| HashThreadError::ThreadOffline)?;
        rx.await.map_err(|_| HashThreadError::ThreadOffline)?
    }

    async fn set_paused(&mut self, paused: bool) -> Result<(), HashThreadError> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(ThreadCommand::SetPaused {
                paused,
                response_tx: tx,
            })
            .await
            .map_err(|_| HashThreadError::ThreadOffline)?;
        rx.await.map_err(|_| HashThreadError::ThreadOffline)?;
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), HashThreadError> {
        // Disable chips via go_idle - this awaits the actor's response,
        // ensuring GPIO writes complete before we return.
        let _ = self.go_idle().await;
        Ok(())
    }

    fn take_event_receiver(&mut self) -> Option<mpsc::Receiver<HashThreadEvent>> {
        self.event_rx.take()
    }

    fn status(&self) -> HashThreadStatus {
        self.status.read().clone()
    }
}

impl std::fmt::Debug for BM13xxThread {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BM13xxThread")
            .field("name", &self.name)
            .field("capabilities", &self.capabilities)
            .finish_non_exhaustive()
    }
}

/// Internal actor for [`BM13xxThread`].
///
/// Spawned as a Tokio task, owns the serial TX stream and chip state. Receives
/// commands from the facade via channel, manages chip power state, and emits
/// nonce events when mining is active.
///
/// Serial RX is handled by a separate reader task that forwards decoded
/// responses via `response_rx`. This ensures continuous reading from the
/// serial port, which is required for USB CDC-ACM flow control.
struct BM13xxActor {
    cmd_rx: mpsc::Receiver<ThreadCommand>,
    evt_tx: mpsc::Sender<HashThreadEvent>,
    status: Arc<RwLock<HashThreadStatus>>,
    /// Live per-thread hashrate, computed from shares the chips
    /// actually produce. Read in `handle_chip_response` after every
    /// accepted share and written into `status.hashrate` so the API /
    /// per-board UI matches the chain-wide hashrate the scheduler
    /// computes the same way.
    hashrate_estimator: HashrateEstimator,
    /// Mirror of the scheduler's `paused` flag, set via
    /// `ThreadCommand::SetPaused`. When true, `handle_chip_response`
    /// drops accepted shares before they reach the hashrate estimator
    /// or `status.hashrate`, so the per-board UI matches the
    /// chain-wide 0 TH/s the scheduler reports.
    paused: bool,
    /// Last value pulled out of `hashrate_estimator`. Cached so other
    /// code paths (e.g. status updates that don't have a fresh share
    /// to record) read a meaningful number instead of falling back
    /// to the static `capabilities.hashrate_estimate`.
    estimated_hashrate: HashRate,
    /// Responses from chips, forwarded by the reader task.
    response_rx: mpsc::Receiver<Result<protocol::Response, std::io::Error>>,
    chip_tx: Pin<Box<dyn Sink<protocol::Command, Error = std::io::Error> + Send + 'static>>,
    /// Handle on the currently-running serial reader task. Replaced
    /// when the actor swaps the underlying serial stream (baud bump).
    reader_handle: Option<tokio::task::JoinHandle<()>>,
    /// Sender used by the reader task to forward decoded responses to
    /// `response_rx`. Retained so we can re-spawn a fresh reader task
    /// after a baud-rate swap.
    response_tx: mpsc::Sender<Result<protocol::Response, std::io::Error>>,
    peripherals: super::chain_config::ChainPeripherals,
    /// If set, after the broadcast-phase init the actor sends a UartBaud
    /// broadcast and switches the controller-side serial to this rate
    /// before running the per-chip phase. See `ChainConfig`.
    post_broadcast_chip_baud: Option<super::protocol::BaudRate>,

    /// Chain model with chip addresses and domain structure.
    chain: Chain,
    /// Sequence generator for initialization and operation.
    sequencer: Sequencer,

    chip_state: ChipState,
    current_task: Option<HashTask>,
    /// Maps chip job IDs to tasks for nonce correlation.
    chip_jobs: ChipJobs,
}

/// Commands sent from the facade ([`BM13xxThread`]) to the actor.
enum ThreadCommand {
    UpdateTask {
        new_task: HashTask,
        response_tx: oneshot::Sender<Result<Option<HashTask>, HashThreadError>>,
    },
    ReplaceTask {
        new_task: HashTask,
        response_tx: oneshot::Sender<Result<Option<HashTask>, HashThreadError>>,
    },
    GoIdle {
        response_tx: oneshot::Sender<Result<Option<HashTask>, HashThreadError>>,
    },
    /// Forwarded from the scheduler's pause/resume API. Doesn't touch
    /// chip power — just flips an internal flag so the actor stops
    /// feeding its hashrate estimator and the per-board status
    /// publishes 0 TH/s / `is_active=false` immediately.
    SetPaused {
        paused: bool,
        response_tx: oneshot::Sender<()>,
    },
}

/// Chip power/initialization state.
enum ChipState {
    /// Chips disabled (low power). Initial state and after go_idle.
    Disabled,
    /// Chips enabled and fully configured, ready to hash.
    Initialized,
}

/// Ring buffer mapping chip job IDs to tasks.
///
/// BM13xx chips include a job_id in nonce responses so we can correlate
/// results with the work that produced them. This buffer stores recent
/// tasks indexed by the job_id sent to chips.
///
/// Uses 16 slots to match the 4-bit job_id field in the protocol and provide
/// sufficient buffer for ntime rolling. With jobs sent every second and nonces
/// potentially taking several seconds to find, fewer slots would cause task
/// overwrites before nonces arrive.
struct ChipJobs {
    tasks: [Option<HashTask>; 16],
    next_id: u8,
}

impl ChipJobs {
    fn new() -> Self {
        Self {
            tasks: Default::default(),
            next_id: 0,
        }
    }

    /// Insert a task and return its chip job ID.
    fn insert(&mut self, task: HashTask) -> u8 {
        let id = self.next_id;
        self.tasks[id as usize] = Some(task);
        self.next_id = (self.next_id + 1) % (self.tasks.len() as u8);
        id
    }

    /// Look up a task by chip job ID.
    fn get(&self, id: u8) -> Option<&HashTask> {
        self.tasks.get(id as usize).and_then(|t| t.as_ref())
    }
}

/// Settle time after changing the voltage regulator target, before adjusting
/// frequency. Allows the regulator output to reach the new setpoint.
const VOLTAGE_SETTLE_DELAY: Duration = Duration::from_millis(50);

/// Maximum total output voltage for per-chip stacked regulators (TPS546).
const DEFAULT_VOUT_MAX: f32 = 4.0;

/// Compute voltage for per-chip stacked regulators (e.g., TPS546 on EmberOne)
/// or single-supply PSUs driving series domains (e.g., APW12 on S19j Pro).
///
/// Near-threshold CMOS (like the BM1362 at 7nm) needs more voltage as clock
/// frequency increases -- faster switching requires more drive current and
/// timing margin. The relationship is approximately linear in the operating
/// range.
///
/// Interpolates between two known-good operating points and returns
/// V_per_chip * domain_count, capped at the max voltage.
///
/// For series chains: domain_count is the number of voltage domains in series.
/// For stacked regulators: domain_count equals chip_count (one domain per chip).
fn voltage_for_frequency_stacked(
    freq: protocol::Frequency,
    domain_count: usize,
    max_voltage: f32,
) -> f32 {
    // Known-good operating points (per-chip voltage):
    //   - Low: confirmed stable in our testing
    //   - High: from reference implementations
    const LOW_FREQ_MHZ: f32 = 56.25;
    const LOW_VOLTAGE: f32 = 0.25625;
    const HIGH_FREQ_MHZ: f32 = 500.0;
    const HIGH_VOLTAGE: f32 = 0.3; // 3.6V total across 12 chips

    // V/MHz -- derived from the two operating points
    const SLOPE: f32 = (HIGH_VOLTAGE - LOW_VOLTAGE) / (HIGH_FREQ_MHZ - LOW_FREQ_MHZ);

    let v_per_chip = LOW_VOLTAGE + SLOPE * (freq.mhz() - LOW_FREQ_MHZ);

    (v_per_chip * domain_count as f32).min(max_voltage)
}

/// Per-chip timeout during chain verification.
///
/// When polling each chip individually, this is how long to wait for a single
/// response before declaring the chip unresponsive. At 115200 baud, an 11-byte
/// response takes ~1ms on the wire; 500ms is generous headroom for USB
/// buffering, chip processing, and post-baud-bump nonce traffic that the
/// verify drains-and-ignores between each addressed read. On BHB56902 at
/// 3.125 Mbaud, 100 ms reported 10–61 chips "missing" even though chain
/// hashrate showed all 77 contributing; bumping the cap reduces the
/// false-negative rate.
const PER_CHIP_TIMEOUT: Duration = Duration::from_millis(500);

/// Convert HashTask to JobFullFormat for chip hardware.
///
/// Extracts or computes the merkle root, then builds a JobFullFormat with all
/// block header fields. For computed merkle roots, requires EN2. For fixed merkle
/// roots (Stratum v2 header-only), uses the template's fixed value directly.
fn task_to_job_full(
    task: &HashTask,
    chip_job_id: u8,
) -> Result<protocol::JobFullFormat, HashThreadError> {
    let template = task.template.as_ref();

    // Get merkle root (computed or fixed)
    let merkle_root = match &template.merkle_root {
        MerkleRootKind::Computed(_) => {
            // Extract EN2 (required for computed merkle roots)
            let en2 = task.en2.as_ref().ok_or_else(|| {
                HashThreadError::WorkAssignmentFailed(
                    "EN2 required for computed merkle root".into(),
                )
            })?;

            // Compute merkle root for this EN2
            template.compute_merkle_root(en2).map_err(|e| {
                HashThreadError::WorkAssignmentFailed(format!(
                    "Merkle root computation failed: {}",
                    e
                ))
            })?
        }
        MerkleRootKind::Fixed(merkle_root) => *merkle_root,
    };

    Ok(protocol::JobFullFormat {
        job_id: chip_job_id,
        num_midstates: 1,
        starting_nonce: 0,
        nbits: template.bits,
        ntime: task.ntime,
        merkle_root,
        prev_block_hash: template.prev_blockhash,
        version: template.version.base(),
    })
}

impl BM13xxActor {
    fn update_status(&self, mutate: impl FnOnce(&mut HashThreadStatus)) -> HashThreadStatus {
        let mut status = self.status.write();
        mutate(&mut status);
        status.clone()
    }

    /// Main actor loop. Runs until command channel closes.
    async fn run(&mut self) {
        // ntime rolling timer - sends new job every second with incremented timestamp
        let mut ntime_ticker = time::interval(Duration::from_secs(1));
        ntime_ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // Command channel - break immediately when closed (facade dropped)
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(cmd) => self.handle_command(cmd).await,
                        None => break, // Channel closed, exit loop
                    }
                }
                Some(result) = self.response_rx.recv() => {
                    self.handle_chip_response(result).await;
                }
                _ = ntime_ticker.tick(), if self.current_task.is_some() => {
                    self.roll_ntime().await;
                }
            }
        }

        // Cleanup on exit
        self.disable_chips().await;
    }

    /// Roll ntime forward and send updated job to chips.
    ///
    /// Called every second to keep chips hashing fresh work. Without this,
    /// chips would exhaust the nonce space within ~4 seconds at high hashrates.
    async fn roll_ntime(&mut self) {
        let Some(task) = self.current_task.as_mut() else {
            return;
        };

        // Increment ntime
        task.ntime += 1;
        let ntime = task.ntime;

        // Clone task for send_job (releases borrow of self.current_task)
        let task = task.clone();

        // Send updated job
        if let Err(e) = self.send_job(&task).await {
            error!(error = %e, "Failed to send ntime-rolled job");
        } else {
            trace!(ntime, "Sent ntime-rolled job");
        }
    }

    async fn handle_command(&mut self, cmd: ThreadCommand) {
        match cmd {
            ThreadCommand::UpdateTask {
                new_task,
                response_tx,
            }
            | ThreadCommand::ReplaceTask {
                new_task,
                response_tx,
            } => {
                let result = self.handle_work_assignment(new_task).await;
                let _ = response_tx.send(result);
            }
            ThreadCommand::GoIdle { response_tx } => {
                let result = self.handle_go_idle().await;
                let _ = response_tx.send(result);
            }
            ThreadCommand::SetPaused {
                paused,
                response_tx,
            } => {
                self.paused = paused;
                if paused {
                    // Hard pause. The board layer is about to drop the
                    // chip power rail; the actor must:
                    //
                    //   1. Zero the per-thread status so the per-board
                    //      UI catches up before the next would-be share.
                    //   2. Drop `current_task` and mark `chip_state =
                    //      Disabled`. The chain will be cold when it
                    //      comes back, so the next `UpdateTask` MUST
                    //      route through `initialize_chips()` rather
                    //      than re-using the stale `current_task` /
                    //      assuming the chips are still configured.
                    //
                    // We deliberately do NOT call `disable_chips()`
                    // here. That path issues a chain RST_N pulse over
                    // UART and then attempts a re-enumeration at
                    // 115200; on BHB56902 the re-enumeration only sees
                    // ~37/77 chips back. The PSU-cycle path below
                    // sidesteps that because it ends up at the same
                    // cold-boot state `start_async` enters at startup,
                    // which works reliably.
                    self.hashrate_estimator =
                        HashrateEstimator::new(ACTOR_HASHRATE_WINDOW);
                    self.estimated_hashrate = HashRate::default();
                    self.current_task = None;
                    self.chip_state = ChipState::Disabled;
                    let status = self.update_status(|status| {
                        status.is_active = false;
                        status.hashrate = HashRate::default();
                    });
                    let _ = self
                        .evt_tx
                        .clone()
                        .send(HashThreadEvent::StatusUpdate(status))
                        .await;
                } else {
                    // Resume. The board layer has already brought PSU
                    // back up and waited for the rail to settle. Chips
                    // will come up cold at their default 115200 baud,
                    // but the host serial may still be configured at
                    // the post-broadcast bumped rate (e.g. 3.125 Mbaud)
                    // from before pause. Reset the host fd back to
                    // base baud so the cold-init register reads land.
                    self.reset_chip_uart_to_base_baud().await;
                }
                let _ = response_tx.send(());
            }
        }
    }

    async fn handle_work_assignment(
        &mut self,
        new_task: HashTask,
    ) -> Result<Option<HashTask>, HashThreadError> {
        // Initialize chips if not already running
        if matches!(self.chip_state, ChipState::Disabled) {
            self.initialize_chips().await?;
        }

        // Send job to chips
        self.send_job(&new_task).await?;

        let old = self.current_task.replace(new_task);
        let status = self.update_status(|status| {
            status.is_active = true;
            // Don't pre-seed status.hashrate from the static
            // `self.estimated_hashrate` (the 6.39 TH/s init value) —
            // wait for the actor's hashrate_estimator to update from
            // real shares so the per-board UI reads 0 during the ramp
            // and the actual rate during steady state.
        });
        let _ = self
            .evt_tx
            .clone()
            .send(HashThreadEvent::StatusUpdate(status))
            .await;
        Ok(old)
    }

    /// Convert task to chip-native format and send to chips.
    ///
    /// Stores the task in `chip_jobs` for nonce correlation. The returned
    /// job_id is embedded in nonce responses from chips.
    async fn send_job(&mut self, task: &HashTask) -> Result<(), HashThreadError> {
        let chip_job_id = self.chip_jobs.insert(task.clone());
        let job_data = task_to_job_full(task, chip_job_id)?;

        // Log job details for debugging hash reconstruction issues
        debug!(
            chip_job_id,
            ntime = task.ntime,
            version = format!("{:#x}", job_data.version.to_consensus()),
            prev_blockhash = %job_data.prev_block_hash,
            merkle_root = %job_data.merkle_root,
            bits = format!("{:#x}", job_data.nbits.to_consensus()),
            en2 = ?task.en2,
            "Sending job to chips"
        );

        self.chip_tx
            .send(Command::JobFull { job_data })
            .await
            .map_err(|e| {
                HashThreadError::WorkAssignmentFailed(format!("Failed to send job: {:?}", e))
            })?;

        Ok(())
    }

    async fn handle_go_idle(&mut self) -> Result<Option<HashTask>, HashThreadError> {
        self.disable_chips().await;

        let old = self.current_task.take();
        let status = self.update_status(|status| {
            status.is_active = false;
            status.hashrate = HashRate::default();
        });
        let _ = self
            .evt_tx
            .clone()
            .send(HashThreadEvent::StatusUpdate(status))
            .await;
        Ok(old)
    }

    /// Initialize chips from cold state.
    ///
    /// Uses the topology from ChainConfig to drive initialization:
    /// 1. Enable chips via AsicEnable
    /// 2. Execute enumeration sequence (from Sequencer)
    /// 3. Verify chip count matches expected topology
    /// 4. Execute domain configuration (if needed)
    /// 5. Execute register configuration
    /// 6. Ramp frequency
    async fn initialize_chips(&mut self) -> Result<(), HashThreadError> {
        let expected_count = self.chain.chip_count();
        debug!(expected_count, "Initializing chips");

        // 1. Enable chips
        self.peripherals
            .asic_enable
            .lock()
            .await
            .enable()
            .await
            .map_err(|e| {
                HashThreadError::InitializationFailed(format!("Failed to enable chips: {}", e))
            })?;

        // Wait for chips to stabilize after power-on
        time::sleep(Duration::from_millis(500)).await;

        // Drain any stale responses that accumulated during power-on
        let mut drained = 0;
        while self.response_rx.try_recv().is_ok() {
            drained += 1;
        }
        if drained > 0 {
            debug!(
                count = drained,
                "Drained stale responses before enumeration"
            );
        }

        // 2. Execute enumeration sequence (assigns addresses)
        self.execute_enumeration().await?;

        // 3. Verify chip count with retries
        const MAX_VERIFY_RETRIES: usize = 3;
        const VERIFY_RETRY_DELAY_MS: u64 = 500;
        let mut responding = 0;
        for attempt in 1..=MAX_VERIFY_RETRIES {
            responding = self.verify_chain().await;
            if responding == expected_count {
                break;
            }
            if attempt < MAX_VERIFY_RETRIES {
                debug!(
                    attempt,
                    expected = expected_count,
                    responding,
                    "Chain verification incomplete, retrying"
                );
                time::sleep(Duration::from_millis(VERIFY_RETRY_DELAY_MS)).await;
            }
        }
        let min_required = min_viable_chip_count(expected_count);
        if responding < min_required {
            return Err(HashThreadError::InitializationFailed(format!(
                "Too few chips responding: expected {}, found {} (minimum {})",
                expected_count, responding, min_required
            )));
        }
        if responding != expected_count {
            warn!(
                expected = expected_count,
                responding,
                missing = expected_count - responding,
                "Chip count mismatch, continuing with available chips"
            );
        }

        // 4. Execute domain configuration (if topology requires it)
        // TODO: Check topology.needs_domain_config() and run build_domain_config()

        // 5. Execute broadcast register configuration (Phase 1)
        self.execute_reg_config_broadcast().await?;

        // 5b. Optionally bump the chip↔controller UART baud rate before
        // the per-chip writes. This matches LuxOS on BHB56902, which
        // switches the chain to 3 Mbaud right after the broadcasts (115200
        // is the throughput bottleneck on chains with ~10+ domains).
        self.maybe_switch_chip_baud().await?;

        // 6. Execute per-chip register configuration (Phase 2)
        // Enables cores before frequency ramp (experimenting with order).
        self.execute_reg_config_perchip().await?;

        // 7. Ramp frequency to target. Per-chip-family target lives on
        //    `ChipConfig::target_frequency_mhz`; falls back to 500 MHz
        //    for older configurations that haven't been profiled.
        let target_mhz = self
            .sequencer
            .chip_config()
            .target_frequency_mhz
            .unwrap_or(500.0);
        self.execute_frequency_ramp(protocol::Frequency::from_mhz(target_mhz))
            .await?;

        self.chip_state = ChipState::Initialized;
        let status = self.update_status(|status| {
            // Leave status.hashrate at whatever the hashrate_estimator
            // produced (default 0 before first share). The static
            // `self.estimated_hashrate` is used for chain-side TicketMask
            // scaling, not for per-board display.
            status.hashrate = HashRate::default();
        });
        let _ = self
            .evt_tx
            .clone()
            .send(HashThreadEvent::StatusUpdate(status))
            .await;
        debug!(chip_count = responding, "Chain initialized");
        Ok(())
    }

    /// Execute a sequence of steps, sending commands with optional delays.
    async fn execute_sequence(
        &mut self,
        steps: Vec<super::sequencer::Step>,
        description: &str,
    ) -> Result<(), HashThreadError> {
        debug!(step_count = steps.len(), "Executing {}", description);

        for step in steps {
            self.chip_tx.send(step.command.clone()).await.map_err(|e| {
                HashThreadError::InitializationFailed(format!(
                    "Failed to send {} command: {:?}",
                    description, e
                ))
            })?;

            if let Some(delay) = step.wait_after {
                time::sleep(delay).await;
            }
        }

        Ok(())
    }

    /// Execute the enumeration sequence from the Sequencer.
    ///
    /// Sends ChainInactive followed by SetChipAddress for each chip.
    /// Addresses come from the pre-computed Chain model (interval 2).
    async fn execute_enumeration(&mut self) -> Result<(), HashThreadError> {
        let steps = self.sequencer.build_enumeration(&self.chain);
        self.execute_sequence(steps, "enumeration").await
    }

    /// Execute broadcast-only register configuration (Phase 1).
    ///
    /// Sets up Core broadcast writes, TicketMask, AnalogMux, IoDriverStrength,
    /// and initial PLL. Call this before frequency ramp.
    async fn execute_reg_config_broadcast(&mut self) -> Result<(), HashThreadError> {
        let steps = self.sequencer.build_reg_config_broadcast(&self.chain);
        self.execute_sequence(steps, "broadcast register config")
            .await
    }

    /// Execute per-chip register configuration (Phase 2).
    ///
    /// Per-chip InitControl, MiscControl, and Core writes with 500ms delays.
    /// Call this AFTER frequency ramp to enable cores at target frequency.
    async fn execute_reg_config_perchip(&mut self) -> Result<(), HashThreadError> {
        let steps = self.sequencer.build_reg_config_perchip(&self.chain);
        self.execute_sequence(steps, "per-chip register config")
            .await
    }

    /// If [`ChainConfig::post_broadcast_chip_baud`] is set, send the
    /// chip-side `UartBaud` broadcast, then ask the board to close the
    /// current `/dev/ttyS2` handle and re-open it at the new rate. The
    /// reader task is torn down and respawned around the new stream so
    /// per-chip writes and steady-state work both flow at the new rate.
    async fn maybe_switch_chip_baud(&mut self) -> Result<(), HashThreadError> {
        let Some(target_baud) = self.post_broadcast_chip_baud else {
            return Ok(());
        };
        let Some(chip_uart) = self.peripherals.chip_uart_baud.as_ref() else {
            warn!(
                target_baud = ?target_baud,
                "post_broadcast_chip_baud configured but peripherals.chip_uart_baud is None; skipping"
            );
            return Ok(());
        };

        let numeric_baud = match target_baud {
            protocol::BaudRate::Baud115200 => 115_200,
            protocol::BaudRate::Baud1M => 1_000_000,
            // 3,125,000 — NOT 3,000,000. The chip-side register value
            // `0x00003011` (wire bytes `11 30 00 00`) produces an
            // actual 3.125 Mbaud, not 3.0 Mbaud (Braiins log:
            // `Set baud rate @ requested: 3125000, actual: 3125000`,
            // and the LuxOS sniff in
            // `captures/luxos-bhb56902-steady-state.log` opens the new
            // fd at 3.125 Mbaud after broadcasting the same wire
            // value). With mujina commanding 3,000,000 the controller
            // and chip-side rates were ~4% apart, which is enough to
            // mangle frames on long chains and drop chips. 3,125,000
            // matches both ends exactly.
            protocol::BaudRate::Baud3M => 3_125_000,
            protocol::BaudRate::Custom(_) => {
                warn!("Custom UartBaud values can't be auto-mapped to a controller baud; skipping");
                return Ok(());
            }
        };

        info!(target_baud_hz = numeric_baud, "Bumping chip UART baud");

        // Two-phase LuxOS-matched baud switch:
        //
        // From `captures/luxos-bhb56902-full-mining.log`:
        //   t=508.025  OPEN64 fd=23      (open new fd ~2 s before broadcast)
        //   t=510.127  TX fd=23  UartBaud broadcast (still at 115200)
        //   t=510.340  OPEN64 fd=25      (213 ms after broadcast)
        //   t=510.4+   per-chip writes on the new fd at 3 Mbaud
        //
        // The key is that the new fd is open BEFORE the broadcast, but
        // not retuned to the target baud — both fds stay at 115200 while
        // the broadcast lands on chips. Only AFTER the broadcast does
        // the active fd get tcsetattr'd to the higher rate.

        // 1. Pre-open the new fd at the CURRENT baud (115200). The
        //    kernel tcsetattr inside `SerialStream::new` is a no-op for
        //    the device since it's already at 115200, so the in-flight
        //    broadcast can still complete cleanly. /dev/ttyS2 now has
        //    two fds open through the rest of the switch.
        const CURRENT_BAUD_HZ: u32 = 115_200;
        let (new_rx, new_tx) = {
            let mut control = chip_uart.lock().await;
            control
                .prepare_new_stream(CURRENT_BAUD_HZ)
                .await
                .map_err(|e| {
                    HashThreadError::InitializationFailed(format!(
                        "Failed to pre-open chip UART at {CURRENT_BAUD_HZ}: {e:?}"
                    ))
                })?
        };

        // 2. Broadcast UartBaud on the OLD writer. Kernel termios is
        //    still 115200, so the bytes leave the wire at 115200 and
        //    chips can decode them and apply their internal baud
        //    change.
        self.chip_tx
            .send(protocol::Command::WriteRegister {
                broadcast: true,
                chip_address: 0x00,
                register: protocol::Register::UartBaud(target_baud),
            })
            .await
            .map_err(|e| {
                HashThreadError::InitializationFailed(format!(
                    "Failed to send UartBaud broadcast: {e:?}"
                ))
            })?;
        self.chip_tx.flush().await.ok();

        // 3. Wait for chips to apply their internal baud change. LuxOS
        //    sees 213 ms broadcast → next OPEN64 in capture; with our
        //    timing at 213 ms we got 44/77 chips, at 213 ms with longer
        //    post-settle we got 67/77. Lean longer here to recover the
        //    far-end chips: their relay timing is more marginal.
        time::sleep(Duration::from_millis(500)).await;

        // 4. Retune the new fd's termios to the target baud rate.
        //    Because termios is shared across fds on the same device,
        //    this also flips the OLD fd's effective hw baud to 3 M —
        //    fine, since we're done using it.
        {
            let mut control = chip_uart.lock().await;
            control
                .finalize_baud_switch(numeric_baud)
                .await
                .map_err(|e| {
                    HashThreadError::InitializationFailed(format!(
                        "Failed to finalize baud switch to {numeric_baud}: {e:?}"
                    ))
                })?;
        }

        // 5. Swap the actor's chip channels onto the new fd. The OLD
        //    writer is dropped here. The OLD reader task must be
        //    aborted explicitly: its `chip_rx` shares the underlying
        //    fd via `Arc<SerialInner>`, and `serial_reader_task` has
        //    no exit signal (it only returns when the stream errors,
        //    which never happens for a live tty), so waiting on the
        //    JoinHandle would block forever. Without an abort the old
        //    reader stays alive sharing /dev/ttyS2 with the new
        //    reader; the kernel TTY buffer hands raw bytes to whoever
        //    asks first, so frames get split between the two tasks
        //    and produce the scattered "Failed to decode response"
        //    warnings + the random ~30/77 chips seen on
        //    pause/resume cold init. Abort, then wait briefly for
        //    the task to actually finish (which releases its Arc and
        //    closes the old fd).
        let stale_reader = self.reader_handle.take();
        let _old_tx = std::mem::replace(&mut self.chip_tx, new_tx);
        drop(_old_tx);
        if let Some(handle) = stale_reader {
            handle.abort();
            let _ = tokio::time::timeout(Duration::from_millis(200), handle).await;
        }
        self.reader_handle = Some(tokio::spawn(serial_reader_task(
            new_rx,
            self.response_tx.clone(),
        )));

        // 6. Settle before the per-chip writes start. Pad generously —
        //    the cost of waiting an extra 100-200 ms on init is small
        //    versus losing a chip out of the chain.
        time::sleep(Duration::from_millis(250)).await;

        Ok(())
    }

    /// Execute coordinated voltage-frequency ramp to target.
    ///
    /// At each step, the voltage regulator is adjusted first (leading the
    /// frequency change), then the PLL command is sent. This ensures chips
    /// always have sufficient voltage for the frequency they're running at.
    async fn execute_frequency_ramp(
        &mut self,
        target: protocol::Frequency,
    ) -> Result<(), HashThreadError> {
        let steps = self.sequencer.build_frequency_ramp(target);
        if steps.is_empty() {
            debug!("No frequency ramp needed");
            return Ok(());
        }

        let chip_count = self.chain.chip_count();
        let domain_count = self.chain.domain_count();
        let has_regulator = self.peripherals.voltage_regulator.is_some();

        // Get voltage calculation parameters from regulator
        let (min_v, max_v) = if let Some(ref regulator) = self.peripherals.voltage_regulator {
            let reg = regulator.lock().await;
            reg.voltage_range()
        } else {
            (0.0, DEFAULT_VOUT_MAX)
        };

        let step_count = steps.len();

        info!(
            steps = step_count,
            target_mhz = target.mhz(),
            coordinated = has_regulator,
            domain_count = domain_count,
            "Ramping frequency"
        );

        for (step_index, (freq, step)) in steps.iter().enumerate() {
            // 1. Set voltage (lead the frequency change)
            // Use domain_count for series voltage calculation:
            //   - For stacked (TPS546): 12 domains, V = V_per_chip * 12
            //   - For series (APW12): 42 domains, V = V_per_chip * 42
            // The regulator's set_voltage() will clamp to its valid range.
            let requested_voltage =
                has_regulator.then(|| voltage_for_frequency_stacked(*freq, domain_count, max_v));
            let applied_voltage = requested_voltage.map(|voltage| voltage.clamp(min_v, max_v));

            if step_index == 0 || step_index + 1 == step_count || (step_index + 1) % 8 == 0 {
                debug!(
                    step = step_index + 1,
                    total_steps = step_count,
                    frequency_mhz = freq.mhz(),
                    requested_voltage_v = requested_voltage,
                    applied_voltage_v = applied_voltage,
                    "Executing frequency ramp step"
                );
            }

            if let Some(ref regulator) = self.peripherals.voltage_regulator {
                regulator
                    .lock()
                    .await
                    .set_voltage(
                        applied_voltage.expect("applied voltage is present when regulator exists"),
                    )
                    .await
                    .map_err(|e| {
                        HashThreadError::InitializationFailed(format!(
                            "Failed to set voltage to {:.2}V: {}",
                            applied_voltage
                                .expect("applied voltage is present when regulator exists"),
                            e
                        ))
                    })?;

                // Brief settle time for voltage regulator
                time::sleep(VOLTAGE_SETTLE_DELAY).await;
            }

            // 2. Set frequency
            self.chip_tx.send(step.command.clone()).await.map_err(|e| {
                HashThreadError::InitializationFailed(format!(
                    "Failed to send frequency ramp command: {:?}",
                    e
                ))
            })?;

            // 3. Wait for PLL to lock (brief delay, no per-step verification)
            if let Some(delay) = step.wait_after {
                time::sleep(delay).await;
            }
        }

        // 4. Health check: verify all chips respond after ramp completes
        let responding = self.verify_chain().await;
        if responding < chip_count {
            warn!(
                expected = chip_count,
                responding, "Chips lost during frequency ramp"
            );
        }

        if has_regulator {
            let final_v =
                voltage_for_frequency_stacked(steps.last().unwrap().0, domain_count, max_v);
            info!(
                target_mhz = target.mhz(),
                voltage = format!("{:.2}V", final_v),
                "Frequency ramp complete"
            );
        } else {
            info!(target_mhz = target.mhz(), "Frequency ramp complete");
        }

        Ok(())
    }

    /// Verify all chips respond by polling each one individually.
    ///
    /// Sends an addressed ReadRegister(ChipId) to each chip in sequence and
    /// waits for a single response. Individual reads avoid the contention and
    /// decoder-desync problems that plague broadcast reads after PLL changes.
    ///
    /// Returns the number of chips that responded.
    async fn verify_chain(&mut self) -> usize {
        let expected_count = self.chain.chip_count();
        let addresses: Vec<u8> = self.chain.chips().map(|(_, chip)| chip.address).collect();

        // Multi-pass: poll all chips, then re-poll the missing ones up to
        // two more times. On BHB56902 at 3.125 Mbaud the first sweep
        // consistently reports the first ~10 chips (addresses 0x00..0x12,
        // closest to host) as missing even though chain hashrate math
        // shows them contributing — likely chain-traffic shadowing the
        // first sweep, not dead chips.
        const VERIFY_PASSES: usize = 3;

        let mut to_poll: Vec<u8> = addresses;
        let mut alive: Vec<u8> = Vec::new();

        for pass in 0..VERIFY_PASSES {
            let mut missing_this_pass: Vec<u8> = Vec::new();

            for &addr in &to_poll {
                // Drain any stale responses before each query
                while self.response_rx.try_recv().is_ok() {}

                if let Err(e) = self
                    .chip_tx
                    .send(Command::ReadRegister {
                        broadcast: false,
                        chip_address: addr,
                        register_address: RegisterAddress::ChipId,
                    })
                    .await
                {
                    error!(error = ?e, "Failed to send verification query");
                    return alive.len();
                }

                let got_response = loop {
                    tokio::select! {
                        response = self.response_rx.recv() => {
                            match response {
                                Some(Ok(protocol::Response::ReadRegister {
                                    register: Register::ChipId { .. },
                                    ..
                                })) => break true,
                                Some(Ok(_)) | Some(Err(_)) => continue,
                                None => break false,
                            }
                        }
                        _ = time::sleep(PER_CHIP_TIMEOUT) => break false,
                    }
                };

                if got_response {
                    alive.push(addr);
                } else {
                    missing_this_pass.push(addr);
                }
            }

            debug!(
                pass = pass + 1,
                total_passes = VERIFY_PASSES,
                alive = alive.len(),
                still_missing = missing_this_pass.len(),
                "Verify pass complete"
            );

            if missing_this_pass.is_empty() {
                break;
            }
            to_poll = missing_this_pass;
        }

        let responding_count = alive.len();
        if responding_count == expected_count {
            debug!(count = responding_count, "Chain verification passed");
        } else {
            let missing_str = to_poll
                .iter()
                .map(|a| format!("0x{:02x}", a))
                .collect::<Vec<_>>()
                .join(",");
            error!(
                expected = expected_count,
                responding = responding_count,
                missing = expected_count.saturating_sub(responding_count),
                missing_addrs = missing_str,
                passes = VERIFY_PASSES,
                "Chain verification failed: not all chips responded after retries"
            );
        }

        responding_count
    }

    /// Disable chips to save power.
    ///
    /// Idempotent - safe to call multiple times. The cleanup at the end of
    /// `run()` acts as a safety net if the facade is dropped without calling
    /// `shutdown()`, but won't duplicate work in the normal shutdown path.
    async fn disable_chips(&mut self) {
        if matches!(self.chip_state, ChipState::Disabled) {
            return;
        }
        debug!("Disabling chips");
        if let Err(e) = self.peripherals.asic_enable.lock().await.disable().await {
            warn!(error = %e, "Failed to disable chips");
            let status = self.update_status(|status| {
                status.hardware_errors += 1;
            });
            let _ = self
                .evt_tx
                .clone()
                .send(HashThreadEvent::StatusUpdate(status))
                .await;
        }

        // NOTE: we used to reset the host UART back to 115200 here so
        // a subsequent PauseMining -> ResumeMining cycle could re-run
        // initialize_chips against freshly-reset chips. That originally
        // got only ~37/77 chips back; the root cause turned out to be
        // a leaked reader task on the OLD chip fd (the JoinHandle was
        // awaited with a 200 ms timeout but never aborted, so its
        // Arc<SerialInner> kept the fd open and both the old and the
        // new reader raced for kernel TTY bytes — splitting frames
        // and producing scattered chip dropouts). The fix is in
        // `maybe_switch_chip_baud` / `reset_chip_uart_to_base_baud`
        // step 5: `handle.abort()` before awaiting the JoinHandle.
        // Hard pause on BHB56902 now drops the PSU rail instead of
        // touching this code path; that path is only hit during
        // shutdown, where the process exits and host termios doesn't
        // matter.
        // Drop the per-thread hashrate estimator so a subsequent
        // re-init starts from zero instead of carrying old samples.
        self.hashrate_estimator = HashrateEstimator::new(ACTOR_HASHRATE_WINDOW);
        self.estimated_hashrate = HashRate::default();

        self.chip_state = ChipState::Disabled;
        let status = self.update_status(|status| {
            status.is_active = false;
            status.hashrate = HashRate::default();
        });
        let _ = self
            .evt_tx
            .clone()
            .send(HashThreadEvent::StatusUpdate(status))
            .await;
    }

    /// Re-open the chip channel at 115200 baud and re-spawn the reader
    /// task on the new fd. Called from `disable_chips` so that
    /// PauseMining -> ResumeMining round-trips end up with host-side
    /// UART matching the chip-side default (chips revert to 115200 on
    /// reset, but `tcsetattr` on the host side persists across resets).
    ///
    /// No-op when the board doesn't expose a baud-control adapter, or
    /// when no baud bump was configured to begin with — in those cases
    /// the chain has never left 115200 and there's nothing to undo.
    async fn reset_chip_uart_to_base_baud(&mut self) {
        if self.post_broadcast_chip_baud.is_none() {
            return;
        }
        let Some(chip_uart) = self.peripherals.chip_uart_baud.as_ref() else {
            return;
        };
        const BASE_BAUD_HZ: u32 = 115_200;

        let new_streams = {
            let mut control = chip_uart.lock().await;
            // Re-use the two-phase swap: prepare a fresh stream at the
            // current rate (whatever the previous baud bump landed on),
            // then `finalize_baud_switch` retunes the device down to
            // 115200 (kernel termios is shared across both fds on the
            // same device, so this also flips the old fd).
            let prepared = control.prepare_new_stream(BASE_BAUD_HZ).await;
            let finalized = control.finalize_baud_switch(BASE_BAUD_HZ).await;
            match (prepared, finalized) {
                (Ok(pair), Ok(())) => Some(pair),
                (Err(e), _) | (_, Err(e)) => {
                    warn!(
                        error = %e,
                        "Failed to reset chip UART to 115200; resume may fail to enumerate"
                    );
                    None
                }
            }
        };

        if let Some((new_rx, new_tx)) = new_streams {
            // Swap the actor's chip channels onto the fresh fd. The
            // OLD reader task must be aborted: see comment in
            // `maybe_switch_chip_baud` step 5 — the reader task has
            // no natural exit signal and its Arc<SerialInner> keeps
            // the old fd alive, so without an abort the old reader
            // races the new reader for kernel TTY bytes and splits
            // frames.
            let stale_reader = self.reader_handle.take();
            let _old_tx = std::mem::replace(&mut self.chip_tx, new_tx);
            drop(_old_tx);
            if let Some(handle) = stale_reader {
                handle.abort();
                let _ = tokio::time::timeout(Duration::from_millis(200), handle).await;
            }
            self.reader_handle = Some(tokio::spawn(serial_reader_task(
                new_rx,
                self.response_tx.clone(),
            )));
            debug!("Chip UART reset to 115200 for the next initialize_chips run");
        }
    }

    async fn handle_chip_response(&mut self, result: Result<protocol::Response, std::io::Error>) {
        match result {
            Ok(protocol::Response::Nonce {
                nonce,
                job_id,
                version,
                midstate_num,
                subcore_id,
            }) => {
                // HACK: BM1362 job_id fix - protocol.rs extracts job_id from bits 7-4,
                // but BM1362 returns it in bits 6-3. Reconstruct result_header and re-extract.
                // TODO: Move this to protocol.rs with chip-type-aware parsing.
                //
                // Empirically required on BM1366 too: gating this to BM1362
                // only collapsed BM1366 mining hashrate from ~29 TH/s to
                // ~3 TH/s on a BHB56902. The BM1366 nonce response uses
                // the BM1362-style 5-bit-job-id / 3-bit-subcore packing,
                // NOT the standard 4+4 layout that the protocol decoder
                // assumes. Until we move chip-type-aware parsing into
                // protocol.rs, the universal fix-up here is what makes
                // BM1366 work.
                let result_header = (job_id << 4) | subcore_id;
                let job_id = (result_header >> 3) & 0x0f;

                // Look up the task for this job_id
                let Some(task) = self.chip_jobs.get(job_id) else {
                    trace!(
                        job_id,
                        nonce = format!("{:#x}", nonce),
                        "Nonce for unknown job_id (possibly stale)"
                    );
                    return;
                };

                let template = task.template.as_ref();

                // Reconstruct full version from rolling bits
                let full_version = version.apply_to_version(template.version.base());

                // Get merkle root for this task
                let merkle_root = match &template.merkle_root {
                    MerkleRootKind::Fixed(root) => *root,
                    MerkleRootKind::Computed(_) => {
                        match task
                            .en2
                            .as_ref()
                            .and_then(|en2| template.compute_merkle_root(en2).ok())
                        {
                            Some(root) => root,
                            None => {
                                error!(job_id, "Failed to compute merkle root for nonce");
                                return;
                            }
                        }
                    }
                };

                // Build block header and compute hash
                let header = BlockHeader {
                    version: full_version,
                    prev_blockhash: template.prev_blockhash,
                    merkle_root,
                    time: task.ntime,
                    bits: template.bits,
                    nonce,
                };

                // Debug: show full 80-byte header before hashing
                {
                    use bitcoin::consensus::Encodable;
                    let mut header_bytes = Vec::with_capacity(80);
                    header.consensus_encode(&mut header_bytes).unwrap();
                    debug!(
                        header_hex = hex::encode(&header_bytes),
                        "Block header bytes (80 bytes) before hashing"
                    );
                }

                let hash = header.block_hash();

                // Validate against task share target
                if task.share_target.is_met_by(hash) {
                    let share = Share {
                        nonce,
                        hash,
                        version: full_version,
                        ntime: task.ntime,
                        extranonce2: task.en2,
                        expected_work: task.share_target.to_work(),
                    };

                    // Drop the share if the scheduler told us we're
                    // paused — chips are still hashing the last job
                    // they loaded but the scheduler ignores incoming
                    // shares while paused, so the per-board UI should
                    // also stop counting them. Without this gate the
                    // actor's hashrate_estimator keeps climbing while
                    // chain-wide hashrate is at 0.
                    if self.paused {
                        trace!(
                            job_id,
                            nonce = format!("{:#x}", nonce),
                            "Share dropped — actor is paused"
                        );
                        return;
                    }

                    // Feed the per-thread hashrate estimator BEFORE we
                    // move `share` into the channel send. We feed regardless
                    // of whether the scheduler ultimately accepts the share —
                    // any nonce that passed our local share_target is
                    // chip-side proof-of-work and should count toward the
                    // per-board hashrate display.
                    self.hashrate_estimator.record(share.expected_work);
                    self.estimated_hashrate = self.hashrate_estimator.hashrate();

                    // Send via task's dedicated channel
                    if task.share_tx.send(share).await.is_err() {
                        // Channel closed = task replaced, share is stale
                        debug!("Share channel closed (task replaced)");
                    } else {
                        let status = self.update_status(|status| {
                            status.chip_shares_found += 1;
                            status.is_active = true;
                            status.hashrate = self.estimated_hashrate;
                        });
                        let _ = self
                            .evt_tx
                            .clone()
                            .send(HashThreadEvent::StatusUpdate(status))
                            .await;

                        debug!(
                            job_id,
                            nonce = format!("{:#x}", nonce),
                            hash = %hash,
                            hash_diff = %Difficulty::from_hash(&hash),
                            "Share found and sent"
                        );
                    }
                } else {
                    // Debug: Show ALL header values for mismatch investigation
                    debug!(
                        job_id,
                        nonce = format!("{:#x}", nonce),
                        gp_bits = ?version,
                        version = format!("{:#x}", full_version.to_consensus()),
                        prev_blockhash = %template.prev_blockhash,
                        merkle_root = %merkle_root,
                        ntime = task.ntime,
                        bits = format!("{:#x}", template.bits.to_consensus()),
                        en2 = ?task.en2,
                        hash = %hash,
                        hash_diff = %Difficulty::from_hash(&hash),
                        "Nonce does not meet target"
                    );
                }

                let _ = (midstate_num, subcore_id); // Unused for now
            }

            Ok(protocol::Response::ReadRegister {
                chip_address,
                register,
            }) => {
                trace!(
                    chip_address = format!("0x{:02x}", chip_address),
                    ?register,
                    "Register read response"
                );
            }

            Err(e) => {
                warn!(error = %e, "Chip stream error");
                let status = self.update_status(|status| {
                    status.hardware_errors += 1;
                });
                let _ = self
                    .evt_tx
                    .clone()
                    .send(HashThreadEvent::StatusUpdate(status))
                    .await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use futures::stream;
    use tokio::sync::Mutex;

    use super::*;
    use crate::asic::bm13xx::chain_config::ChainPeripherals;
    use crate::asic::bm13xx::chip_config;
    use crate::asic::bm13xx::protocol::ChipType;
    use crate::asic::bm13xx::topology::TopologySpec;
    use crate::asic::hash_thread::AsicEnable;

    /// Mock AsicEnable for testing enable/disable behavior.
    struct MockAsicEnable {
        enabled: bool,
        enable_error: Option<String>,
    }

    impl MockAsicEnable {
        fn new() -> Self {
            Self {
                enabled: false,
                enable_error: None,
            }
        }

        fn with_enable_error(mut self, msg: &str) -> Self {
            self.enable_error = Some(msg.to_string());
            self
        }
    }

    #[async_trait]
    impl AsicEnable for MockAsicEnable {
        async fn enable(&mut self) -> anyhow::Result<()> {
            if let Some(msg) = &self.enable_error {
                return Err(anyhow::anyhow!("{}", msg));
            }
            self.enabled = true;
            Ok(())
        }

        async fn disable(&mut self) -> anyhow::Result<()> {
            self.enabled = false;
            Ok(())
        }
    }

    /// Drain-style sink with the `Error = std::io::Error` bound the
    /// production `ChipTxSink` requires. Tests previously passed
    /// `futures::sink::drain()` directly, but that has
    /// `Error = std::convert::Infallible`, so it no longer matches.
    fn drain_sink() -> impl Sink<Command, Error = std::io::Error> + Send + 'static {
        use futures::sink::SinkExt;
        futures::sink::drain().sink_map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "drain")
        })
    }

    /// Sink that fails on first send---used to test send error handling.
    struct FailingSink;

    impl Sink<Command> for FailingSink {
        type Error = io::Error;

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, _item: Command) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "simulated send failure",
            ))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Sink that captures all commands sent---used to verify command sequences.
    struct CapturingSink {
        commands: Arc<std::sync::Mutex<Vec<Command>>>,
    }

    impl CapturingSink {
        fn new() -> (Self, Arc<std::sync::Mutex<Vec<Command>>>) {
            let commands = Arc::new(std::sync::Mutex::new(Vec::new()));
            (
                Self {
                    commands: Arc::clone(&commands),
                },
                commands,
            )
        }
    }

    impl Sink<Command> for CapturingSink {
        type Error = io::Error;

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, item: Command) -> io::Result<()> {
            self.commands.lock().unwrap().push(item);
            Ok(())
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Build a ChipId response for testing.
    fn chip_id_response(chip_type: ChipType, address: u8) -> Result<protocol::Response, io::Error> {
        Ok(protocol::Response::ReadRegister {
            chip_address: address,
            register: Register::ChipId {
                chip_type,
                core_count: 0x40,
                address,
            },
        })
    }

    /// Build a ChainConfig for testing with the given topology and mock enable.
    fn test_config(chip_count: usize, asic_enable: MockAsicEnable) -> ChainConfig {
        ChainConfig {
            name: format!("test-{}-chips", chip_count),
            topology: TopologySpec::single_domain(chip_count),
            chip_config: chip_config::bm1362(),
            peripherals: ChainPeripherals {
                asic_enable: Arc::new(Mutex::new(asic_enable)),
                voltage_regulator: None,
                chip_uart_baud: None,
            },
            post_broadcast_chip_baud: None,
        }
    }

    /// Create a test actor directly (not via BM13xxThread).
    ///
    /// Takes pre-loaded responses to inject via channel.
    fn test_actor<W>(
        responses: Vec<Result<protocol::Response, io::Error>>,
        chip_tx: W,
        chain: Chain,
        sequencer: Sequencer,
        asic_enable: MockAsicEnable,
    ) -> BM13xxActor
    where
        W: Sink<protocol::Command, Error = std::io::Error> + Send + 'static,
    {
        let (_cmd_tx, cmd_rx) = mpsc::channel(10);
        let (evt_tx, _evt_rx) = mpsc::channel(100);
        let status = Arc::new(RwLock::new(HashThreadStatus::default()));

        // Pre-load responses into a channel
        let (response_tx, response_rx) = mpsc::channel(128);
        for response in responses {
            response_tx.try_send(response).unwrap();
        }

        BM13xxActor {
            cmd_rx,
            evt_tx,
            status,
            hashrate_estimator: HashrateEstimator::new(ACTOR_HASHRATE_WINDOW),
            paused: false,
            estimated_hashrate: HashRate::from_gigahashes(83.0 * chain.chip_count() as f64),
            response_rx,
            chip_tx: Box::pin(chip_tx),
            reader_handle: None,
            response_tx,
            peripherals: ChainPeripherals {
                asic_enable: Arc::new(Mutex::new(asic_enable)),
                voltage_regulator: None,
                chip_uart_baud: None,
            },
            post_broadcast_chip_baud: None,
            chain,
            sequencer,
            chip_state: ChipState::Disabled,
            current_task: None,
            chip_jobs: ChipJobs::new(),
        }
    }

    /// Create chain and sequencer for a given chip count.
    fn chain_and_sequencer(chip_count: usize) -> (Chain, Sequencer) {
        let topology = TopologySpec::single_domain(chip_count);
        let mut chain = Chain::from_topology(&topology);
        chain.assign_addresses().unwrap();
        let sequencer = Sequencer::new(chip_config::bm1362());
        (chain, sequencer)
    }

    #[tokio::test]
    async fn construction_succeeds() {
        let responses: Vec<Result<protocol::Response, io::Error>> = vec![];
        let chip_rx = stream::iter(responses);
        let chip_tx = drain_sink();

        let config = test_config(1, MockAsicEnable::new());
        let result = BM13xxThread::new(chip_rx, chip_tx, config);

        assert!(result.is_ok());
        let thread = result.unwrap();
        assert_eq!(thread.name(), "test-1-chips");
    }

    #[tokio::test(start_paused = true)]
    async fn go_idle_on_idle_thread_returns_none() {
        let responses: Vec<Result<protocol::Response, io::Error>> = vec![];
        let chip_rx = stream::iter(responses);
        let chip_tx = drain_sink();

        let config = test_config(1, MockAsicEnable::new());
        let mut thread = BM13xxThread::new(chip_rx, chip_tx, config).unwrap();

        let result = thread.go_idle().await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn actor_exits_when_facade_dropped() {
        let responses: Vec<Result<protocol::Response, io::Error>> = vec![];
        let chip_rx = stream::iter(responses);
        let chip_tx = drain_sink();

        let config = test_config(1, MockAsicEnable::new());
        let thread = BM13xxThread::new(chip_rx, chip_tx, config).unwrap();

        drop(thread);
        tokio::time::sleep(Duration::from_millis(10)).await;
        // If we get here without hanging, the actor exited correctly
    }

    #[tokio::test(start_paused = true)]
    async fn initialize_single_chip_succeeds() {
        // Topology expects 1 chip, hardware provides 1 chip response
        let responses = vec![chip_id_response(ChipType::BM1362, 0x00)];
        let chip_tx = drain_sink();

        let (chain, sequencer) = chain_and_sequencer(1);
        let mut actor = test_actor(responses, chip_tx, chain, sequencer, MockAsicEnable::new());

        let result = actor.initialize_chips().await;

        assert!(result.is_ok());
        assert!(matches!(actor.chip_state, ChipState::Initialized));
    }

    #[tokio::test(start_paused = true)]
    async fn initialize_12_chips_succeeds() {
        // EmberOne: 12 chips expected, 12 responses provided
        let responses: Vec<_> = (0..12)
            .map(|i| chip_id_response(ChipType::BM1362, i * 2)) // interval 2
            .collect();
        let chip_tx = drain_sink();

        let (chain, sequencer) = chain_and_sequencer(12);
        let mut actor = test_actor(responses, chip_tx, chain, sequencer, MockAsicEnable::new());

        let result = actor.initialize_chips().await;

        assert!(result.is_ok());
        assert!(matches!(actor.chip_state, ChipState::Initialized));
    }

    #[tokio::test(start_paused = true)]
    async fn initialize_continues_with_minor_chip_mismatch() {
        // Topology expects 12 chips, respond with minimum viable count (at threshold)
        let expected = 12;
        let responding = min_viable_chip_count(expected);
        let responses: Vec<_> = (0..responding)
            .map(|i| chip_id_response(ChipType::BM1362, (i * 2) as u8))
            .collect();
        let chip_tx = drain_sink();

        let (chain, sequencer) = chain_and_sequencer(expected);
        let mut actor = test_actor(responses, chip_tx, chain, sequencer, MockAsicEnable::new());

        // Should succeed with warning, not fail
        let result = actor.initialize_chips().await;
        assert!(result.is_ok());
        assert!(matches!(actor.chip_state, ChipState::Initialized));
    }

    #[tokio::test(start_paused = true)]
    async fn initialize_fails_on_pathologically_low_chip_count() {
        // Topology expects 12 chips, respond with one fewer than minimum viable
        let expected = 12;
        let responding = min_viable_chip_count(expected) - 1;
        let responses: Vec<_> = (0..responding)
            .map(|i| chip_id_response(ChipType::BM1362, (i * 2) as u8))
            .collect();
        let chip_tx = drain_sink();

        let (chain, sequencer) = chain_and_sequencer(expected);
        let mut actor = test_actor(responses, chip_tx, chain, sequencer, MockAsicEnable::new());

        let result = actor.initialize_chips().await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(&err, HashThreadError::InitializationFailed(msg) if msg.contains("Too few")),
            "Expected 'too few chips' error, got: {:?}",
            err
        );
    }

    #[tokio::test(start_paused = true)]
    async fn initialize_enable_failure_propagates() {
        let responses: Vec<Result<protocol::Response, io::Error>> = vec![];
        let chip_tx = drain_sink();

        let (chain, sequencer) = chain_and_sequencer(1);
        let mut actor = test_actor(
            responses,
            chip_tx,
            chain,
            sequencer,
            MockAsicEnable::new().with_enable_error("GPIO fault"),
        );

        let result = actor.initialize_chips().await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(&err, HashThreadError::InitializationFailed(msg) if msg.contains("enable")),
            "Expected enable error, got: {:?}",
            err
        );
    }

    #[tokio::test(start_paused = true)]
    async fn initialize_sink_failure_propagates() {
        let responses: Vec<Result<protocol::Response, io::Error>> = vec![];
        let chip_tx = FailingSink;

        let (chain, sequencer) = chain_and_sequencer(1);
        let mut actor = test_actor(responses, chip_tx, chain, sequencer, MockAsicEnable::new());

        let result = actor.initialize_chips().await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(&err, HashThreadError::InitializationFailed(msg) if msg.contains("send")),
            "Expected send error, got: {:?}",
            err
        );
    }

    #[tokio::test(start_paused = true)]
    async fn enumeration_sends_version_mask_chain_inactive_and_addresses() {
        let responses: Vec<Result<protocol::Response, io::Error>> = vec![];
        let (sink, commands) = CapturingSink::new();

        let (chain, sequencer) = chain_and_sequencer(3);
        let mut actor = test_actor(responses, sink, chain, sequencer, MockAsicEnable::new());

        let result = actor.execute_enumeration().await;

        assert!(result.is_ok());
        let cmds = commands.lock().unwrap();

        // Sequence: 3 VersionMask + InitControl + MiscControl + ChainInactive + N SetChipAddress
        assert_eq!(cmds.len(), 9); // 3 + 1 + 1 + 1 + 3 chips

        // First 3 commands: VersionMask writes
        for cmd in &cmds[0..3] {
            assert!(
                matches!(
                    cmd,
                    Command::WriteRegister {
                        register: Register::VersionMask(_),
                        ..
                    }
                ),
                "Expected VersionMask, got {:?}",
                cmd
            );
        }

        // InitControl broadcast (0x00)
        assert!(
            matches!(
                cmds[3],
                Command::WriteRegister {
                    register: Register::InitControl { .. },
                    ..
                }
            ),
            "Expected InitControl, got {:?}",
            cmds[3]
        );

        // MiscControl broadcast
        assert!(
            matches!(
                cmds[4],
                Command::WriteRegister {
                    register: Register::MiscControl { .. },
                    ..
                }
            ),
            "Expected MiscControl, got {:?}",
            cmds[4]
        );

        // Then ChainInactive
        assert!(matches!(cmds[5], Command::ChainInactive));

        // Then SetChipAddress for each chip
        for cmd in &cmds[6..] {
            assert!(
                matches!(cmd, Command::SetChipAddress { .. }),
                "Expected SetChipAddress, got {:?}",
                cmd
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn verify_chain_counts_chip_id_responses() {
        let responses: Vec<_> = (0..5)
            .map(|i| chip_id_response(ChipType::BM1362, i * 2))
            .collect();
        let (sink, _commands) = CapturingSink::new();

        let (chain, sequencer) = chain_and_sequencer(5);
        let mut actor = test_actor(responses, sink, chain, sequencer, MockAsicEnable::new());

        let count = actor.verify_chain().await;

        assert_eq!(count, 5);
    }

    #[tokio::test(start_paused = true)]
    async fn verify_chain_ignores_other_responses() {
        let responses: Vec<Result<protocol::Response, io::Error>> = vec![
            chip_id_response(ChipType::BM1362, 0x00),
            // Non-ChipId response should be ignored
            Ok(protocol::Response::ReadRegister {
                chip_address: 0x02,
                register: Register::VersionMask(protocol::VersionMask::full_rolling()),
            }),
            chip_id_response(ChipType::BM1362, 0x04),
        ];
        let (sink, _commands) = CapturingSink::new();

        let (chain, sequencer) = chain_and_sequencer(2);
        let mut actor = test_actor(responses, sink, chain, sequencer, MockAsicEnable::new());

        let count = actor.verify_chain().await;

        assert_eq!(count, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn verify_chain_handles_stream_errors() {
        let responses: Vec<Result<protocol::Response, io::Error>> = vec![
            chip_id_response(ChipType::BM1362, 0x00),
            Err(io::Error::new(io::ErrorKind::Other, "glitch")),
            chip_id_response(ChipType::BM1362, 0x02),
        ];
        let (sink, _commands) = CapturingSink::new();

        let (chain, sequencer) = chain_and_sequencer(2);
        let mut actor = test_actor(responses, sink, chain, sequencer, MockAsicEnable::new());

        let count = actor.verify_chain().await;

        // Errors logged but counting continues
        assert_eq!(count, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn disable_chips_noop_when_already_disabled() {
        let responses: Vec<Result<protocol::Response, io::Error>> = vec![];
        let chip_tx = drain_sink();

        let (chain, sequencer) = chain_and_sequencer(1);
        let mut actor = test_actor(responses, chip_tx, chain, sequencer, MockAsicEnable::new());

        // Actor starts disabled
        actor.disable_chips().await;
        assert!(matches!(actor.chip_state, ChipState::Disabled));
    }

    #[tokio::test(start_paused = true)]
    async fn handle_go_idle_disables_and_clears_task() {
        let responses: Vec<Result<protocol::Response, io::Error>> = vec![];
        let chip_tx = drain_sink();

        let (chain, sequencer) = chain_and_sequencer(1);
        let mut actor = test_actor(responses, chip_tx, chain, sequencer, MockAsicEnable::new());

        // Manually set state as if we had been running
        actor.chip_state = ChipState::Initialized;
        actor.status.write().is_active = true;

        let result = actor.handle_go_idle().await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
        assert!(matches!(actor.chip_state, ChipState::Disabled));
        assert!(!actor.status.read().is_active);
    }

    mod job_dispatch {
        use super::*;
        use crate::asic::bm13xx::test_data::esp_miner_job;
        use crate::job_source::{
            Extranonce2, GeneralPurposeBits, JobTemplate, MerkleRootKind, VersionTemplate,
        };

        fn fixed_merkle_task() -> HashTask {
            let (share_tx, _) = mpsc::channel(1);

            let template = Arc::new(JobTemplate {
                id: "test".into(),
                prev_blockhash: *esp_miner_job::wire_tx::PREV_BLOCKHASH,
                version: VersionTemplate::new(
                    *esp_miner_job::wire_tx::VERSION,
                    GeneralPurposeBits::full(),
                )
                .expect("Valid version template"),
                bits: *esp_miner_job::wire_tx::NBITS,
                share_target: crate::types::Difficulty::from(100u64).to_target(),
                time: *esp_miner_job::wire_tx::NTIME,
                merkle_root: MerkleRootKind::Fixed(*esp_miner_job::wire_tx::MERKLE_ROOT),
            });

            HashTask {
                template,
                en2_range: None,
                en2: Some(Extranonce2::new(0, 1).unwrap()),
                share_target: crate::types::Difficulty::from(100u64).to_target(),
                ntime: *esp_miner_job::wire_tx::NTIME,
                share_tx,
            }
        }

        #[test]
        fn task_to_job_full_with_fixed_merkle_root() {
            let task = fixed_merkle_task();
            let result = super::super::task_to_job_full(&task, 2);

            assert!(result.is_ok());
            let job = result.unwrap();

            assert_eq!(job.job_id, 2);
            assert_eq!(job.num_midstates, 1);
            assert_eq!(job.starting_nonce, 0);
            assert_eq!(job.nbits, *esp_miner_job::wire_tx::NBITS);
            assert_eq!(job.ntime, *esp_miner_job::wire_tx::NTIME);
            assert_eq!(job.merkle_root, *esp_miner_job::wire_tx::MERKLE_ROOT);
            assert_eq!(job.prev_block_hash, *esp_miner_job::wire_tx::PREV_BLOCKHASH);
        }

        #[test]
        fn chip_jobs_insert_and_get() {
            let task = fixed_merkle_task();
            let mut jobs = ChipJobs::new();

            let id = jobs.insert(task.clone());
            assert_eq!(id, 0);

            let retrieved = jobs.get(0);
            assert!(retrieved.is_some());
            // Can't compare HashTask directly, but we can check it exists
        }

        #[test]
        fn chip_jobs_wraps_at_sixteen() {
            let task = fixed_merkle_task();
            let mut jobs = ChipJobs::new();

            // Insert 16 tasks, should use IDs 0-15
            for expected_id in 0..16 {
                let id = jobs.insert(task.clone());
                assert_eq!(id, expected_id);
            }

            // 17th insert should wrap to ID 0
            let id = jobs.insert(task.clone());
            assert_eq!(id, 0);
        }

        /// Validates hash computation using computed merkle root (DummySource path).
        ///
        /// This tests the exact code path used when thread_v2 handles nonces with a
        /// MerkleRootKind::Computed template. Uses block 881423 test data with the
        /// winning EN2/version/nonce to verify correct hash computation.
        #[test]
        fn hash_computation_with_computed_merkle_root() {
            use crate::job_source::test_blocks::block_881423;
            use crate::job_source::{
                Extranonce2Range, GeneralPurposeBits, MerkleRootKind, MerkleRootTemplate,
                VersionTemplate,
            };
            use crate::types::Difficulty;
            use bitcoin::block::Header as BlockHeader;
            use tokio::sync::mpsc;

            // Build template exactly like DummySource does
            let merkle_template = MerkleRootTemplate {
                coinbase1: block_881423::coinbase1_bytes().to_vec(),
                extranonce1: block_881423::extranonce1_bytes().to_vec(),
                extranonce2_range: Extranonce2Range::new(4).unwrap(),
                coinbase2: block_881423::coinbase2_bytes().to_vec(),
                merkle_branches: block_881423::MERKLE_BRANCHES.clone(),
            };

            // Clean version like DummySource
            let v = block_881423::VERSION.to_consensus() as u32;
            let base_cleaned = (v & !0x1fff_e000) as i32;
            let version_template = VersionTemplate::new(
                bitcoin::block::Version::from_consensus(base_cleaned),
                GeneralPurposeBits::full(),
            )
            .unwrap();

            let template = crate::job_source::JobTemplate {
                id: "test".into(),
                prev_blockhash: *block_881423::PREV_BLOCKHASH,
                version: version_template,
                bits: *block_881423::BITS,
                share_target: crate::types::Target::MAX,
                time: block_881423::TIME,
                merkle_root: MerkleRootKind::Computed(merkle_template),
            };

            // Create task with winning EN2
            let (share_tx, _share_rx) = mpsc::channel(1);
            let task = HashTask {
                template: std::sync::Arc::new(template.clone()),
                en2_range: None,
                en2: Some(*block_881423::EXTRANONCE2),
                share_target: crate::types::Target::MAX,
                ntime: block_881423::TIME,
                share_tx,
            };

            // Simulate task_to_job_full path: compute merkle root from EN2
            let computed_merkle_root = template
                .compute_merkle_root(task.en2.as_ref().unwrap())
                .expect("merkle root computation should succeed");

            // Verify computed merkle root matches expected
            assert_eq!(
                computed_merkle_root,
                *block_881423::MERKLE_ROOT,
                "Computed merkle root should match block 881423"
            );

            // Simulate handle_chip_response path: reconstruct header from stored task
            // In real operation, chip returns GP bits. Use winning version's GP bits.
            let winning_version = block_881423::VERSION.to_consensus();
            let gp_bits = ((winning_version >> 13) & 0xFFFF) as u16;
            let version_bits = GeneralPurposeBits::from(gp_bits.to_be_bytes());
            let full_version = version_bits.apply_to_version(template.version.base());

            // Build header like handle_chip_response does
            let header = BlockHeader {
                version: full_version,
                prev_blockhash: template.prev_blockhash,
                merkle_root: computed_merkle_root,
                time: task.ntime,
                bits: template.bits,
                nonce: block_881423::NONCE,
            };

            let hash = header.block_hash();
            let difficulty = Difficulty::from_hash(&hash);

            // Should match block 881423's hash exactly
            assert_eq!(
                hash,
                *block_881423::BLOCK_HASH,
                "Computed hash should match block 881423"
            );

            // Difficulty should be significant (not 0.00)
            assert!(
                difficulty.as_u64() > 1000,
                "Hash difficulty {} should be significant (>1000)",
                difficulty
            );
        }
    }
}
