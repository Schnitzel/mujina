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
    types::{Difficulty, HashRate},
};

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
        R: Stream<Item = Result<protocol::Response, std::io::Error>> + Unpin + Send + 'static,
        W: Sink<protocol::Command> + Unpin + Send + 'static,
        W::Error: std::fmt::Debug,
    {
        // Build chain model from topology
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
        tokio::spawn(serial_reader_task(chip_rx, response_tx));

        // Spawn actor - it will initialize chips lazily on first work assignment
        tokio::spawn(async move {
            let mut actor = BM13xxActor {
                cmd_rx,
                evt_tx,
                status: status_clone,
                estimated_hashrate: initial_hashrate,
                response_rx,
                chip_tx,
                peripherals: config.peripherals,
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
async fn serial_reader_task<R>(
    mut chip_rx: R,
    response_tx: mpsc::Sender<Result<protocol::Response, std::io::Error>>,
) where
    R: Stream<Item = Result<protocol::Response, std::io::Error>> + Unpin,
{
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
struct BM13xxActor<W> {
    cmd_rx: mpsc::Receiver<ThreadCommand>,
    evt_tx: mpsc::Sender<HashThreadEvent>,
    status: Arc<RwLock<HashThreadStatus>>,
    estimated_hashrate: HashRate,
    /// Responses from chips, forwarded by the reader task.
    response_rx: mpsc::Receiver<Result<protocol::Response, std::io::Error>>,
    chip_tx: W,
    peripherals: super::chain_config::ChainPeripherals,

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
/// response takes ~1ms on the wire; 100ms is generous headroom for USB
/// buffering and chip processing.
const PER_CHIP_TIMEOUT: Duration = Duration::from_millis(100);

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

impl<W> BM13xxActor<W>
where
    W: Sink<protocol::Command> + Unpin,
    W::Error: std::fmt::Debug,
{
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
            if status.hashrate.is_zero() {
                status.hashrate = self.estimated_hashrate;
            }
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

        // 6. Execute per-chip register configuration (Phase 2)
        // Enables cores before frequency ramp (experimenting with order).
        self.execute_reg_config_perchip().await?;

        // 7. Ramp frequency to target (flat voltage throughout)
        self.execute_frequency_ramp(protocol::Frequency::from_mhz(500.0))
            .await?;

        self.chip_state = ChipState::Initialized;
        let status = self.update_status(|status| {
            status.hashrate = self.estimated_hashrate;
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

        let mut responding_count: usize = 0;

        for &addr in &addresses {
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
                return responding_count;
            }

            // Wait for a single response
            let got_response = loop {
                tokio::select! {
                    response = self.response_rx.recv() => {
                        match response {
                            Some(Ok(protocol::Response::ReadRegister {
                                register: Register::ChipId { .. },
                                ..
                            })) => break true,
                            Some(Ok(_)) | Some(Err(_)) => {
                                // Ignore non-ChipId responses and errors,
                                // keep waiting for the real response.
                                continue;
                            }
                            None => break false,
                        }
                    }
                    _ = time::sleep(PER_CHIP_TIMEOUT) => {
                        break false;
                    }
                }
            };

            if got_response {
                responding_count += 1;
            }
        }

        // Log result
        if responding_count == expected_count {
            debug!(count = responding_count, "Chain verification passed");
        } else {
            error!(
                expected = expected_count,
                responding = responding_count,
                missing = expected_count.saturating_sub(responding_count),
                "Chain verification failed: not all chips responded"
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
            },
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
    ) -> BM13xxActor<W>
    where
        W: Sink<protocol::Command> + Unpin,
        W::Error: std::fmt::Debug,
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
            estimated_hashrate: HashRate::from_gigahashes(83.0 * chain.chip_count() as f64),
            response_rx,
            chip_tx,
            peripherals: ChainPeripherals {
                asic_enable: Arc::new(Mutex::new(asic_enable)),
                voltage_regulator: None,
            },
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
        let chip_tx = futures::sink::drain();

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
        let chip_tx = futures::sink::drain();

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
        let chip_tx = futures::sink::drain();

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
        let chip_tx = futures::sink::drain();

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
        let chip_tx = futures::sink::drain();

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
        let chip_tx = futures::sink::drain();

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
        let chip_tx = futures::sink::drain();

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
        let chip_tx = futures::sink::drain();

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
        let chip_tx = futures::sink::drain();

        let (chain, sequencer) = chain_and_sequencer(1);
        let mut actor = test_actor(responses, chip_tx, chain, sequencer, MockAsicEnable::new());

        // Actor starts disabled
        actor.disable_chips().await;
        assert!(matches!(actor.chip_state, ChipState::Disabled));
    }

    #[tokio::test(start_paused = true)]
    async fn handle_go_idle_disables_and_clears_task() {
        let responses: Vec<Result<protocol::Response, io::Error>> = vec![];
        let chip_tx = futures::sink::drain();

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
