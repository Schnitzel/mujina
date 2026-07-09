//! The scheduler module manages the distribution of mining jobs to hash boards
//! and ASIC chips.
//!
//! # Share Filtering (Three-Layer Architecture)
//!
//! Share filtering happens at three independent levels:
//!
//! **Layer 1 - Chip TicketMask (hardware pre-filter):**
//! - Configured by thread during initialization
//! - Chip only reports nonces meeting this threshold
//! - Set for frequent health signals (~1/sec at current hashrate)
//!
//! **Layer 2 - HashTask.share_target (scheduler target, per-thread):**
//! - Computed per thread from that thread's hashrate
//! - Clamps source difficulty between a measurement floor (1
//!   share/sec) and a flood ceiling (10 shares/sec)
//! - Feeds per-thread hashrate estimators with frequent samples
//! - Decoupled from pool difficulty so measurement works even
//!   when pool difficulty is very high
//!
//! **Layer 3 - JobTemplate.share_target (scheduler-to-source filter):**
//! - Set by pool via Stratum mining.set_difficulty
//! - Scheduler validates before forwarding to source
//! - Only pool-worthy shares submitted
//!
//! The scheduler receives shares meeting HashTask.share_target, uses them for
//! statistics and monitoring, then filters again before pool submission. This
//! provides accurate per-thread metrics while controlling network traffic.
//!
//! This is a work-in-progress. It's currently the main and initial place where
//! functionality is added, after which the functionality is refactored out to
//! where it belongs.

use slotmap::SlotMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{StreamExt, StreamMap};
use tokio_util::sync::CancellationToken;

use crate::api::commands::SchedulerCommand;
use crate::api_client::types::{MinerState, SourceState};
use crate::asic::hash_thread::{HashTask, HashThread, HashThreadEvent, Share};
use crate::job_source::{
    JobTemplate, MerkleRootKind, Share as SourceShare, SourceCommand, SourceEvent,
};
use crate::tracing::prelude::*;
use crate::types::{
    AlarmStatus, DebouncedAlarm, Difficulty, HashRate, HashrateEstimator, ShareRate, Target,
    expected_time_to_share_from_target, target_for_share_rate,
};

/// Unique identifier for a job source, assigned by the scheduler.
type SourceId = slotmap::DefaultKey;

/// Unique identifier for a hash thread, assigned by the scheduler.
type ThreadId = slotmap::DefaultKey;

/// Unique identifier for a task, assigned by the scheduler.
type TaskId = slotmap::DefaultKey;

// StreamMap type aliases for cleaner function signatures.
// These are kept as locals in run() rather than struct fields to avoid
// borrow conflicts with tokio::select!.
type SourceEventStream = StreamMap<SourceId, ReceiverStream<SourceEvent>>;
type ThreadEventStream = StreamMap<ThreadId, ReceiverStream<HashThreadEvent>>;
type ShareStream = StreamMap<TaskId, ReceiverStream<Share>>;

/// Window duration for per-thread hashrate estimation.
const HASHRATE_WINDOW: Duration = Duration::from_secs(5 * 60);
/// Short "live" window for the responsive top-level `hashrate_1min` (5 s, to
/// match LuxOS/Braiins' live hashrate; field name kept for API stability).
/// See `ACTOR_HASHRATE_WINDOW_1MIN` in `asic::bm13xx::thread_v2` — the real
/// fix for this window's swinginess was more samples (BM1362's TicketMask
/// override), not a longer time constant.
const HASHRATE_WINDOW_1MIN: Duration = Duration::from_secs(5);

/// `SetOperatingPoint`'s voltage is considered unchanged (and its whole
/// per-chain PSU write phase skipped) when the requested volts are within
/// this of `current_voltage_v`. Comfortably tighter than the DAC's own
/// ~0.1 V step, so this only catches genuinely-repeated requests, never a
/// real (if small) voltage move.
const VOLTAGE_APPLY_EPS_V: f32 = 0.01;

/// Per-thread measurement floor: minimum share rate for hashrate
/// estimation (1 share/sec).
///
/// When pool difficulty is high relative to a thread's hashrate,
/// shares arrive too infrequently for the estimator to settle. The
/// scheduler overrides with an easier target so each thread produces
/// at least this many samples per second.
const MEASUREMENT_SHARE_RATE: ShareRate = ShareRate::from_interval(Duration::from_secs(1));

/// Per-thread flood ceiling: maximum share rate to bound scheduler
/// processing and network traffic to the source (10 shares/sec).
///
/// This is deliberately much higher than a typical pool's target
/// share rate (~0.3/sec for ckpool). Capping closer to the pool's
/// target would mask the natural share flood that vardiff algorithms
/// use to raise difficulty---the pool would see a well-behaved rate
/// and never adjust. At 10/sec the pool sees a ~33x overshoot,
/// giving vardiff a clear signal to converge quickly.
const FLOOD_CAP_RATE: ShareRate = ShareRate::from_interval(Duration::from_millis(100));

/// Scheduler-side bookkeeping for an active task.
///
/// Each HashTask sent to a thread has a corresponding TaskEntry in the
/// scheduler. When a share arrives on the task's channel, this provides
/// routing: which source to submit to and the job template for validation.
#[derive(Debug)]
struct TaskEntry {
    /// Source that provided this job
    source_id: SourceId,

    /// Job template (shared with the HashTask sent to thread)
    template: Arc<JobTemplate>,

    /// Thread this task was assigned to
    thread_id: ThreadId,
}

/// Registration message for adding a job source to the scheduler.
///
/// The daemon creates sources and sends this message to register them.
/// The scheduler inserts the source into its SlotMap and begins listening
/// for events.
pub struct SourceRegistration {
    /// Source name for logging
    pub name: String,

    /// Connection URL for this source (e.g. "stratum+tcp://pool:3333").
    pub url: Option<String>,

    /// Event receiver for this source (UpdateJob, ReplaceJob, ClearJobs)
    pub event_rx: mpsc::Receiver<SourceEvent>,

    /// Command sender for this source (SubmitShare, etc.)
    pub command_tx: mpsc::Sender<SourceCommand>,
}

/// Internal scheduler tracking for a registered source.
#[derive(Debug)]
struct SourceEntry {
    /// Source name for logging
    name: String,

    /// Connection URL for this source.
    url: Option<String>,

    /// Command channel for sending to this source
    command_tx: mpsc::Sender<SourceCommand>,

    /// Last job received from this source (for assigning to newly-arriving threads)
    last_job: Option<Arc<JobTemplate>>,

    /// Debounced alarm for high-difficulty warnings.
    difficulty_alarm: DebouncedAlarm,
}

/// Whether to update alongside existing work or replace it.
#[derive(Debug)]
enum AssignMode {
    /// Add new task alongside existing (UpdateJob behavior)
    Update,
    /// Invalidate old tasks, replace current work (ReplaceJob behavior)
    Replace,
}

/// Scheduler-side bookkeeping for a hash thread.
struct ThreadEntry {
    thread: Box<dyn HashThread>,
    hashrate: HashrateEstimator,
    /// Same shares over a 1-minute window, driving the responsive top-level
    /// `hashrate_1min` (settles ~5× faster than `hashrate` after a power dial).
    hashrate_1min: HashrateEstimator,
}

/// Core scheduler state.
///
/// StreamMaps are kept separate (in `run()`) to avoid borrow conflicts with
/// `tokio::select!`. This struct holds the business state that methods operate
/// on.
struct Scheduler {
    /// Source storage and command channels
    sources: SlotMap<SourceId, SourceEntry>,

    /// Thread storage
    threads: SlotMap<ThreadId, ThreadEntry>,

    /// Task bookkeeping (maps tasks to sources/threads)
    tasks: SlotMap<TaskId, TaskEntry>,

    /// Mining statistics
    stats: MiningStats,

    /// Track thread count for disconnect detection
    last_thread_count: usize,

    /// Mining paused
    paused: bool,

    /// Last commanded chain voltage (V), tracked so `SetOperatingPoint` can
    /// pick the safe V/f ordering. Seeded to the factory cold-init setpoint and
    /// reset there on resume (a cold-init returns the rail to factory voltage).
    current_voltage_v: f32,
}

/// Factory cold-init chain voltage (V) — the rail's value after any cold init.
const COLD_INIT_VOLTAGE_V: f32 = 13.9;

/// Upper bound on how long a single thread's `set_frequency`/`set_voltage` may
/// take before the scheduler gives up on it. A full-range PLL re-ramp is ~8 s
/// per chain, so 30 s is generous headroom. The guard exists so a wedged chip
/// bus (a chain that stops accepting UART mid-ramp and never replies) can never
/// hang the scheduler — and therefore the whole HTTP/control plane — forever.
/// On timeout we log, surface it as a per-op error, and move on; the stuck
/// chain shows up as ~0 board hashrate rather than freezing every surface.
const THREAD_OP_TIMEOUT: Duration = Duration::from_secs(30);

/// Fold the outcome of a [`THREAD_OP_TIMEOUT`]-wrapped thread op into
/// `last_err`, logging failures and timeouts uniformly. A timeout means the
/// chain stopped replying (most likely a wedged chip bus mid-ramp); we record
/// it and let the scheduler keep serving every other command and surface.
fn guard_thread_op<K: std::fmt::Debug, E: std::fmt::Display>(
    outcome: Result<Result<(), E>, tokio::time::error::Elapsed>,
    id: K,
    op: &str,
    last_err: &mut Option<String>,
) {
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            warn!(thread_id = ?id, op, error = %e, "thread op failed");
            *last_err = Some(e.to_string());
        }
        Err(_) => {
            warn!(
                thread_id = ?id,
                op,
                timeout_s = THREAD_OP_TIMEOUT.as_secs(),
                "thread op timed out — chain unresponsive, skipping"
            );
            *last_err = Some(format!("{op} timed out"));
        }
    }
}

impl Scheduler {
    fn new() -> Self {
        Self {
            sources: SlotMap::new(),
            threads: SlotMap::new(),
            tasks: SlotMap::new(),
            stats: MiningStats::default(),
            last_thread_count: 0,
            paused: false,
            current_voltage_v: COLD_INIT_VOLTAGE_V,
        }
    }

    /// Aggregate measured hashrate from per-thread estimators.
    ///
    /// Returns the truth: zero if no shares have been recorded yet.
    fn measured_hashrate(&mut self) -> HashRate {
        self.threads
            .values_mut()
            .map(|entry| entry.hashrate.hashrate())
            .sum()
    }

    /// Aggregate measured hashrate over the responsive 1-minute window.
    fn measured_hashrate_1min(&mut self) -> HashRate {
        self.threads
            .values_mut()
            .map(|entry| entry.hashrate_1min.hashrate())
            .sum()
    }

    /// Aggregate hashrate for operational decisions.
    ///
    /// Per thread, uses measured hashrate if the estimator has settled,
    /// otherwise falls back to the static capability estimate. Suitable
    /// for broadcasting to sources and difficulty warnings, where a zero
    /// value at startup would be unhelpful.
    fn operational_hashrate(&mut self) -> HashRate {
        self.threads
            .values_mut()
            .map(|entry| {
                entry
                    .hashrate
                    .settled_hashrate()
                    .unwrap_or(entry.thread.capabilities().hashrate_estimate)
            })
            .sum()
    }

    /// Build a [`MinerState`] snapshot from current scheduler state.
    ///
    /// The scheduler contributes aggregate stats and source info. Board
    /// and thread details come from the backplane, not the scheduler, so
    /// `boards` is left empty here.
    fn compute_miner_state(&mut self) -> MinerState {
        MinerState {
            uptime_secs: self.stats.start_time.elapsed().as_secs(),
            hashrate: u64::from(self.measured_hashrate()),
            hashrate_1min: u64::from(self.measured_hashrate_1min()),
            shares_submitted: self.stats.shares_submitted,
            best_difficulty: self.stats.best_difficulty,
            paused: self.paused,
            boards: vec![],
            sources: self
                .sources
                .values()
                .map(|s| SourceState {
                    name: s.name.clone(),
                    url: s.url.clone(),
                    difficulty: s
                        .last_job
                        .as_ref()
                        .map(|j| Difficulty::from_target(j.share_target).as_u64()),
                })
                .collect(),
        }
    }

    /// Compute the per-thread scheduler target for HashTask.
    ///
    /// Clamps the source's pool difficulty between a measurement floor
    /// (1 share/sec) and a flood ceiling (10 shares/sec). When pool
    /// difficulty falls outside this range, the scheduler target
    /// overrides it; when inside, the source target passes through.
    fn compute_scheduler_target(hashrate: HashRate, source_target: Target) -> Target {
        if hashrate.is_zero() {
            return source_target;
        }

        // 1 share/sec -> more hashes per share -> harder (lower) target
        let hardest = target_for_share_rate(MEASUREMENT_SHARE_RATE, hashrate);
        // 10 shares/sec -> fewer hashes per share -> easier (higher) target
        let easiest = target_for_share_rate(FLOOD_CAP_RATE, hashrate);

        source_target.clamp(hardest, easiest)
    }

    /// Collects hashrate command senders from all sources.
    ///
    /// Used with `broadcast_hashrate()` to avoid capturing `&self` across
    /// await points (Scheduler contains Box<dyn HashThread> which isn't Sync).
    fn hashrate_senders(&self) -> Vec<mpsc::Sender<SourceCommand>> {
        self.sources
            .values()
            .map(|s| s.command_tx.clone())
            .collect()
    }

    /// Remove tasks matching a predicate, closing their share channels.
    fn remove_tasks_where(
        &mut self,
        share_channels: &mut ShareStream,
        predicate: impl Fn(&TaskEntry) -> bool,
    ) {
        let task_ids: Vec<TaskId> = self
            .tasks
            .iter()
            .filter(|(_, entry)| predicate(entry))
            .map(|(id, _)| id)
            .collect();

        for task_id in task_ids {
            self.tasks.remove(task_id);
            share_channels.remove(&task_id);
        }
    }

    /// Handle registration of a new job source.
    async fn handle_source_registration(
        &mut self,
        registration: SourceRegistration,
        source_events: &mut SourceEventStream,
    ) {
        let source_id = self.sources.insert(SourceEntry {
            name: registration.name.clone(),
            url: registration.url,
            command_tx: registration.command_tx,
            last_job: None,
            difficulty_alarm: DebouncedAlarm::new(HIGH_DIFFICULTY_DEBOUNCE),
        });
        source_events.insert(source_id, ReceiverStream::new(registration.event_rx));
        debug!(source_id = ?source_id, name = %registration.name, "Source registered");

        // Send current hashrate estimate to the new source
        let hashrate = self.operational_hashrate();
        let _ = self.sources[source_id]
            .command_tx
            .send(SourceCommand::UpdateHashRate(hashrate))
            .await;
    }

    /// Assign or replace work on all threads from a job template.
    async fn assign_job_to_threads(
        &mut self,
        mode: AssignMode,
        source_id: SourceId,
        job_template: JobTemplate,
        share_channels: &mut ShareStream,
    ) {
        let source_name = self
            .sources
            .get(source_id)
            .map(|s| s.name.clone())
            .unwrap_or_else(|| "unknown".to_string());

        // Extract EN2 range (only supported for computed merkle roots)
        let full_en2_range = match &job_template.merkle_root {
            MerkleRootKind::Computed(template) => template.extranonce2_range.clone(),
            MerkleRootKind::Fixed(_) => {
                error!(job_id = %job_template.id, "Header-only jobs not supported");
                return;
            }
        };

        let template = Arc::new(job_template);

        // Reset debounce when difficulty changes so the alarm doesn't
        // fire during the transient after a pool adjustment.
        if let Some(source) = self.sources.get_mut(source_id) {
            let prev_target = source.last_job.as_ref().map(|j| j.share_target);
            if prev_target != Some(template.share_target) {
                source.difficulty_alarm.reset();
            }
            source.last_job = Some(template.clone());
        }

        // Skip assignment if no threads registered yet
        if self.threads.is_empty() {
            debug!(source = %source_name, "No threads yet, job cached for later");
            return;
        }

        // Skip assignment if mining is paused via API. The template is
        // still stored on `source.last_job` above so `resume_mining` can
        // re-dispatch it without waiting for the source to emit a new
        // job. Threads stay idle (chips disabled) until then.
        if self.paused {
            debug!(source = %source_name, "Mining is paused; caching job for resume");
            return;
        }

        // Debounced difficulty warning
        let hashrate = self.operational_hashrate();
        if let Some(source) = self.sources.get_mut(source_id) {
            let too_high = is_difficulty_too_high(&template, hashrate);
            match source.difficulty_alarm.check(too_high) {
                AlarmStatus::Triggered => {
                    let difficulty = Difficulty::from_target(template.share_target);
                    warn!(
                        source = %source_name,
                        job_id = %template.id,
                        difficulty = %difficulty,
                        hashrate = %hashrate.to_human_readable(),
                        expected_share_interval =
                            %format_duration(expected_time_to_share_from_target(
                                template.share_target, hashrate).as_secs()),
                        "Share difficulty too high for hashrate \
                         (expected > 5 min between shares)"
                    );
                }
                AlarmStatus::Resolved => {
                    info!(
                        source = %source_name,
                        "Share difficulty now acceptable for hashrate"
                    );
                }
                _ => {}
            }
        }

        // If replacing, invalidate old tasks for this source first
        if matches!(mode, AssignMode::Replace) {
            self.remove_tasks_where(share_channels, |e| e.source_id == source_id);
        }

        // Split EN2 range among all threads
        let en2_slices = full_en2_range
            .split(self.threads.len())
            .expect("Failed to split EN2 range among threads");

        // Assign work to all threads
        for ((thread_id, entry), en2_range) in self.threads.iter_mut().zip(en2_slices) {
            let starting_en2 = en2_range.iter().next();

            let hashrate = entry
                .hashrate
                .settled_hashrate()
                .unwrap_or(entry.thread.capabilities().hashrate_estimate);
            let share_target = Self::compute_scheduler_target(hashrate, template.share_target);

            // Create share channel for this task
            let (share_tx, share_rx) = mpsc::channel(32);

            let hash_task = HashTask {
                template: template.clone(),
                en2_range: Some(en2_range),
                en2: starting_en2,
                share_target,
                ntime: template.time,
                share_tx,
            };

            let result = match mode {
                AssignMode::Update => entry.thread.update_task(hash_task).await,
                AssignMode::Replace => entry.thread.replace_task(hash_task).await,
            };

            if let Err(e) = result {
                error!(thread = %entry.thread.name(), error = %e, "Failed to assign task");
            } else {
                let task_id = self.tasks.insert(TaskEntry {
                    source_id,
                    template: template.clone(),
                    thread_id,
                });
                share_channels.insert(task_id, ReceiverStream::new(share_rx));
            }
        }
    }

    /// Handle ClearJobs event from a source.
    fn handle_clear_jobs(&mut self, source_id: SourceId, share_channels: &mut ShareStream) {
        let source_name = self
            .sources
            .get(source_id)
            .map(|s| s.name.as_str())
            .unwrap_or("unknown");
        debug!(source = %source_name, "ClearJobs received");

        // Clear cached job so newly-arriving threads don't get stale work
        if let Some(source) = self.sources.get_mut(source_id) {
            source.last_job = None;
        }

        // Remove tasks for this source (channels close, stale shares fail)
        self.remove_tasks_where(share_channels, |e| e.source_id == source_id);
    }

    /// Handle a share arriving from a task's channel.
    async fn handle_share(&mut self, task_id: TaskId, share: Share) {
        // When paused, drop everything. Chips keep mining the last job
        // we sent them and will keep emitting nonces; if we forwarded
        // them, the pool would see hashrate even though the user
        // pressed pause. Stop counting them toward the per-thread
        // hashrate estimator too so the UI reads 0 TH/s.
        if self.paused {
            trace!(?task_id, "Share dropped — scheduler is paused");
            return;
        }

        // Look up task context for routing
        let Some(task_entry) = self.tasks.get(task_id) else {
            // Task was removed (ReplaceJob/ClearJobs) but share arrived
            // before channel closed. This is normal; just drop the share.
            trace!(task_id = ?task_id, "Share for removed task (dropped)");
            return;
        };

        // Extract fields for logging (share may be consumed on submission)
        let nonce = share.nonce;
        let hash = share.hash;
        let share_difficulty = Difficulty::from_hash(&hash);
        let threshold = Difficulty::from_target(task_entry.template.share_target);

        self.stats.best_difficulty = Some(
            self.stats
                .best_difficulty
                .map_or(share_difficulty.as_u64(), |best| {
                    best.max(share_difficulty.as_u64())
                }),
        );

        debug!(
            source = %self.sources.get(task_entry.source_id).map(|s| s.name.as_str()).unwrap_or("unknown"),
            job_id = %task_entry.template.id,
            nonce = format!("{:#x}", nonce),
            hash = %hash,
            share_difficulty = %share_difficulty,
            threshold = %threshold,
            "Share found"
        );

        // Feed share work to per-thread hashrate estimator
        if let Some(entry) = self.threads.get_mut(task_entry.thread_id) {
            entry.hashrate.record(share.expected_work);
            entry.hashrate_1min.record(share.expected_work);
        }

        // Check if share meets source threshold
        if task_entry.template.share_target.is_met_by(hash) {
            self.stats.shares_submitted += 1;

            // Submit share to originating source
            if let Some(source) = self.sources.get(task_entry.source_id) {
                let source_share = SourceShare::from((share, task_entry.template.id.clone()));

                if let Err(e) = source
                    .command_tx
                    .send(SourceCommand::SubmitShare(source_share))
                    .await
                {
                    error!(
                        source_id = ?task_entry.source_id,
                        error = %e,
                        "Failed to submit share to source"
                    );
                } else {
                    debug!(source = %source.name, "Share submitted to source");
                }
            } else {
                error!(source_id = ?task_entry.source_id, "Share for unknown source");
            }
        } else {
            trace!(
                source = %self.sources.get(task_entry.source_id).map(|s| s.name.as_str()).unwrap_or("unknown"),
                job_id = %task_entry.template.id,
                nonce = format!("{:#x}", nonce),
                share_difficulty = %share_difficulty,
                threshold = %threshold,
                "Share below source threshold (not submitted)"
            );
        }
    }

    /// Handle an event from a hash thread.
    fn handle_thread_event(&mut self, thread_id: ThreadId, event: HashThreadEvent) {
        let thread_name = self
            .threads
            .get(thread_id)
            .map(|entry| entry.thread.name())
            .unwrap_or("unknown");

        match event {
            HashThreadEvent::WorkExhausted { en2_searched } => {
                info!(thread = %thread_name, en2_searched, "Work exhausted");
                // TODO: Assign new work to this thread
            }

            HashThreadEvent::WorkDepletionWarning {
                estimated_remaining_ms,
            } => {
                debug!(thread = %thread_name, remaining_ms = estimated_remaining_ms, "Work depletion warning");
                // TODO: Prepare next work assignment
            }

            HashThreadEvent::StatusUpdate(status) => {
                trace!(
                    thread = %thread_name,
                    hashrate = %status.hashrate.to_human_readable(),
                    active = status.is_active,
                    "Thread status"
                );
            }
        }
    }

    /// Handle a new thread arriving from the backplane.
    async fn handle_new_thread(
        &mut self,
        mut thread: Box<dyn HashThread>,
        thread_events: &mut ThreadEventStream,
        share_channels: &mut ShareStream,
    ) {
        let event_rx = thread
            .take_event_receiver()
            .expect("Thread missing event receiver");

        let thread_name = thread.name().to_string();
        let thread_id = self.threads.insert(ThreadEntry {
            thread,
            hashrate: HashrateEstimator::new(HASHRATE_WINDOW),
            hashrate_1min: HashrateEstimator::new_ewma(HASHRATE_WINDOW_1MIN),
        });
        thread_events.insert(thread_id, ReceiverStream::new(event_rx));
        debug!(thread = %thread_name, "Thread registered");

        // Broadcast updated hashrate to all sources
        let hashrate = self.operational_hashrate();
        let senders = self.hashrate_senders();
        broadcast_hashrate(senders, hashrate).await;

        // Reset difficulty alarm since hashrate changed
        for source in self.sources.values_mut() {
            source.difficulty_alarm.reset();
        }

        self.last_thread_count = thread_events.len();

        // Hashrate is constant for a brand-new thread (estimator has no
        // samples yet, so this always falls back to the static estimate).
        // Compute once rather than repeating inside the source loop.
        let thread_hashrate = {
            let entry = self
                .threads
                .get_mut(thread_id)
                .expect("Just inserted thread");
            entry
                .hashrate
                .settled_hashrate()
                .unwrap_or(entry.thread.capabilities().hashrate_estimate)
        };

        // Assign cached jobs from all sources to the new thread — but
        // only if the scheduler isn't paused. If it is, the thread joins
        // the paused pool and stays idle until `resume_mining` fires.
        if self.paused {
            debug!(thread = %thread_name, "Mining is paused; new thread will stay idle");
            return;
        }
        for (source_id, source) in self.sources.iter() {
            let Some(template) = &source.last_job else {
                continue;
            };

            // Extract full EN2 range (new thread overlaps with others)
            let full_en2_range = match &template.merkle_root {
                MerkleRootKind::Computed(t) => t.extranonce2_range.clone(),
                MerkleRootKind::Fixed(_) => continue,
            };

            let share_target =
                Self::compute_scheduler_target(thread_hashrate, template.share_target);

            let (share_tx, share_rx) = mpsc::channel(32);
            let hash_task = HashTask {
                template: template.clone(),
                en2_range: Some(full_en2_range.clone()),
                en2: full_en2_range.iter().next(),
                share_target,
                ntime: template.time,
                share_tx,
            };

            let entry = self
                .threads
                .get_mut(thread_id)
                .expect("Just inserted thread");
            if let Err(e) = entry.thread.update_task(hash_task).await {
                error!(thread = %thread_name, error = %e, "Failed to assign cached job");
            } else {
                let task_id = self.tasks.insert(TaskEntry {
                    source_id,
                    template: template.clone(),
                    thread_id,
                });
                share_channels.insert(task_id, ReceiverStream::new(share_rx));
                debug!(
                    thread = %thread_name,
                    source = %source.name,
                    job_id = %template.id,
                    "Assigned cached job to new thread"
                );
            }
        }
    }

    /// Detect and handle thread disconnections.
    async fn handle_thread_disconnections(
        &mut self,
        thread_events: &ThreadEventStream,
        share_channels: &mut ShareStream,
    ) {
        let current_count = thread_events.len();
        if current_count == self.last_thread_count {
            return;
        }

        debug!(
            previous = self.last_thread_count,
            current = current_count,
            "Thread count changed"
        );

        // Remove threads that no longer have active event streams
        let active_thread_ids: HashSet<_> = thread_events.keys().collect();
        self.threads.retain(|id, _| active_thread_ids.contains(&id));

        // Remove tasks for disconnected threads
        self.remove_tasks_where(share_channels, |e| {
            !active_thread_ids.contains(&e.thread_id)
        });

        self.last_thread_count = current_count;

        // Broadcast updated hashrate to all sources
        let hashrate = self.operational_hashrate();
        let senders = self.hashrate_senders();
        broadcast_hashrate(senders, hashrate).await;

        // Reset difficulty alarm since hashrate changed
        for source in self.sources.values_mut() {
            source.difficulty_alarm.reset();
        }
    }

    /// Handle an API command, sending the result back on the reply channel.
    ///
    /// Publishes an updated state snapshot before replying so the API
    /// handler's subsequent `borrow()` sees the new value.
    async fn handle_api_command(
        &mut self,
        cmd: SchedulerCommand,
        miner_state_tx: &watch::Sender<MinerState>,
        share_channels: &mut ShareStream,
    ) {
        match cmd {
            SchedulerCommand::PauseMining { reply } => {
                if !self.paused {
                    self.paused = true;
                    info!(
                        thread_count = self.threads.len(),
                        "Mining paused — share submissions and new job dispatch stopped"
                    );
                    // Tell every hash thread so its own status (per-board
                    // hashrate + is_active) zeroes immediately, instead
                    // of carrying pre-pause numbers in the UI.
                    for (id, entry) in self.threads.iter_mut() {
                        if let Err(e) = entry.thread.set_paused(true).await {
                            warn!(thread_id = ?id, thread = %entry.thread.name(), error = %e, "set_paused(true) failed");
                        }
                    }
                    // Soft pause: drop any in-flight task on each hash
                    // thread but DON'T touch the chip power rail. Chips
                    // burn through whatever nonces are still queued on
                    // their last job, mujina drops every share that
                    // comes back (`assign_job_to_threads` is gated on
                    // `!self.paused`, and the task lookup in
                    // `handle_share` only matches templates we still
                    // have entries for; clearing `current_task` on the
                    // hash thread stops ntime rolling so the chip work
                    // staleness curve takes over within a minute).
                    //
                    // Hard pause (full asic_enable.disable() + chip
                    // power cycle) is broken on BHB56902 today because
                    // the post-reset re-enumeration only sees ~half the
                    // chain at 115200 — likely a chain-RST_N propagation
                    // issue with our reset_release_ms timing. Left as a
                    // follow-up; tracked in the TODO below.
                    for (id, entry) in self.threads.iter_mut() {
                        // Reset the per-thread hashrate estimator so the
                        // API/UI stops showing the pre-pause hashrate
                        // while the window ages out.
                        entry.hashrate = HashrateEstimator::new(HASHRATE_WINDOW);
                        entry.hashrate_1min = HashrateEstimator::new_ewma(HASHRATE_WINDOW_1MIN);
                        debug!(thread_id = ?id, thread = %entry.thread.name(), "Thread marked paused");
                    }
                    // Note: we intentionally do NOT call
                    // `thread.go_idle()` here. That triggers
                    // disable_chips() which has the reset-baud
                    // recovery problem; until that's debugged we leave
                    // chips warm and rely on `self.paused` gating in
                    // `assign_job_to_threads` to stop new work.
                } else {
                    debug!("PauseMining received but scheduler is already paused");
                }
                let _ = miner_state_tx.send(self.compute_miner_state());
                let _ = reply.send(Ok(()));
            }
            SchedulerCommand::ResumeMining { reply } => {
                if self.paused {
                    self.paused = false;
                    // A resume cold-inits the chains, returning the rail to the
                    // factory voltage — re-seed the tracker so the next
                    // SetOperatingPoint picks the right V/f ordering.
                    self.current_voltage_v = COLD_INIT_VOLTAGE_V;
                    info!(
                        thread_count = self.threads.len(),
                        "Mining resumed — re-dispatching cached jobs"
                    );
                    // Tell every thread to start feeding its own status
                    // hashrate again. assign_job_to_threads below will
                    // also re-dispatch cached work so shares start
                    // flowing back.
                    for (id, entry) in self.threads.iter_mut() {
                        if let Err(e) = entry.thread.set_paused(false).await {
                            warn!(thread_id = ?id, thread = %entry.thread.name(), error = %e, "set_paused(false) failed");
                        }
                    }
                    // Snapshot the latest job per source. We can't iterate
                    // `self.sources` while calling `assign_job_to_threads`
                    // (which borrows `&mut self`), so collect first.
                    let jobs_to_redispatch: Vec<(SourceId, JobTemplate)> = self
                        .sources
                        .iter()
                        .filter_map(|(id, source)| {
                            source.last_job.as_ref().map(|t| (id, t.as_ref().clone()))
                        })
                        .collect();
                    for (source_id, template) in jobs_to_redispatch {
                        self.assign_job_to_threads(
                            AssignMode::Replace,
                            source_id,
                            template,
                            share_channels,
                        )
                        .await;
                    }
                } else {
                    debug!("ResumeMining received but scheduler is not paused");
                }
                let _ = miner_state_tx.send(self.compute_miner_state());
                let _ = reply.send(Ok(()));
            }
            SchedulerCommand::SetFrequency { mhz, reply } => {
                info!(
                    mhz,
                    thread_count = self.threads.len(),
                    "Setting chip frequency on all chains (power dial)"
                );
                // Per-chain re-ramp at fixed voltage. Each thread clamps to
                // its own safe range. Collect the last error (if any) so the
                // HTTP caller learns it didn't fully apply.
                let mut last_err: Option<String> = None;
                for (id, entry) in self.threads.iter_mut() {
                    guard_thread_op(
                        tokio::time::timeout(THREAD_OP_TIMEOUT, entry.thread.set_frequency(mhz)).await,
                        id, "set_frequency", &mut last_err,
                    );
                }
                let _ = miner_state_tx.send(self.compute_miner_state());
                let _ = reply.send(match last_err {
                    Some(e) => Err(anyhow::anyhow!(e)),
                    None => Ok(()),
                });
            }
            SchedulerCommand::SetOperatingPoint { mhz, volts, reply } => {
                // Skip the voltage phase entirely when the requested rail is
                // already where it is (within DAC-quantization noise). Every
                // set_voltage call is a real, several-hundred-ms-plus i2c PSU
                // exchange PER CHAIN (see NativeAmlogicPsu::exchange) -- for
                // chassis whose effective rail is a constant regardless of
                // frequency (e.g. this S19j/S19k Amlogic driver -- see
                // `effective_voltage_spec`), Nova's dial sends the SAME
                // voltage on essentially every operating-point change, so
                // without this check every dial move paid ~3 chains worth of
                // no-op PSU writes -- measured on .222 as a flat ~2-3s+ delay
                // before the commanded frequency (and so the real wattage)
                // even started moving, well before the ramp itself begins.
                let voltage_unchanged = (volts - self.current_voltage_v).abs() < VOLTAGE_APPLY_EPS_V;
                let lowering_voltage = volts < self.current_voltage_v;
                info!(
                    mhz,
                    volts,
                    from_volts = self.current_voltage_v,
                    lowering_voltage,
                    voltage_unchanged,
                    "Setting operating point (V/f) on all chains"
                );

                // V/f safety ordering. Frequency is per-chain; the voltage rail
                // is shared, so setting it on any one chain moves the whole
                // board (the rest are no-ops). The two sequential loops give
                // the ordering: never a high frequency at a low voltage.
                //   - lowering power: ALL chains drop frequency, THEN voltage.
                //   - raising power:  voltage up, THEN ALL chains raise freq.
                // When voltage isn't actually moving there's no V/f race to
                // guard against, so skip straight to frequency-only.
                let mut last_err: Option<String> = None;
                if voltage_unchanged {
                    for (id, entry) in self.threads.iter_mut() {
                        guard_thread_op(
                            tokio::time::timeout(THREAD_OP_TIMEOUT, entry.thread.set_frequency(mhz)).await,
                            id, "set_frequency", &mut last_err,
                        );
                    }
                } else if lowering_voltage {
                    for (id, entry) in self.threads.iter_mut() {
                        guard_thread_op(
                            tokio::time::timeout(THREAD_OP_TIMEOUT, entry.thread.set_frequency(mhz)).await,
                            id, "set_frequency", &mut last_err,
                        );
                    }
                    for (id, entry) in self.threads.iter_mut() {
                        guard_thread_op(
                            tokio::time::timeout(THREAD_OP_TIMEOUT, entry.thread.set_voltage(volts)).await,
                            id, "set_voltage", &mut last_err,
                        );
                    }
                } else {
                    for (id, entry) in self.threads.iter_mut() {
                        guard_thread_op(
                            tokio::time::timeout(THREAD_OP_TIMEOUT, entry.thread.set_voltage(volts)).await,
                            id, "set_voltage", &mut last_err,
                        );
                    }
                    for (id, entry) in self.threads.iter_mut() {
                        guard_thread_op(
                            tokio::time::timeout(THREAD_OP_TIMEOUT, entry.thread.set_frequency(mhz)).await,
                            id, "set_frequency", &mut last_err,
                        );
                    }
                }
                self.current_voltage_v = volts;
                let _ = miner_state_tx.send(self.compute_miner_state());
                let _ = reply.send(match last_err {
                    Some(e) => Err(anyhow::anyhow!(e)),
                    None => Ok(()),
                });
            }
        }
    }

    /// Main scheduler loop.
    async fn run(
        &mut self,
        running: CancellationToken,
        mut thread_rx: mpsc::Receiver<Box<dyn HashThread>>,
        mut source_reg_rx: mpsc::Receiver<SourceRegistration>,
        miner_state_tx: watch::Sender<MinerState>,
        mut cmd_rx: mpsc::Receiver<SchedulerCommand>,
    ) {
        // StreamMaps as locals (not in self) to avoid borrow conflicts in select!
        let mut source_events: SourceEventStream = StreamMap::new();
        let mut thread_events: ThreadEventStream = StreamMap::new();
        let mut share_channels: ShareStream = StreamMap::new();

        // Create interval for periodic status logging
        let mut status_interval = tokio::time::interval(Duration::from_secs(30));
        status_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut first_status_tick = true;

        // Create interval for periodic hashrate broadcasts to sources
        let mut hashrate_interval = tokio::time::interval(Duration::from_secs(10));
        hashrate_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut first_hashrate_tick = true;

        // Publish the API/UI state snapshot frequently — decoupled from the 10 s
        // pool hashrate broadcast — so the UI reflects changes within ~2 s.
        let mut state_publish_interval = tokio::time::interval(Duration::from_secs(2));
        state_publish_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        while !running.is_cancelled() {
            tokio::select! {
                // Source registration
                Some(registration) = source_reg_rx.recv() => {
                    self.handle_source_registration(registration, &mut source_events).await;
                }

                // Source events
                Some((source_id, event)) = source_events.next() => {
                    let source_name = self.sources.get(source_id)
                        .map(|s| s.name.as_str())
                        .unwrap_or("unknown");

                    match event {
                        SourceEvent::UpdateJob(job_template) => {
                            debug!(
                                source = %source_name,
                                job_id = %job_template.id,
                                "UpdateJob received"
                            );
                            self.assign_job_to_threads(
                                AssignMode::Update,
                                source_id,
                                job_template,
                                &mut share_channels,
                            ).await;
                        }

                        SourceEvent::ReplaceJob(job_template) => {
                            debug!(
                                source = %source_name,
                                job_id = %job_template.id,
                                "ReplaceJob received"
                            );
                            self.assign_job_to_threads(
                                AssignMode::Replace,
                                source_id,
                                job_template,
                                &mut share_channels,
                            ).await;
                        }

                        SourceEvent::ClearJobs => {
                            self.handle_clear_jobs(source_id, &mut share_channels);
                        }
                    }
                }

                // Share channels (from tasks)
                Some((task_id, share)) = share_channels.next() => {
                    self.handle_share(task_id, share).await;
                }

                // Thread events
                Some((thread_id, event)) = thread_events.next() => {
                    self.handle_thread_event(thread_id, event);
                }

                // New thread from backplane
                Some(thread) = thread_rx.recv() => {
                    self.handle_new_thread(thread, &mut thread_events, &mut share_channels).await;
                }

                // Periodic status logging
                _ = status_interval.tick() => {
                    if first_status_tick {
                        first_status_tick = false;
                    } else {
                        let hashrate = self.measured_hashrate();
                        self.stats.log_summary(hashrate);
                    }
                }

                // API commands
                Some(cmd) = cmd_rx.recv() => {
                    self.handle_api_command(cmd, &miner_state_tx, &mut share_channels).await;
                }

                // Periodic hashrate broadcast to pool sources (10 s).
                _ = hashrate_interval.tick() => {
                    if first_hashrate_tick {
                        first_hashrate_tick = false;
                    } else {
                        let hashrate = self.operational_hashrate();
                        let senders = self.hashrate_senders();
                        broadcast_hashrate(senders, hashrate).await;
                    }
                }

                // Frequent API/UI state snapshot publish (~2 s).
                _ = state_publish_interval.tick() => {
                    let _ = miner_state_tx.send(self.compute_miner_state());
                }

                // Shutdown
                _ = running.cancelled() => {
                    debug!("Scheduler shutdown requested");
                    break;
                }
            }

            // Detect thread disconnections (StreamMap silently removes ended streams)
            self.handle_thread_disconnections(&thread_events, &mut share_channels)
                .await;
        }

        // Shut down all threads (async cleanup before drop)
        for (_, entry) in &mut self.threads {
            if let Err(e) = entry.thread.shutdown().await {
                warn!(error = %e, "Thread shutdown error");
            }
        }

        // Log final statistics
        let hashrate = self.measured_hashrate();
        self.stats.log_summary(hashrate);

        debug!("Scheduler shutdown complete");
    }
}

/// Broadcasts hashrate update to all registered sources.
///
/// Takes pre-collected senders to avoid capturing Scheduler across await
/// points (it contains Box<dyn HashThread> which isn't Sync).
async fn broadcast_hashrate(senders: Vec<mpsc::Sender<SourceCommand>>, hashrate: HashRate) {
    for sender in senders {
        let _ = sender.send(SourceCommand::UpdateHashRate(hashrate)).await;
    }
}

/// Threshold for warning about high share difficulty.
///
/// If expected time to find a share exceeds this, warn the operator that the
/// pool difficulty may be misconfigured for this hashrate.
const HIGH_DIFFICULTY_THRESHOLD: Duration = Duration::from_secs(300); // 5 minutes

/// How long difficulty must remain too high before warning.
///
/// Absorbs transients like pool connections starting with a default
/// difficulty before `suggest_difficulty` takes effect, and brief
/// hashrate changes from board hotplug.
const HIGH_DIFFICULTY_DEBOUNCE: Duration = Duration::from_secs(30);

/// Check whether job difficulty is unreasonably high for our hashrate.
fn is_difficulty_too_high(job: &JobTemplate, hashrate: HashRate) -> bool {
    if hashrate.is_zero() {
        return false;
    }

    let time_to_share = expected_time_to_share_from_target(job.share_target, hashrate);
    time_to_share > HIGH_DIFFICULTY_THRESHOLD
}

/// Run the scheduler task, receiving hash threads and job sources.
pub async fn task(
    running: CancellationToken,
    thread_rx: mpsc::Receiver<Box<dyn HashThread>>,
    source_reg_rx: mpsc::Receiver<SourceRegistration>,
    miner_state_tx: watch::Sender<MinerState>,
    cmd_rx: mpsc::Receiver<SchedulerCommand>,
) {
    let mut scheduler = Scheduler::new();
    scheduler
        .run(running, thread_rx, source_reg_rx, miner_state_tx, cmd_rx)
        .await;
}

/// Format seconds as human-readable duration.
///
/// Scales format based on duration to keep output compact:
/// - Under 1 minute: "45s"
/// - Under 1 hour: "12m 30s"
/// - Under 1 day: "12h 38m"
/// - 1 day or more: "1d 12h"
fn format_duration(secs: u64) -> String {
    const MINUTE: u64 = 60;
    const HOUR: u64 = 60 * MINUTE;
    const DAY: u64 = 24 * HOUR;

    if secs >= DAY {
        let days = secs / DAY;
        let hours = (secs % DAY) / HOUR;
        format!("{}d {}h", days, hours)
    } else if secs >= HOUR {
        let hours = secs / HOUR;
        let mins = (secs % HOUR) / MINUTE;
        format!("{}h {}m", hours, mins)
    } else if secs >= MINUTE {
        let mins = secs / MINUTE;
        let s = secs % MINUTE;
        format!("{}m {}s", mins, s)
    } else {
        format!("{}s", secs)
    }
}

/// Mining statistics tracker.
#[derive(Debug)]
struct MiningStats {
    start_time: std::time::Instant,
    shares_submitted: u64,
    best_difficulty: Option<u64>,
}

impl Default for MiningStats {
    fn default() -> Self {
        Self {
            start_time: std::time::Instant::now(),
            shares_submitted: 0,
            best_difficulty: None,
        }
    }
}

impl MiningStats {
    fn log_summary(&self, hashrate: HashRate) {
        let elapsed = self.start_time.elapsed();

        let hashrate_str = if hashrate.is_zero() {
            "--".to_string()
        } else {
            hashrate.to_human_readable()
        };

        info!(
            uptime = %format_duration(elapsed.as_secs()),
            hashrate = %hashrate_str,
            shares = self.shares_submitted,
            "Mining status."
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Difficulty;

    #[test]
    fn scheduler_target_zero_hashrate_passthrough() {
        let source_target = Difficulty::from(1024).to_target();
        let result = Scheduler::compute_scheduler_target(HashRate::from(0), source_target);
        assert_eq!(result, source_target);
    }

    #[test]
    fn scheduler_target_passthrough_when_in_range() {
        // Pick a source target that falls between the two bounds.
        // At 1 TH/s the bounds span roughly difficulty 23 (easiest)
        // to difficulty 233 (hardest). Difficulty 100 sits in between.
        let hashrate = HashRate::from_terahashes(1.0);
        let source_target = Difficulty::from(100).to_target();
        let result = Scheduler::compute_scheduler_target(hashrate, source_target);
        assert_eq!(result, source_target);
    }

    #[test]
    fn scheduler_target_clamps_hard_source_to_easier() {
        // Pool difficulty much higher than what our hashrate warrants.
        // The scheduler should ease it to the measurement floor so the
        // estimator gets samples.
        let hashrate = HashRate::from_terahashes(1.0);
        let very_hard = Difficulty::from(1_000_000).to_target();
        let result = Scheduler::compute_scheduler_target(hashrate, very_hard);

        let hardest = target_for_share_rate(MEASUREMENT_SHARE_RATE, hashrate);
        assert_eq!(result, hardest, "should clamp to measurement floor");
        assert!(result > very_hard, "clamped target should be easier");
    }

    #[test]
    fn scheduler_target_clamps_easy_source_to_harder() {
        // Pool difficulty absurdly low -- would flood the scheduler.
        // The scheduler should harden it to the flood ceiling.
        let hashrate = HashRate::from_terahashes(1.0);
        let very_easy = Target::MAX;
        let result = Scheduler::compute_scheduler_target(hashrate, very_easy);

        let easiest = target_for_share_rate(FLOOD_CAP_RATE, hashrate);
        assert_eq!(result, easiest, "should clamp to flood ceiling");
        assert!(result < very_easy, "clamped target should be harder");
    }

    #[test]
    fn scheduler_target_clamp_ordering_invariant() {
        // Verify hardest <= easiest in Ord terms for several
        // representative hashrates. This is the invariant that
        // clamp(hardest, easiest) relies on to not panic.
        for hashrate in [
            HashRate::from_megahashes(5.0),
            HashRate::from_gigahashes(500.0),
            HashRate::from_terahashes(1.0),
            HashRate::from_terahashes(100.0),
        ] {
            let hardest = target_for_share_rate(MEASUREMENT_SHARE_RATE, hashrate);
            let easiest = target_for_share_rate(FLOOD_CAP_RATE, hashrate);
            assert!(
                hardest <= easiest,
                "clamp invariant violated at {hashrate}: \
                 hardest={hardest:?} easiest={easiest:?}"
            );
        }
    }
}
