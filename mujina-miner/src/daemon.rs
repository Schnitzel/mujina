//! Daemon lifecycle management for mujina-miner.
//!
//! This module handles the core daemon functionality including initialization,
//! task management, signal handling, and graceful shutdown.

use std::env;
use std::sync::{Arc, Mutex};

use tokio::signal::unix::{self, SignalKind};
use tokio::sync::{mpsc, watch};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::api_client::types::MinerState;
use crate::tracing::prelude::*;
use crate::{
    api::{self, ApiConfig, BoardRegistry, commands::SchedulerCommand},
    asic::hash_thread::HashThread,
    backplane::Backplane,
    board::s19x_amlogic,
    config::{Config, HashboardModel},
    cpu_miner::CpuMinerConfig,
    display::gt_touch,
    job_source::{
        SourceCommand, SourceEvent,
        dummy::DummySource,
        forced_rate::{ForcedRateConfig, ForcedRateSource},
        stratum_v1::StratumV1Source,
    },
    scheduler::{self, SourceRegistration},
    stratum_v1::{PoolConfig as StratumPoolConfig, TcpConnector},
    transport::{
        AmlogicDeviceInfo, CpuDeviceInfo, TransportEvent, amlogic as amlogic_transport,
        cpu as cpu_transport,
    },
};

#[cfg(feature = "usb-discovery")]
use crate::transport::UsbTransport;

/// The main daemon.
pub struct Daemon {
    config: Option<Config>,
    shutdown: CancellationToken,
    tracker: TaskTracker,
}

impl Daemon {
    /// Create a new daemon instance.
    pub fn new(config: Option<Config>) -> Self {
        Self {
            config,
            shutdown: CancellationToken::new(),
            tracker: TaskTracker::new(),
        }
    }

    /// Run the daemon until shutdown is requested.
    pub async fn run(self) -> anyhow::Result<()> {
        let result = async {
            // Create channels for component communication
            let (transport_tx, transport_rx) = mpsc::channel::<TransportEvent>(100);
            let (thread_tx, thread_rx) = mpsc::channel::<Box<dyn HashThread>>(10);
            let (source_reg_tx, source_reg_rx) = mpsc::channel::<SourceRegistration>(10);

            // Create and start USB transport discovery
            #[cfg(feature = "usb-discovery")]
            {
                if std::env::var("MUJINA_USB_DISABLE").is_err() {
                    let usb_transport = UsbTransport::new(transport_tx.clone());
                    if let Err(e) = usb_transport.start_discovery(self.shutdown.clone()).await {
                        error!("Failed to start USB discovery: {}", e);
                    }
                } else {
                    info!("USB discovery disabled (MUJINA_USB_DISABLE set)");
                }
            }

            #[cfg(not(feature = "usb-discovery"))]
            {
                info!("USB discovery disabled (compiled without usb-discovery feature)");
            }

            if let Some(config) = self
                .config
                .as_ref()
                .and_then(Config::enabled_amlogic_control_board)
            {
                // Both S19j Pro and S19k Pro run on the same Amlogic
                // A113D control board and are handled by the single
                // unified `s19x_amlogic` driver; only the chip family
                // and hashboard topology differ (factored per-model
                // inside the driver). They share the
                // AmlogicControlBoardConfig schema. The model — taken
                // from the first configured hashboard — is still carried
                // on the transport event so downstream code can label
                // the board correctly.
                let model = config
                    .hashboards
                    .first()
                    .map(|hb| hb.model)
                    .unwrap_or(HashboardModel::S19jPro);

                let device_id = s19x_amlogic::device_id(config);
                s19x_amlogic::install_config(config.clone())?;

                info!(
                    board = %device_id,
                    model = ?model,
                    "Native Amlogic control board enabled from config"
                );

                let event =
                    TransportEvent::Amlogic(amlogic_transport::TransportEvent::DeviceConnected(
                        AmlogicDeviceInfo {
                            device_id,
                            model,
                        },
                    ));
                if let Err(e) = transport_tx.send(event).await {
                    error!("Failed to send native Amlogic board event: {}", e);
                }
            }

            // Inject CPU miner virtual device if configured
            if let Some(config) = CpuMinerConfig::from_env() {
                info!(
                    threads = config.thread_count,
                    duty = config.duty_percent,
                    "CPU miner enabled"
                );
                let event = TransportEvent::Cpu(cpu_transport::TransportEvent::CpuDeviceConnected(
                    CpuDeviceInfo {
                        device_id: format!("cpu-{}x{}%", config.thread_count, config.duty_percent),
                        thread_count: config.thread_count,
                        duty_percent: config.duty_percent,
                    },
                ));
                if let Err(e) = transport_tx.send(event).await {
                    error!("Failed to send CPU miner event: {}", e);
                }
            }

            // Board registration channel: backplane forwards board
            // registrations here, the API server collects and serves them.
            let (board_reg_tx, board_reg_rx) = mpsc::channel(10);
            let board_registry = Arc::new(Mutex::new(BoardRegistry::new()));

            self.tracker.spawn({
                let board_registry = board_registry.clone();
                async move {
                    let mut board_reg_rx = board_reg_rx;
                    while let Some(reg) = board_reg_rx.recv().await {
                        board_registry
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .push(reg);
                    }
                }
            });

            // Miner state channel: scheduler publishes snapshots, API serves them.
            // Created early so backplane can pass receiver to boards for display.
            let (miner_state_tx, miner_state_rx) = watch::channel(MinerState::default());

            // Create and start backplane
            let mut backplane = Backplane::new(
                transport_rx,
                thread_tx,
                board_reg_tx,
                miner_state_rx.clone(),
            );
            self.tracker.spawn({
                let shutdown = self.shutdown.clone();
                async move {
                    tokio::select! {
                        result = backplane.run() => {
                            if let Err(e) = result {
                                error!("Backplane error: {}", e);
                            }
                        }
                        _ = shutdown.cancelled() => {}
                    }

                    backplane.shutdown_all_boards().await;
                }
            });

            // Create job source (Stratum v1 or Dummy)
            // Controlled by environment variables:
            // - MUJINA_POOL_URL: Pool address (e.g., stratum+tcp://localhost:3333)
            // - MUJINA_POOL_USER: Worker username (optional, defaults to "mujina-testing")
            // - MUJINA_POOL_PASS: Worker password (optional, defaults to "x")
            let (source_event_tx, source_event_rx) = mpsc::channel::<SourceEvent>(100);
            let (source_cmd_tx, source_cmd_rx) = mpsc::channel(10);

            if let Ok(pool_url) = env::var("MUJINA_POOL_URL") {
                // Use Stratum v1 source
                let pool_user =
                    env::var("MUJINA_POOL_USER").unwrap_or_else(|_| "mujina-testing".to_string());
                let pool_pass = env::var("MUJINA_POOL_PASS").unwrap_or_else(|_| "x".to_string());

                let stratum_config = StratumPoolConfig {
                    url: pool_url.clone(),
                    username: pool_user,
                    password: pool_pass,
                    user_agent: "mujina-miner/0.1.0-alpha".to_string(),
                };

                // Optionally wrap with ForcedRateSource for testing
                if let Some(forced_rate_config) = ForcedRateConfig::from_env() {
                    info!(
                        rate = %forced_rate_config.target_rate,
                        "Forced share rate wrapper enabled"
                    );

                    // Create inner channels (stratum <-> wrapper)
                    let (inner_event_tx, inner_event_rx) = mpsc::channel::<SourceEvent>(100);
                    let (inner_cmd_tx, inner_cmd_rx) = mpsc::channel::<SourceCommand>(10);

                    let stratum_source = StratumV1Source::new(
                        stratum_config,
                        inner_cmd_rx,
                        inner_event_tx,
                        self.shutdown.clone(),
                        Box::new(TcpConnector::new(pool_url.clone())),
                    );
                    let stratum_name = stratum_source.name();

                    // Spawn stratum source
                    self.tracker.spawn(async move {
                        if let Err(e) = stratum_source.run().await {
                            error!("Stratum v1 source error: {}", e);
                        }
                    });

                    // Create and spawn wrapper (uses outer channels from above)
                    let forced_rate = ForcedRateSource::new(
                        forced_rate_config,
                        inner_event_rx,
                        source_event_tx,
                        inner_cmd_tx,
                        source_cmd_rx,
                        self.shutdown.clone(),
                    );

                    source_reg_tx
                        .send(SourceRegistration {
                            name: format!("{} (forced-rate)", stratum_name),
                            url: Some(pool_url.clone()),
                            event_rx: source_event_rx,
                            command_tx: source_cmd_tx,
                        })
                        .await?;

                    self.tracker.spawn(async move {
                        if let Err(e) = forced_rate.run().await {
                            error!("Forced rate wrapper error: {}", e);
                        }
                    });
                } else {
                    // Direct stratum source (no wrapper)
                    let stratum_source = StratumV1Source::new(
                        stratum_config,
                        source_cmd_rx,
                        source_event_tx,
                        self.shutdown.clone(),
                        Box::new(TcpConnector::new(pool_url.clone())),
                    );

                    source_reg_tx
                        .send(SourceRegistration {
                            name: stratum_source.name(),
                            url: Some(pool_url),
                            event_rx: source_event_rx,
                            command_tx: source_cmd_tx,
                        })
                        .await?;

                    self.tracker.spawn(async move {
                        if let Err(e) = stratum_source.run().await {
                            error!("Stratum v1 source error: {}", e);
                        }
                    });
                }
            } else {
                // Use DummySource
                info!("Using dummy job source (set MUJINA_POOL_URL to use Stratum v1)");

                let dummy_source = DummySource::new(
                    source_cmd_rx,
                    source_event_tx,
                    self.shutdown.clone(),
                    tokio::time::Duration::from_secs(30),
                )?;

                source_reg_tx
                    .send(SourceRegistration {
                        name: "dummy".into(),
                        url: None,
                        event_rx: source_event_rx,
                        command_tx: source_cmd_tx,
                    })
                    .await?;

                self.tracker.spawn(async move {
                    if let Err(e) = dummy_source.run().await {
                        error!("DummySource error: {}", e);
                    }
                });
            }

            // Command channel: API sends commands, scheduler processes them.
            let (scheduler_cmd_tx, scheduler_cmd_rx) = mpsc::channel::<SchedulerCommand>(16);

            // Start the scheduler
            self.tracker.spawn(scheduler::task(
                self.shutdown.clone(),
                thread_rx,
                source_reg_rx,
                miner_state_tx,
                scheduler_cmd_rx,
            ));

            // Start the API server
            self.tracker.spawn({
                let shutdown = self.shutdown.clone();
                let board_registry = board_registry.clone();
                let api_miner_state_rx = miner_state_rx.clone();
                async move {
                    // ASCII 'M' (77) + 'U' (85) = 7785
                    const API_PORT: u16 = 7785;

                    let bind_addr = match env::var("MUJINA_API_LISTEN") {
                        Ok(addr) if addr.contains(':') => addr,
                        Ok(addr) => format!("{addr}:{API_PORT}"),
                        Err(_) => format!("127.0.0.1:{API_PORT}"),
                    };
                    let config = ApiConfig { bind_addr };
                    if let Err(e) = api::serve(
                        config,
                        shutdown,
                        api_miner_state_rx,
                        board_registry,
                        scheduler_cmd_tx,
                    )
                    .await
                    {
                        error!("API server error: {}", e);
                    }
                }
            });

            if let Some(gt_touch_config) = self
                .config
                .as_ref()
                .and_then(Config::enabled_amlogic_control_board)
                .and_then(|config| config.gt_touch_display.clone())
                .filter(|config| config.enabled)
            {
                let default_asic_model = self
                    .config
                    .as_ref()
                    .and_then(Config::enabled_amlogic_control_board)
                    .and_then(|config| config.hashboards.first())
                    .map(|hashboard| hashboard.model.asic_model_label().to_string());
                let default_device_model = self
                    .config
                    .as_ref()
                    .and_then(Config::enabled_amlogic_control_board)
                    .and_then(|config| config.hashboards.first())
                    .map(|hashboard| hashboard.model.board_model_label().to_string());
                let default_voltage = self
                    .config
                    .as_ref()
                    .and_then(Config::enabled_amlogic_control_board)
                    .map(|config| config.startup.initial_voltage);
                let default_pool_url = env::var("MUJINA_POOL_URL").ok();
                let default_pool_user = env::var("MUJINA_POOL_USER").ok();

                self.tracker.spawn(gt_touch::task(
                    gt_touch_config,
                    gt_touch::ServiceContext {
                        miner_state_rx: miner_state_rx.clone(),
                        board_registry: board_registry.clone(),
                        default_device_model,
                        default_asic_model,
                        default_pool_url,
                        default_pool_user,
                        default_mode: Some("normal".to_string()),
                        default_voltage,
                    },
                    self.shutdown.clone(),
                ));
            }

            info!("Started.");
            info!("For debugging, set RUST_LOG=mujina_miner=debug or trace.");

            // Install signal handlers
            let mut sigint = unix::signal(SignalKind::interrupt())?;
            let mut sigterm = unix::signal(SignalKind::terminate())?;

            // Wait for shutdown signal
            tokio::select! {
                _ = sigint.recv() => {
                    info!("Received SIGINT.");
                },
                _ = sigterm.recv() => {
                    info!("Received SIGTERM.");
                },
            }

            Ok(())
        }
        .await;

        self.shutdown.cancel();
        self.tracker.close();
        self.tracker.wait().await;

        match &result {
            Ok(()) => info!("Exiting."),
            Err(error) => error!(error = %error, "Exiting after daemon error."),
        }

        result
    }
}

impl Default for Daemon {
    fn default() -> Self {
        Self::new(None)
    }
}
