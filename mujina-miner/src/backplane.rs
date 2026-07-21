//! Backplane for board communication and lifecycle management.
//!
//! The Backplane acts as the communication substrate between mining boards and
//! the scheduler. Like a hardware backplane, it provides connection points for
//! boards to plug into, routes events between components, and manages board
//! lifecycle (hotplug, emergency shutdown, etc.).

use crate::{
    api_client::types::MinerState,
    asic::hash_thread::HashThread,
    board::{Board, BoardDescriptor, BoardRegistration, VirtualBoardRegistry},
    error::Result,
    tracing::prelude::*,
    transport::{
        TransportEvent, UsbDeviceInfo, amlogic::TransportEvent as AmlogicTransportEvent,
        cpu::TransportEvent as CpuTransportEvent, usb::TransportEvent as UsbTransportEvent,
    },
};
use crate::api::commands::SchedulerCommand;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot, watch};

/// Board registry that uses inventory to find registered boards.
pub struct BoardRegistry;

impl BoardRegistry {
    /// Find the best matching board descriptor for this USB device.
    ///
    /// Uses pattern matching with specificity scoring to select the most
    /// appropriate board handler. When multiple patterns match, the one
    /// with the highest specificity score wins.
    ///
    /// Returns None if no registered boards match the device.
    pub fn find_descriptor(&self, device: &UsbDeviceInfo) -> Option<&'static BoardDescriptor> {
        inventory::iter::<BoardDescriptor>()
            .filter(|desc| desc.pattern.matches(device))
            .max_by_key(|desc| desc.pattern.specificity())
    }
}

/// Backplane that connects boards to the scheduler.
///
/// Acts as the communication substrate between mining boards and the work
/// scheduler. Boards plug into the backplane, which routes their events and
/// manages their lifecycle.
pub struct Backplane {
    registry: BoardRegistry,
    virtual_registry: VirtualBoardRegistry,
    /// Active boards managed by the backplane
    boards: HashMap<String, Box<dyn Board + Send>>,
    event_rx: mpsc::Receiver<TransportEvent>,
    /// Channel to send hash threads to the scheduler
    scheduler_tx: mpsc::Sender<Box<dyn HashThread>>,
    /// Channel to forward board registrations to the API server
    board_reg_tx: mpsc::Sender<BoardRegistration>,
    /// Receiver for miner state (passed to boards for display)
    miner_state_rx: watch::Receiver<MinerState>,
    /// Command channel to the scheduler. Used to start the scheduler PAUSED for
    /// a board that came up not-yet-minable (see `Board::needs_initial_pause`).
    scheduler_cmd_tx: mpsc::Sender<crate::api::commands::SchedulerCommand>,
}

impl Backplane {
    /// Create a new backplane.
    pub fn new(
        event_rx: mpsc::Receiver<TransportEvent>,
        scheduler_tx: mpsc::Sender<Box<dyn HashThread>>,
        board_reg_tx: mpsc::Sender<BoardRegistration>,
        miner_state_rx: watch::Receiver<MinerState>,
        scheduler_cmd_tx: mpsc::Sender<crate::api::commands::SchedulerCommand>,
    ) -> Self {
        Self {
            registry: BoardRegistry,
            virtual_registry: VirtualBoardRegistry,
            boards: HashMap::new(),
            event_rx,
            scheduler_tx,
            board_reg_tx,
            miner_state_rx,
            scheduler_cmd_tx,
        }
    }

    /// Run the backplane event loop.
    pub async fn run(&mut self) -> Result<()> {
        while let Some(event) = self.event_rx.recv().await {
            match event {
                TransportEvent::Amlogic(amlogic_event) => {
                    self.handle_amlogic_event(amlogic_event).await?;
                }
                TransportEvent::Usb(usb_event) => {
                    self.handle_usb_event(usb_event).await?;
                }
                TransportEvent::Cpu(cpu_event) => {
                    self.handle_cpu_event(cpu_event).await?;
                }
            }
        }

        Ok(())
    }

    /// Shutdown all boards managed by this backplane.
    pub async fn shutdown_all_boards(&mut self) {
        let board_ids: Vec<String> = self.boards.keys().cloned().collect();

        for board_id in board_ids {
            if let Some(mut board) = self.boards.remove(&board_id) {
                let model = board.board_info().model;
                debug!(board = %model, serial = %board_id, "Shutting down board");

                match board.shutdown().await {
                    Ok(()) => {
                        debug!(board = %model, serial = %board_id, "Board shutdown complete");
                    }
                    Err(e) => {
                        error!(
                            board = %model,
                            serial = %board_id,
                            error = %e,
                            "Failed to shutdown board"
                        );
                    }
                }
            }
        }
    }

    /// Handle native Amlogic control-board virtual transport events.
    async fn handle_amlogic_event(&mut self, event: AmlogicTransportEvent) -> Result<()> {
        match event {
            AmlogicTransportEvent::DeviceConnected(device_info) => {
                // Both S19j Pro and S19k Pro are served by the single
                // unified `s19x_amlogic` driver; the chip-family
                // specifics are resolved per hashboard model inside it.
                let descriptor_key = match device_info.model {
                    crate::config::HashboardModel::S19jPro
                    | crate::config::HashboardModel::S19kPro => "s19x_amlogic",
                };
                let Some(descriptor) = self.virtual_registry.find(descriptor_key) else {
                    error!(
                        descriptor = descriptor_key,
                        "No virtual board descriptor found for Amlogic model"
                    );
                    return Ok(());
                };

                info!(
                    board = descriptor.name,
                    id = %device_info.device_id,
                    "Native Amlogic control board enabled."
                );

                let (mut board, registration) = match (descriptor.create_fn)().await {
                    Ok(result) => result,
                    Err(e) => {
                        error!(
                            board = descriptor.name,
                            id = %device_info.device_id,
                            error = %e,
                            "Failed to create native Amlogic board"
                        );
                        return Ok(());
                    }
                };

                let board_info = board.board_info();
                let board_id = device_info.device_id.clone();

                if let Err(e) = self.board_reg_tx.send(registration).await {
                    error!(
                        board = %board_info.model,
                        error = %e,
                        "Failed to register board with API server"
                    );
                }

                board.set_miner_state_rx(self.miner_state_rx.clone());

                match board.create_hash_threads().await {
                    Ok(threads) => {
                        let thread_count = threads.len();

                        // If the board came up not-yet-minable (no PSU on the
                        // i2c bus), start the scheduler PAUSED *before* its
                        // threads register — `handle_new_thread` leaves threads
                        // added while paused idle, so no work is dispatched into
                        // unpowered chains. Await the ack so the pause is in
                        // effect before the threads arrive (select! across the
                        // scheduler's channels is not ordered, so send-order
                        // alone wouldn't guarantee it). A later resume energizes
                        // the rail and cold-inits them — no process restart.
                        if board.needs_initial_pause() {
                            let (reply_tx, reply_rx) = oneshot::channel();
                            if self
                                .scheduler_cmd_tx
                                .send(SchedulerCommand::PauseMining { reply: reply_tx })
                                .await
                                .is_ok()
                            {
                                let _ = reply_rx.await;
                                info!(
                                    board = %board_info.model,
                                    id = %board_id,
                                    "Board came up without a PSU — scheduler started PAUSED; \
                                     power the rail and resume to mine (no restart needed)."
                                );
                            } else {
                                error!(
                                    board = %board_info.model,
                                    "Failed to pause scheduler for a no-PSU board; chains may \
                                     be driven while unpowered"
                                );
                            }
                        }

                        self.boards.insert(board_id.clone(), board);

                        for thread in threads {
                            if let Err(e) = self.scheduler_tx.send(thread).await {
                                error!(
                                    board = %board_info.model,
                                    error = %e,
                                    "Failed to send thread to scheduler"
                                );
                                break;
                            }
                        }

                        info!(
                            board = %board_info.model,
                            id = %board_id,
                            threads = thread_count,
                            "Native Amlogic board started."
                        );
                    }
                    Err(e) => {
                        error!(
                            board = %board_info.model,
                            id = %board_id,
                            error = %e,
                            "Native Amlogic board failed to start."
                        );
                    }
                }
            }
            AmlogicTransportEvent::DeviceDisconnected { device_id } => {
                if let Some(mut board) = self.boards.remove(&device_id) {
                    let model = board.board_info().model;
                    debug!(board = %model, id = %device_id, "Shutting down native Amlogic board");

                    match board.shutdown().await {
                        Ok(()) => {
                            info!(board = %model, id = %device_id, "Native Amlogic board disconnected");
                        }
                        Err(e) => {
                            error!(
                                board = %model,
                                id = %device_id,
                                error = %e,
                                "Failed to shutdown native Amlogic board"
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Handle USB transport events.
    async fn handle_usb_event(&mut self, event: UsbTransportEvent) -> Result<()> {
        match event {
            UsbTransportEvent::UsbDeviceConnected(device_info) => {
                // Check if this device matches any registered board pattern
                let Some(descriptor) = self.registry.find_descriptor(&device_info) else {
                    // No match - this is expected for most USB devices
                    return Ok(());
                };

                // Pattern matched - log the match
                info!(
                    board = descriptor.name,
                    vid = %format!("{:04x}", device_info.vid),
                    pid = %format!("{:04x}", device_info.pid),
                    manufacturer = ?device_info.manufacturer,
                    product = ?device_info.product,
                    serial = ?device_info.serial_number,
                    "Hash board connected via USB."
                );

                // Create the board using the descriptor's factory function
                let (mut board, registration) = match (descriptor.create_fn)(device_info).await {
                    Ok(result) => result,
                    Err(e) => {
                        error!(
                            board = descriptor.name,
                            error = %e,
                            "Failed to create board"
                        );
                        return Ok(());
                    }
                };

                let board_info = board.board_info();
                let board_id = board_info
                    .serial_number
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());

                // Forward board registration to the API server
                if let Err(e) = self.board_reg_tx.send(registration).await {
                    error!(
                        board = %board_info.model,
                        error = %e,
                        "Failed to register board with API server"
                    );
                }

                // Provide miner state receiver for boards that display aggregate metrics
                board.set_miner_state_rx(self.miner_state_rx.clone());

                // Create hash threads from the board
                match board.create_hash_threads().await {
                    Ok(threads) => {
                        // Store board for lifecycle management
                        self.boards.insert(board_id.clone(), board);

                        // Send threads to scheduler individually
                        for thread in threads {
                            if let Err(e) = self.scheduler_tx.send(thread).await {
                                tracing::error!(
                                    board = %board_info.model,
                                    error = %e,
                                    "Failed to send thread to scheduler"
                                );
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            board = %board_info.model,
                            serial = %board_id,
                            error = %e,
                            "Hash board failed to start."
                        );
                    }
                }
            }
            UsbTransportEvent::UsbDeviceDisconnected { device_path: _ } => {
                // Find and shutdown the board
                // Note: Current design uses serial number as key, but we get device_path
                // in disconnect event. For single-board setups this works fine.
                // TODO: Maintain device_path -> board_id mapping for multi-board support
                let board_ids: Vec<String> = self.boards.keys().cloned().collect();
                for board_id in board_ids {
                    if let Some(mut board) = self.boards.remove(&board_id) {
                        let model = board.board_info().model;
                        debug!(board = %model, serial = %board_id, "Shutting down board");

                        match board.shutdown().await {
                            Ok(()) => {
                                info!(
                                    board = %model,
                                    serial = %board_id,
                                    "Board disconnected"
                                );
                            }
                            Err(e) => {
                                tracing::error!(
                                    board = %model,
                                    serial = %board_id,
                                    error = %e,
                                    "Failed to shutdown board"
                                );
                            }
                        }
                        // Don't re-insert - board is removed
                        break; // For now, assume one board per device
                    }
                }
            }
        }

        Ok(())
    }

    /// Handle CPU miner transport events.
    async fn handle_cpu_event(&mut self, event: CpuTransportEvent) -> Result<()> {
        match event {
            CpuTransportEvent::CpuDeviceConnected(device_info) => {
                // Find the virtual board descriptor for cpu_miner
                let Some(descriptor) = self.virtual_registry.find("cpu_miner") else {
                    error!("No virtual board descriptor found for cpu_miner");
                    return Ok(());
                };

                info!(
                    board = descriptor.name,
                    threads = device_info.thread_count,
                    duty = device_info.duty_percent,
                    "CPU miner board connected."
                );

                // Create the board using the descriptor's factory function
                let (mut board, registration) = match (descriptor.create_fn)().await {
                    Ok(result) => result,
                    Err(e) => {
                        error!(
                            board = descriptor.name,
                            error = %e,
                            "Failed to create CPU miner board"
                        );
                        return Ok(());
                    }
                };

                let board_info = board.board_info();
                let board_id = device_info.device_id.clone();

                // Forward board registration to the API server
                if let Err(e) = self.board_reg_tx.send(registration).await {
                    error!(
                        board = %board_info.model,
                        error = %e,
                        "Failed to register board with API server"
                    );
                }

                // Create hash threads from the board
                match board.create_hash_threads().await {
                    Ok(threads) => {
                        let thread_count = threads.len();

                        // Store board for lifecycle management
                        self.boards.insert(board_id.clone(), board);

                        // Send threads to scheduler individually
                        for thread in threads {
                            if let Err(e) = self.scheduler_tx.send(thread).await {
                                tracing::error!(
                                    board = %board_info.model,
                                    error = %e,
                                    "Failed to send thread to scheduler"
                                );
                                break;
                            }
                        }

                        info!(
                            board = %board_info.model,
                            threads = thread_count,
                            "CPU miner started."
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            board = %board_info.model,
                            error = %e,
                            "CPU miner failed to start."
                        );
                    }
                }
            }
            CpuTransportEvent::CpuDeviceDisconnected { device_id } => {
                if let Some(mut board) = self.boards.remove(&device_id) {
                    let model = board.board_info().model;
                    debug!(board = %model, id = %device_id, "Shutting down CPU miner");

                    match board.shutdown().await {
                        Ok(()) => {
                            info!(board = %model, id = %device_id, "CPU miner disconnected");
                        }
                        Err(e) => {
                            tracing::error!(
                                board = %model,
                                id = %device_id,
                                error = %e,
                                "Failed to shutdown CPU miner"
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
