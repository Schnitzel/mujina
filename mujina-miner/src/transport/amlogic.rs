//! Native Antminer Amlogic control-board virtual transport.
//!
//! Provides synthesized transport events for the local Amlogic control board
//! when enabled through Mujina configuration.

use crate::config::HashboardModel;

/// Transport events for native Amlogic control-board devices.
#[derive(Debug)]
pub enum TransportEvent {
    /// A configured Amlogic control board should be started.
    DeviceConnected(AmlogicDeviceInfo),

    /// A configured Amlogic control board should be stopped.
    DeviceDisconnected { device_id: String },
}

/// Information about a configured native Amlogic control board.
#[derive(Debug, Clone)]
pub struct AmlogicDeviceInfo {
    /// Unique identifier for this configured device.
    pub device_id: String,
    /// Hashboard model selected at config time. Both S19j Pro and S19k
    /// Pro are handled by the single unified `s19x_amlogic` board
    /// factory; the model is carried here so the board can resolve the
    /// per-model chip-family spec (chip family + topology + voltage).
    pub model: HashboardModel,
}
