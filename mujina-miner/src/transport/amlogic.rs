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
    /// Hashboard model selected at config time, used by the backplane
    /// to dispatch to the right board factory (`s19j_pro_amlogic` vs
    /// `s19k_pro_amlogic`). Both models share the AmlogicControlBoard
    /// schema; they differ only in chip family + topology.
    pub model: HashboardModel,
}
