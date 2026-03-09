//! Native Antminer Amlogic control-board virtual transport.
//!
//! Provides synthesized transport events for the local Amlogic control board
//! when enabled through Mujina configuration.

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
}
