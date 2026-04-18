//! Fan control for bitcrane protocol.
//!
//! The bitcrane supports up to 4 PWM-controlled fans with tachometer feedback.
//! Fan control uses PAGE_FAN (0x09) with speed and tach commands.

use crate::hw_trait::HwError;
use crate::mgmt_protocol::{
    ControlChannel,
    bitaxe_raw::{Packet, Page},
};
use crate::tracing::prelude::*;

type Result<T> = std::result::Result<T, HwError>;

/// Fan speed command offsets (fan_num + 0x10)
const FAN_SPEED_CMD_BASE: u8 = 0x10;
/// Fan tachometer command offsets (fan_num + 0x20)
const FAN_TACH_CMD_BASE: u8 = 0x20;

/// Bitcrane fan controller.
///
/// Controls PWM duty cycle and reads tachometer RPM for up to 4 fans.
#[derive(Clone)]
pub struct BitcraneFan {
    channel: ControlChannel,
    /// Fan number (1-4).
    fan_num: u8,
    /// Human-readable name.
    name: String,
}

impl BitcraneFan {
    /// Create a fan controller for the specified fan.
    ///
    /// # Arguments
    /// * `channel` - Control channel for bitcrane communication
    /// * `fan_num` - Fan number (1-4)
    ///
    /// # Panics
    /// Panics if fan_num is not 1-4.
    pub fn new(channel: ControlChannel, fan_num: u8) -> Self {
        assert!(fan_num >= 1 && fan_num <= 4, "fan_num must be 1-4");

        Self {
            channel,
            fan_num,
            name: format!("Fan{}", fan_num),
        }
    }

    /// Get the fan name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Set fan speed as a percentage (0-100).
    pub async fn set_speed(&self, percent: u8) -> Result<()> {
        let percent = percent.min(100);
        let cmd = FAN_SPEED_CMD_BASE + self.fan_num;

        let packet = Packet::new(
            0x00, // ID auto-assigned by channel
            Page::Fan,
            cmd,
            vec![percent],
        );

        self.channel.send_packet(packet).await?;

        debug!(
            fan = %self.name,
            percent,
            "Fan speed set"
        );

        Ok(())
    }

    /// Read fan RPM from tachometer.
    ///
    /// Returns the measured RPM, or an error if the read fails.
    pub async fn read_rpm(&self) -> Result<u32> {
        let cmd = FAN_TACH_CMD_BASE + self.fan_num;

        let packet = Packet::new(
            0x00, // ID auto-assigned by channel
            Page::Fan,
            cmd,
            vec![],
        );

        let response = self.channel.send_packet(packet).await?;

        // Response data contains RPM as little-endian u16
        if response.data.len() < 2 {
            return Err(HwError::Other(format!(
                "{}: Tach response too short ({} bytes)",
                self.name,
                response.data.len()
            )));
        }

        // RPM is in the last 2 bytes as little-endian
        let data_len = response.data.len();
        let rpm =
            u16::from_le_bytes([response.data[data_len - 2], response.data[data_len - 1]]) as u32;

        trace!(
            fan = %self.name,
            rpm,
            "Fan RPM read"
        );

        Ok(rpm)
    }
}

/// Create all 4 fans for a bitcrane controller.
pub fn all_fans(channel: ControlChannel) -> [BitcraneFan; 4] {
    [
        BitcraneFan::new(channel.clone(), 1),
        BitcraneFan::new(channel.clone(), 2),
        BitcraneFan::new(channel.clone(), 3),
        BitcraneFan::new(channel, 4),
    ]
}
