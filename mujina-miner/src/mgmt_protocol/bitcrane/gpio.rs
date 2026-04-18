//! GPIO implementation using bitcrane control protocol.
//!
//! The bitcrane protocol uses logical GPIO command codes rather than physical
//! pin numbers:
//!
//! - RST0: 0x00 - Reset for hashboard 0
//! - PLUG0: 0x01 - Plug detect for hashboard 0 (read-only)
//! - RST1: 0x10 - Reset for hashboard 1
//! - PLUG1: 0x11 - Plug detect for hashboard 1 (read-only)
//! - RST2: 0x20 - Reset for hashboard 2
//! - PLUG2: 0x21 - Plug detect for hashboard 2 (read-only)
//! - PSU_EN: 0x50 - PSU enable

use async_trait::async_trait;
use tracing::debug;

use crate::hw_trait::gpio::{Gpio, GpioPin, PinMode, PinValue};
use crate::hw_trait::{HwError, Result};
use crate::mgmt_protocol::ControlChannel;
use crate::mgmt_protocol::bitaxe_raw::{Packet, Page};

/// Bitcrane GPIO pin identifiers.
///
/// These map to the command bytes in the bitcrane GPIO protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BitcraneGpioPin {
    /// Reset for hashboard 0 (active-low)
    Rst0 = 0x00,
    /// Plug detect for hashboard 0 (read-only)
    Plug0 = 0x01,
    /// Reset for hashboard 1 (active-low)
    Rst1 = 0x10,
    /// Plug detect for hashboard 1 (read-only)
    Plug1 = 0x11,
    /// Reset for hashboard 2 (active-low)
    Rst2 = 0x20,
    /// Plug detect for hashboard 2 (read-only)
    Plug2 = 0x21,
    /// PSU enable
    PsuEn = 0x50,
}

/// GPIO controller using bitcrane control protocol.
#[derive(Clone)]
pub struct BitcraneGpioController {
    channel: ControlChannel,
}

impl BitcraneGpioController {
    /// Create a new GPIO controller using the given control channel.
    pub fn new(channel: ControlChannel) -> Self {
        Self { channel }
    }

    /// Get a handle to a specific bitcrane GPIO pin.
    pub fn pin(&self, pin: BitcraneGpioPin) -> BitcraneGpioPinHandle {
        BitcraneGpioPinHandle {
            channel: self.channel.clone(),
            pin,
        }
    }
}

#[async_trait]
impl Gpio for BitcraneGpioController {
    type Pin = BitcraneGpioPinHandle;

    async fn pin(&mut self, number: u8) -> Result<Self::Pin> {
        // Map numeric pin to bitcrane GPIO pin
        let pin = match number {
            0x00 => BitcraneGpioPin::Rst0,
            0x01 => BitcraneGpioPin::Plug0,
            0x10 => BitcraneGpioPin::Rst1,
            0x11 => BitcraneGpioPin::Plug1,
            0x20 => BitcraneGpioPin::Rst2,
            0x21 => BitcraneGpioPin::Plug2,
            0x50 => BitcraneGpioPin::PsuEn,
            _ => {
                return Err(HwError::InvalidParameter(format!(
                    "Invalid bitcrane GPIO pin: 0x{:02X}",
                    number
                )));
            }
        };
        Ok(BitcraneGpioPinHandle {
            channel: self.channel.clone(),
            pin,
        })
    }
}

/// GPIO pin handle using bitcrane control protocol.
#[derive(Clone)]
pub struct BitcraneGpioPinHandle {
    channel: ControlChannel,
    pin: BitcraneGpioPin,
}

#[async_trait]
impl GpioPin for BitcraneGpioPinHandle {
    async fn set_mode(&mut self, _mode: PinMode) -> Result<()> {
        // Bitcrane pins are fixed-function, mode setting is a no-op
        Ok(())
    }

    async fn write(&mut self, value: PinValue) -> Result<()> {
        debug!(pin = ?self.pin, value = ?value, "Bitcrane GPIO write");
        let data = vec![if value == PinValue::High { 0x01 } else { 0x00 }];
        let packet = Packet::new(0, Page::GPIO, self.pin as u8, data);
        self.channel.send_packet(packet).await?;
        Ok(())
    }

    async fn read(&mut self) -> Result<PinValue> {
        let packet = Packet::new(0, Page::GPIO, self.pin as u8, vec![]);
        let response = self.channel.send_packet(packet).await?;

        if response.data.len() != 1 {
            return Err(HwError::InvalidParameter(format!(
                "Expected 1 byte in GPIO read response, got {}",
                response.data.len()
            )));
        }

        let value = if response.data[0] != 0 {
            PinValue::High
        } else {
            PinValue::Low
        };
        debug!(pin = ?self.pin, value = ?value, "Bitcrane GPIO read");
        Ok(value)
    }
}
