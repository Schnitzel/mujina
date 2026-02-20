//! Generic I2C operations for bitcrane protocol.
//!
//! Provides I2C read/write operations over the bitcrane control channel using
//! PAGE_I2C (0x05). Used by TMP75 temperature sensors on S19j Pro hashboards.

use crate::mgmt_protocol::{ControlChannel, bitaxe_raw::{I2CCommand, Page, Packet}};
use crate::hw_trait::HwError;

type Result<T> = std::result::Result<T, HwError>;

/// Generic I2C interface over bitcrane control channel.
#[derive(Clone)]
pub struct BitcraneI2c {
    channel: ControlChannel,
}

impl BitcraneI2c {
    /// Create a new I2C interface.
    pub fn new(channel: ControlChannel) -> Self {
        Self { channel }
    }

    /// Write bytes to an I2C device register.
    ///
    /// # Arguments
    /// * `address` - 7-bit I2C device address
    /// * `register` - Register address to write to
    /// * `data` - Data bytes to write
    pub async fn write(&self, address: u8, register: u8, data: &[u8]) -> Result<()> {
        let mut payload = vec![address, register];
        payload.extend_from_slice(data);

        let packet = Packet::new(
            0xBC, // ID
            Page::I2C,
            I2CCommand::Write as u8,
            payload,
        );

        self.channel.send_packet(packet).await?;
        Ok(())
    }

    /// Read bytes from an I2C device register.
    ///
    /// Performs a write-then-read transaction: writes the register address,
    /// then reads the specified number of bytes.
    ///
    /// # Arguments
    /// * `address` - 7-bit I2C device address
    /// * `register` - Register address to read from
    /// * `count` - Number of bytes to read
    pub async fn read_register(&self, address: u8, register: u8, count: u8) -> Result<Vec<u8>> {
        // Packet format: [address, register, count]
        // Note: ID is auto-assigned by ControlChannel::send_packet
        let packet = Packet::new(
            0x00, // Placeholder, channel assigns actual ID
            Page::I2C,
            I2CCommand::WriteRead as u8,
            vec![address, register, count],
        );

        let response = self.channel.send_packet(packet).await?;

        // Extract the data bytes (last `count` bytes of response.data)
        let data_len = response.data.len();
        if data_len < count as usize {
            return Err(HwError::Other(format!(
                "I2C read returned {} bytes, expected {}",
                data_len, count
            )));
        }

        Ok(response.data[data_len - count as usize..].to_vec())
    }
}
