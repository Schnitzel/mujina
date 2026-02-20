//! APW12 PSU support via bitcrane protocol.
//!
//! The APW12 PSU is controlled over bit-banged I2C (PAGE_PSU = 0x04) with the
//! PSU_EN GPIO pin (0x50) controlling power enable.
//!
//! Protocol details:
//! - I2C address: 0x10
//! - Register: 0x11
//! - Command format: 0x55 0xAA <len> <cmd bytes...> <checksum_lo> <checksum_hi>
//! - Voltage formula: hex_voltage = (voltage - 15.092) / -0.013

use async_trait::async_trait;
use tracing::{debug, warn};

use crate::hw_trait::gpio::{GpioPin, PinValue};
use crate::hw_trait::{HwError, Result};
use crate::mgmt_protocol::bitaxe_raw::{Page, Packet};
use crate::mgmt_protocol::bitcrane::gpio::{BitcraneGpioController, BitcraneGpioPin};
use crate::mgmt_protocol::ControlChannel;

/// APW12 PSU I2C address
const PSU_I2C_ADDR: u8 = 0x10;
/// APW12 PSU register for commands
const PSU_REGISTER: u8 = 0x11;

/// APW12 PSU commands
#[repr(u8)]
enum PsuCommand {
    GetFwVersion = 0x01,
    GetHwVersion = 0x02,
    GetVoltage = 0x03,
    MeasureVoltage = 0x04,
    DisableWdt = 0x81,
    SetVoltage = 0x83,
}

/// I2C commands for PSU bus (bit-banged I2C on PAGE_PSU)
const I2C_COMMAND_WRITE: u8 = 0x20;
const I2C_COMMAND_READ: u8 = 0x30;

/// APW12 PSU controller via bitcrane protocol.
pub struct Apw12Psu {
    channel: ControlChannel,
    gpio: BitcraneGpioController,
    enabled: bool,
}

impl Apw12Psu {
    /// Create a new APW12 PSU controller.
    pub fn new(channel: ControlChannel) -> Self {
        let gpio = BitcraneGpioController::new(channel.clone());
        Self {
            channel,
            gpio,
            enabled: false,
        }
    }

    /// Enable or disable the PSU via PSU_EN GPIO.
    ///
    /// Note: PSU_EN is active-low (LOW = enabled, HIGH = disabled).
    pub async fn set_enabled(&mut self, enable: bool) -> Result<()> {
        let mut psu_en = self.gpio.pin(BitcraneGpioPin::PsuEn);
        // PSU_EN is active-low
        let value = if enable {
            PinValue::Low
        } else {
            PinValue::High
        };
        psu_en.write(value).await?;
        self.enabled = enable;
        debug!(enabled = enable, "APW12 PSU enable set");
        Ok(())
    }

    /// Check if PSU is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Configure the watchdog timer.
    ///
    /// Set value to 0x00 to disable watchdog, 0x01 to enable.
    pub async fn config_watchdog(&mut self, value: u8) -> Result<()> {
        let cmd_bytes = vec![PsuCommand::DisableWdt as u8, value, 0x00];
        let packet = Self::make_psu_packet(&cmd_bytes);
        debug!(watchdog_value = value, "Configuring APW12 watchdog");
        self.psu_send_bytes(&packet).await?;

        // Read response
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let _response = self.psu_read_bytes(8).await?;
        Ok(())
    }

    /// Set the PSU output voltage.
    ///
    /// Voltage range is approximately 12V to 15V based on the formula.
    pub async fn set_voltage(&mut self, voltage: f32) -> Result<()> {
        // Voltage formula: hex_voltage = (voltage - 15.092) / -0.013
        let hex_voltage = ((voltage - 15.092) / -0.013) as u8;

        debug!(
            voltage = voltage,
            hex_voltage = hex_voltage,
            "Setting APW12 voltage"
        );

        let cmd_bytes = vec![PsuCommand::SetVoltage as u8, hex_voltage, 0x00];
        let packet = Self::make_psu_packet(&cmd_bytes);
        self.psu_send_bytes(&packet).await?;

        // Read response
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let _response = self.psu_read_bytes(8).await?;
        Ok(())
    }

    /// Get the configured voltage setting.
    pub async fn get_voltage(&mut self) -> Result<f32> {
        let cmd_bytes = vec![PsuCommand::GetVoltage as u8];
        let packet = Self::make_psu_packet(&cmd_bytes);
        self.psu_send_bytes(&packet).await?;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let response = self.psu_read_bytes(8).await?;

        if response.len() >= 5 {
            // Voltage = 15.092 - (response[4] * 0.013)
            let voltage = 15.092 - (response[4] as f32 * 0.013);
            debug!(raw = response[4], voltage = voltage, "APW12 voltage read");
            Ok(voltage)
        } else {
            Err(HwError::InvalidParameter(
                "Invalid voltage response length".to_string(),
            ))
        }
    }

    /// Measure the actual output voltage.
    pub async fn measure_voltage(&mut self) -> Result<f32> {
        let cmd_bytes = vec![PsuCommand::MeasureVoltage as u8];
        let packet = Self::make_psu_packet(&cmd_bytes);
        self.psu_send_bytes(&packet).await?;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let response = self.psu_read_bytes(8).await?;

        if response.len() >= 6 {
            // measured_voltage = (response[5] << 8 | response[4])
            // actual = (measured_voltage + 0.8615) / 63.017
            let raw = (response[5] as u16) << 8 | response[4] as u16;
            let voltage = (raw as f32 + 0.8615) / 63.017;
            debug!(raw = raw, voltage = voltage, "APW12 measured voltage");
            Ok(voltage)
        } else {
            Err(HwError::InvalidParameter(
                "Invalid measured voltage response length".to_string(),
            ))
        }
    }

    /// Get PSU firmware version.
    pub async fn get_fw_version(&mut self) -> Result<Vec<u8>> {
        let cmd_bytes = vec![PsuCommand::GetFwVersion as u8];
        let packet = Self::make_psu_packet(&cmd_bytes);
        self.psu_send_bytes(&packet).await?;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        self.psu_read_bytes(8).await
    }

    /// Get PSU hardware version.
    pub async fn get_hw_version(&mut self) -> Result<Vec<u8>> {
        let cmd_bytes = vec![PsuCommand::GetHwVersion as u8];
        let packet = Self::make_psu_packet(&cmd_bytes);
        self.psu_send_bytes(&packet).await?;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        self.psu_read_bytes(8).await
    }

    /// Create a PSU command packet with 0x55 0xAA header and checksum.
    fn make_psu_packet(cmd_bytes: &[u8]) -> Vec<u8> {
        // Length includes: length byte + cmd_bytes + 2 checksum bytes
        let len = cmd_bytes.len() + 3;

        // Build the checksummed portion
        let mut data = vec![len as u8];
        data.extend_from_slice(cmd_bytes);

        // Calculate checksum (sum of all bytes as u16, little-endian)
        let checksum: u16 = data.iter().map(|&b| b as u16).sum();
        data.push((checksum & 0xFF) as u8); // low byte
        data.push(((checksum >> 8) & 0xFF) as u8); // high byte

        // Add header
        let mut packet = vec![0x55, 0xAA];
        packet.extend(data);
        packet
    }

    /// Send bytes to PSU over I2C (one byte at a time via PAGE_PSU).
    async fn psu_send_bytes(&mut self, data: &[u8]) -> Result<()> {
        debug!(data = ?data, "Sending bytes to APW12 PSU");
        for &byte in data {
            self.i2c_send_byte(PSU_I2C_ADDR, PSU_REGISTER, byte).await?;
        }
        Ok(())
    }

    /// Read bytes from PSU over I2C (one byte at a time via PAGE_PSU).
    async fn psu_read_bytes(&mut self, count: usize) -> Result<Vec<u8>> {
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            let byte = self.i2c_read_byte(PSU_I2C_ADDR).await?;
            result.push(byte);
        }
        debug!(data = ?result, "Read bytes from APW12 PSU");
        Ok(result)
    }

    /// Send a single byte over I2C via PAGE_PSU.
    async fn i2c_send_byte(&mut self, address: u8, register: u8, data: u8) -> Result<()> {
        // Packet: LEN_LO LEN_HI ID BUS PAGE CMD ADDR REG DATA
        // PAGE_PSU = 0x04, I2C_COMMAND_WRITE = 0x20
        let packet = Packet::new(
            0xBC, // ID
            Page::PSU,
            I2C_COMMAND_WRITE,
            vec![address, register, data],
        );
        let response = self.channel.send_packet(packet).await?;

        // Check for at least 1 byte response
        if response.data.is_empty() {
            warn!("Empty response from I2C write");
        }
        Ok(())
    }

    /// Read a single byte over I2C via PAGE_PSU.
    async fn i2c_read_byte(&mut self, address: u8) -> Result<u8> {
        // Packet: LEN_LO LEN_HI ID BUS PAGE CMD ADDR NUM_BYTES
        let packet = Packet::new(
            0xAB, // ID
            Page::PSU,
            I2C_COMMAND_READ,
            vec![address, 1], // Read 1 byte
        );
        let response = self.channel.send_packet(packet).await?;

        if response.data.is_empty() {
            return Err(HwError::Other("No data received from I2C read".to_string()));
        }
        Ok(response.data[response.data.len() - 1])
    }
}

/// APW12 voltage limits
const APW12_MIN_VOLTAGE: f32 = 12.0;
const APW12_MAX_VOLTAGE: f32 = 15.0;
const APW12_TARGET_VOLTAGE: f32 = 12.6;
const APW12_VOLTAGE_STEP: f32 = 0.1;

/// Trait for voltage regulation, implemented by APW12 for the hash thread.
#[async_trait]
impl crate::asic::bm13xx::chain_config::VoltageRegulator for Apw12Psu {
    async fn set_voltage(&mut self, volts: f32) -> anyhow::Result<()> {
        // Clamp to valid APW12 range
        let clamped = volts.clamp(APW12_MIN_VOLTAGE, APW12_MAX_VOLTAGE);
        if (clamped - volts).abs() > 0.01 {
            debug!(
                requested = volts,
                clamped = clamped,
                "APW12 voltage clamped to valid range"
            );
        }
        Apw12Psu::set_voltage(self, clamped)
            .await
            .map_err(|e| anyhow::anyhow!("APW12 set voltage failed: {}", e))
    }

    fn voltage_range(&self) -> (f32, f32) {
        (APW12_MIN_VOLTAGE, APW12_MAX_VOLTAGE)
    }

    fn target_voltage(&self) -> f32 {
        APW12_TARGET_VOLTAGE
    }

    fn voltage_step(&self) -> f32 {
        APW12_VOLTAGE_STEP
    }
}
