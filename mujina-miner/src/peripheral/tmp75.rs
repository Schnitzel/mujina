//! TMP75 temperature sensor driver for S19j Pro hashboards.
//!
//! Each S19j Pro hashboard has two TMP75 sensors with different I2C addresses
//! depending on the hashboard number:
//!
//! | Hashboard | Sensor 0 | Sensor 1 |
//! |-----------|----------|----------|
//! | HB0       | 0x4C     | 0x48     |
//! | HB1       | 0x4D     | 0x49     |
//! | HB2       | 0x4E     | 0x4A     |

use crate::mgmt_protocol::bitcrane::i2c::BitcraneI2c;
use crate::hw_trait::HwError;
use crate::tracing::prelude::*;

type Result<T> = std::result::Result<T, HwError>;

/// TMP75 register addresses.
const TMP75_TEMP_REG: u8 = 0x00;

/// I2C addresses for TMP75 sensors on each hashboard.
const TMP75_ADDRESSES: [[u8; 2]; 3] = [
    [0x4C, 0x48], // HB0: Sensor 0, Sensor 1
    [0x4D, 0x49], // HB1: Sensor 0, Sensor 1
    [0x4E, 0x4A], // HB2: Sensor 0, Sensor 1
];

/// TMP75 temperature sensor driver.
#[derive(Clone)]
pub struct Tmp75 {
    i2c: BitcraneI2c,
    /// 7-bit I2C address of this sensor.
    address: u8,
    /// Human-readable name for logging.
    name: String,
}

impl Tmp75 {
    /// Create a TMP75 driver for a specific hashboard and sensor.
    ///
    /// # Arguments
    /// * `i2c` - I2C interface over bitcrane control channel
    /// * `hashboard` - Hashboard number (0, 1, or 2)
    /// * `sensor` - Sensor number (0 or 1)
    ///
    /// # Panics
    /// Panics if hashboard > 2 or sensor > 1.
    pub fn new(i2c: BitcraneI2c, hashboard: u8, sensor: u8) -> Self {
        assert!(hashboard <= 2, "hashboard must be 0, 1, or 2");
        assert!(sensor <= 1, "sensor must be 0 or 1");

        let address = TMP75_ADDRESSES[hashboard as usize][sensor as usize];
        let name = format!("HB{}-Temp{}", hashboard, sensor);

        Self { i2c, address, name }
    }

    /// Read the temperature in degrees Celsius.
    ///
    /// The TMP75 returns a 12-bit signed value in big-endian format,
    /// with 0.0625°C resolution.
    pub async fn read_temperature(&self) -> Result<f32> {
        let data = self.i2c.read_register(self.address, TMP75_TEMP_REG, 2).await?;

        if data.len() < 2 {
            return Err(HwError::Other(format!(
                "{}: TMP75 returned {} bytes, expected 2",
                self.name, data.len()
            )));
        }

        // Parse 12-bit signed temperature (big-endian, upper 12 bits)
        // Byte 0: MSB (8 bits)
        // Byte 1: LSB (4 bits in upper nibble)
        let raw = i16::from_be_bytes([data[0], data[1]]) >> 4;
        let temp_c = raw as f32 * 0.0625;

        trace!(
            sensor = %self.name,
            address = format!("0x{:02X}", self.address),
            raw_bytes = format!("{:02X} {:02X}", data[0], data[1]),
            temp_c,
            "TMP75 temperature read"
        );

        Ok(temp_c)
    }

    /// Get the sensor name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Create TMP75 sensor pair for a specific hashboard.
///
/// Returns two sensors: (Temp0, Temp1).
pub fn sensors_for_hashboard(i2c: BitcraneI2c, hashboard: u8) -> (Tmp75, Tmp75) {
    let temp0 = Tmp75::new(i2c.clone(), hashboard, 0);
    let temp1 = Tmp75::new(i2c, hashboard, 1);
    (temp0, temp1)
}
