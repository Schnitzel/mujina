//! EmberOne mining board support.
//!
//! The EmberOne is a mining board with 12 BM1362 ASIC chips, communicating via
//! USB using the bitaxe-raw protocol (same as Bitaxe boards).

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{Mutex, watch};
use tokio_serial::SerialPortBuilderExt;
use tokio_util::codec::{FramedRead, FramedWrite};

use super::{
    Board, BoardDescriptor, BoardError, BoardInfo,
    pattern::{BoardPattern, Match, StringMatch},
};
use crate::{
    api_client::types::BoardState,
    asic::{
        bm13xx::{
            self,
            chain_config::{ChainConfig, ChainPeripherals, VoltageRegulator},
            chip_config, thread_v2,
            topology::TopologySpec,
        },
        hash_thread::{AsicEnable, HashThread},
    },
    error::Error,
    hw_trait::{
        gpio::{Gpio, GpioPin, PinValue},
        i2c::I2c,
    },
    mgmt_protocol::{
        ControlChannel,
        bitaxe_raw::{
            gpio::{BitaxeRawGpioController, BitaxeRawGpioPin},
            i2c::BitaxeRawI2c,
        },
    },
    peripheral::tps546::{self, Tps546, Tps546Config},
    tracing::prelude::*,
    transport::{
        UsbDeviceInfo,
        serial::{SerialControl, SerialStream},
    },
};

// Register this board type with the inventory system
inventory::submit! {
    BoardDescriptor {
        pattern: BoardPattern {
            vid: Match::Any,
            pid: Match::Any,
            // Skip hardware revision check to allow testing with old firmware
            // bcd_device: Match::Specific(BcdVersionMatch::Major(5)),
            bcd_device: Match::Any,
            manufacturer: Match::Specific(StringMatch::Exact("256F")),
            product: Match::Specific(StringMatch::Exact("EmberOne00")),
            serial_pattern: Match::Any,
        },
        name: "EmberOne",
        create_fn: |device| Box::pin(create_from_usb(device)),
    }
}

/// EmberOne mining board.
pub struct EmberOne {
    device_info: UsbDeviceInfo,
    data_port_path: String,
    /// Control channel for board management.
    control_channel: ControlChannel,
    /// I2C bus controller.
    i2c: BitaxeRawI2c,
    /// Control handle for data channel (for baud rate changes).
    data_control: Option<SerialControl>,
    /// ASIC reset pin (active low).
    reset_pin: Option<BitaxeRawGpioPin>,
    /// Power enable pin (active high).
    power_enable_pin: Option<BitaxeRawGpioPin>,
    /// IO power enable pin (active high).
    io_power_enable_pin: Option<BitaxeRawGpioPin>,
    /// Voltage regulator.
    regulator: Option<Arc<Mutex<Tps546<BitaxeRawI2c>>>>,

    /// Channel for publishing board state to the API server.
    #[expect(dead_code, reason = "will publish telemetry in a follow-up commit")]
    state_tx: watch::Sender<BoardState>,
}

impl EmberOne {
    /// GPIO pin number for ASIC reset control (active low).
    const ASIC_RESET_PIN: u8 = 0;
    /// GPIO pin number for ASIC power enable (active high).
    const POWER_ENABLE_PIN: u8 = 1;
    /// GPIO pin number for IO power enable (active high).
    const IO_POWER_ENABLE_PIN: u8 = 2;

    /// Create a new EmberOne board instance.
    pub fn new(
        device_info: UsbDeviceInfo,
        control_channel: ControlChannel,
        data_port_path: String,
        state_tx: watch::Sender<BoardState>,
    ) -> Self {
        let i2c = BitaxeRawI2c::new(control_channel.clone());
        Self {
            device_info,
            data_port_path,
            control_channel,
            i2c,
            data_control: None,
            reset_pin: None,
            power_enable_pin: None,
            io_power_enable_pin: None,
            regulator: None,
            state_tx,
        }
    }

    /// Initialize the board hardware.
    ///
    /// Sets up GPIO pins, holds ASICs in reset, configures I2C, and probes
    /// for the TPS546 voltage regulator.
    pub async fn initialize(&mut self) -> Result<(), BoardError> {
        // Get GPIO pins
        let mut gpio = BitaxeRawGpioController::new(self.control_channel.clone());

        let mut reset_pin = gpio.pin(Self::ASIC_RESET_PIN).await.map_err(|e| {
            BoardError::InitializationFailed(format!("Failed to get reset pin: {}", e))
        })?;
        let mut power_enable_pin = gpio.pin(Self::POWER_ENABLE_PIN).await.map_err(|e| {
            BoardError::InitializationFailed(format!("Failed to get power enable pin: {}", e))
        })?;
        let mut io_power_enable_pin = gpio.pin(Self::IO_POWER_ENABLE_PIN).await.map_err(|e| {
            BoardError::InitializationFailed(format!("Failed to get IO power enable pin: {}", e))
        })?;

        // Initialize to safe state: chips in reset, power enabled for TPS546 config
        debug!("Initializing EmberOne: chips in reset, power enabled");
        reset_pin.write(PinValue::Low).await.map_err(|e| {
            BoardError::InitializationFailed(format!("Failed to assert reset: {}", e))
        })?;
        // Enable power (TPS546 CONTROL pin) - regulator needs this to turn on
        // Chips stay in reset so they won't draw power until we're ready
        power_enable_pin.write(PinValue::High).await.map_err(|e| {
            BoardError::InitializationFailed(format!("Failed to enable power: {}", e))
        })?;
        // Enable IO power for serial communication
        io_power_enable_pin
            .write(PinValue::High)
            .await
            .map_err(|e| {
                BoardError::InitializationFailed(format!("Failed to enable IO power: {}", e))
            })?;

        // Store GPIO pins for later use
        self.reset_pin = Some(reset_pin);
        self.power_enable_pin = Some(power_enable_pin);
        self.io_power_enable_pin = Some(io_power_enable_pin);

        // Configure I2C bus
        self.i2c.set_frequency(100_000).await.map_err(|e| {
            BoardError::InitializationFailed(format!("Failed to set I2C frequency: {}", e))
        })?;

        // Initialize TPS546 voltage regulator
        self.init_power_controller().await?;

        Ok(())
    }

    /// Initialize the TPS546 voltage regulator with EmberOne configuration.
    ///
    /// Configuration values are based on the EmberOne Python reference implementation.
    async fn init_power_controller(&mut self) -> Result<(), BoardError> {
        debug!(
            "Initializing TPS546 voltage regulator at address 0x{:02X}",
            tps546::constants::DEFAULT_ADDRESS
        );

        // Clone I2C bus for the power controller
        let power_i2c = self.i2c.clone();

        // EmberOne TPS546 configuration
        // Values from emberone-miner Python config
        let config = Tps546Config {
            // Phase and frequency
            phase: 0xFF,               // All phases
            frequency_switch_khz: 325, // 325 kHz (vs Bitaxe's 650 kHz)

            // Input voltage thresholds (12V nominal input)
            vin_on: 11.0,
            vin_off: 10.5,
            vin_uv_warn_limit: 0.0, // Disabled due to TI bug
            vin_ov_fault_limit: 14.0,
            vin_ov_fault_response: 0xB7, // Immediate shutdown, 6 retries

            // Output voltage configuration for BM1362 chain
            vout_scale_loop: 0.125, // Different from Bitaxe's 0.25
            vout_min: 3.0,
            vout_max: 4.0,
            vout_command: 3.5,

            // Output voltage protection (relative to vout_command)
            vout_ov_fault_limit: 1.25, // 125% of VOUT_COMMAND
            vout_ov_warn_limit: 1.16,  // 116% of VOUT_COMMAND
            vout_margin_high: 1.10,    // 110% of VOUT_COMMAND
            vout_margin_low: 0.90,     // 90% of VOUT_COMMAND
            vout_uv_warn_limit: 0.90,  // 90% of VOUT_COMMAND
            vout_uv_fault_limit: 0.75, // 75% of VOUT_COMMAND

            // Output current protection (higher limits for 12-chip chain)
            iout_oc_warn_limit: 50.0,  // 50A warning (vs Bitaxe's 25A)
            iout_oc_fault_limit: 55.0, // 55A fault (vs Bitaxe's 30A)
            iout_oc_fault_response: 0xC0,

            // Temperature protection
            ot_warn_limit: 105,
            ot_fault_limit: 145,
            ot_fault_response: 0xFF,

            // Timing configuration
            ton_delay: 0,
            ton_rise: 3,
            ton_max_fault_limit: 0,
            ton_max_fault_response: 0x3B,
            toff_delay: 0,
            toff_fall: 0,

            // Pin configuration - use NVM defaults for EmberOne
            pin_detect_override: 0x0000,

            // EmberOne-specific registers (from Python config)
            vout_ov_fault_response: Some(0xB7),
            vout_uv_fault_response: Some(0xB7),
            compensation_config: Some([0x13, 0x20, 0xC6, 0x19, 0xC6]),
            power_stage_config: Some(0x70),
            telemetry_config: Some([0x03, 0x03, 0x03, 0x03, 0x03, 0x00]),
            iout_cal_gain: Some(0xC880),
            iout_cal_offset: Some(0xE000),
        };

        let mut tps546 = Tps546::new(power_i2c, config);

        // Initialize the TPS546
        match tps546.init().await {
            Ok(()) => {
                // Delay before setting voltage
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                // Initial voltage matches the V-f curve at the initial PLL
                // frequency (~56.25 MHz). See thread_v2::voltage_for_frequency_stacked().
                const DEFAULT_VOUT: f32 = 3.04;

                tps546.set_vout_target(DEFAULT_VOUT).await.map_err(|e| {
                    BoardError::InitializationFailed(format!("Failed to set core voltage: {}", e))
                })?;
                tps546.clear_faults().await.map_err(|e| {
                    BoardError::InitializationFailed(format!("Failed to clear faults: {}", e))
                })?;
                tps546.enable_output().await.map_err(|e| {
                    BoardError::InitializationFailed(format!("Failed to enable output: {}", e))
                })?;

                debug!("Core voltage set to {DEFAULT_VOUT}V");

                // Wait for voltage to stabilize
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

                // Verify voltage readback
                match tps546.get_vout().await {
                    Ok(mv) => {
                        let volts = mv as f32 / 1000.0;
                        info!("Core voltage readback: {:.3}V", volts);

                        if (volts - DEFAULT_VOUT).abs() > 0.2 {
                            warn!(
                                "Core voltage {:.3}V differs from target {:.1}V",
                                volts, DEFAULT_VOUT
                            );
                        }
                    }
                    Err(e) => warn!("Failed to read core voltage: {}", e),
                }

                // Dump configuration for debugging
                if let Err(e) = tps546.dump_configuration().await {
                    warn!("Failed to dump TPS546 configuration: {}", e);
                }

                self.regulator = Some(Arc::new(Mutex::new(tps546)));
                Ok(())
            }
            Err(e) => {
                let msg = format!(
                    "Failed to initialize TPS546 power controller: {}. \
                     Is input voltage connected?",
                    e
                );
                error!("{}", msg);
                Err(BoardError::InitializationFailed(msg))
            }
        }
    }
}

#[async_trait]
impl Board for EmberOne {
    fn board_info(&self) -> BoardInfo {
        BoardInfo {
            model: "EmberOne".to_string(),
            firmware_version: None,
            serial_number: self.device_info.serial_number.clone(),
        }
    }

    async fn shutdown(&mut self) -> Result<(), BoardError> {
        if let Some(ref regulator) = self.regulator {
            match regulator.lock().await.disable_output().await {
                Ok(()) => debug!("Core voltage turned off"),
                Err(e) => warn!("Failed to disable output: {}", e),
            }
        }

        // Dump TPS546 status for debugging
        if let Some(ref regulator) = self.regulator {
            let mut tps = regulator.lock().await;
            if let Err(e) = tps.dump_configuration().await {
                warn!("Failed to dump TPS546 status on shutdown: {}", e);
            }
        }

        info!("EmberOne shutdown complete");
        Ok(())
    }

    async fn create_hash_threads(&mut self) -> Result<Vec<Box<dyn HashThread>>, BoardError> {
        // Take GPIO pins from initialization
        let reset_pin = self.reset_pin.take().ok_or_else(|| {
            BoardError::InitializationFailed("Reset pin not initialized".to_string())
        })?;
        let power_enable_pin = self.power_enable_pin.take().ok_or_else(|| {
            BoardError::InitializationFailed("Power enable pin not initialized".to_string())
        })?;
        let io_power_enable_pin = self.io_power_enable_pin.take().ok_or_else(|| {
            BoardError::InitializationFailed("IO power enable pin not initialized".to_string())
        })?;

        // Open data port
        let data_stream = SerialStream::new(&self.data_port_path, 115200).map_err(|e| {
            BoardError::InitializationFailed(format!("Failed to open data port: {}", e))
        })?;
        let (data_reader, data_writer, data_control) = data_stream.split();
        self.data_control = Some(data_control);

        // Create framed reader/writer for BM13xx protocol
        let chip_rx = FramedRead::new(data_reader, bm13xx::FrameCodec);
        let chip_tx = FramedWrite::new(data_writer, bm13xx::FrameCodec);

        // Build thread name from board model and serial
        let thread_name = match &self.device_info.serial_number {
            Some(serial) => format!("EmberOne-{}", &serial[..8.min(serial.len())]),
            None => "EmberOne".to_string(),
        };

        // Wire voltage regulator for coordinated V-f ramping
        let voltage_regulator: Option<Arc<Mutex<dyn VoltageRegulator + Send>>> =
            self.regulator.as_ref().map(|reg| {
                Arc::new(Mutex::new(EmberOneVoltageRegulator {
                    tps546: Arc::clone(reg),
                })) as Arc<Mutex<dyn VoltageRegulator + Send>>
            });

        // Build chain configuration for EmberOne: 12 BM1362 chips, each in its own domain
        let config = ChainConfig {
            name: thread_name,
            topology: TopologySpec::individual_domains(12, false),
            chip_config: chip_config::bm1362(),
            peripherals: ChainPeripherals {
                asic_enable: Arc::new(Mutex::new(EmberOneAsicEnable {
                    reset_pin,
                    power_enable_pin,
                    io_power_enable_pin,
                })),
                voltage_regulator,
                chip_uart_baud: None,
                ramp_coordinator: None,
                thermal_cap_mhz: None,
            },
            post_broadcast_chip_baud: None,
        };

        // Create the hash thread
        let thread = thread_v2::BM13xxThread::new(chip_rx, chip_tx, config).map_err(|e| {
            BoardError::InitializationFailed(format!("Failed to create hash thread: {}", e))
        })?;

        Ok(vec![Box::new(thread)])
    }
}

/// Adapter implementing [`VoltageRegulator`] for EmberOne's TPS546 power controller.
///
/// Delegates to [`Tps546::set_vout_target()`] which writes the VOUT_COMMAND register
/// without changing the output enable state. The voltage regulator is already
/// enabled during board initialization; the ramp only adjusts the target.
struct EmberOneVoltageRegulator {
    tps546: Arc<Mutex<Tps546<BitaxeRawI2c>>>,
}

#[async_trait]
impl VoltageRegulator for EmberOneVoltageRegulator {
    async fn set_voltage(&mut self, volts: f32) -> anyhow::Result<()> {
        self.tps546.lock().await.set_vout_target(volts).await?;
        Ok(())
    }
}

/// Adapter implementing `AsicEnable` for EmberOne's GPIO-based power and reset control.
///
/// Controls three GPIO pins:
/// - Reset pin (GPIO 0): active-low reset for ASIC chips
/// - Power enable pin (GPIO 1): active-high power enable for voltage regulator
/// - IO power enable pin (GPIO 2): active-high IO power enable for serial communication
struct EmberOneAsicEnable {
    reset_pin: BitaxeRawGpioPin,
    power_enable_pin: BitaxeRawGpioPin,
    io_power_enable_pin: BitaxeRawGpioPin,
}

#[async_trait]
impl AsicEnable for EmberOneAsicEnable {
    async fn enable(&mut self) -> anyhow::Result<()> {
        // Enable power first
        self.power_enable_pin
            .write(PinValue::High)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to enable power: {}", e))?;

        // Wait for power to stabilize
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;

        // Release reset (nRST is active-low, so High = running)
        self.reset_pin
            .write(PinValue::High)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to release reset: {}", e))
    }

    async fn disable(&mut self) -> anyhow::Result<()> {
        // Assert reset first
        self.reset_pin
            .write(PinValue::Low)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to assert reset: {}", e))?;

        // Disable main power
        self.power_enable_pin
            .write(PinValue::Low)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to disable power: {}", e))?;

        // Disable IO power
        self.io_power_enable_pin
            .write(PinValue::Low)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to disable IO power: {}", e))
    }
}

// Factory function to create EmberOne board from USB device info
async fn create_from_usb(
    device: UsbDeviceInfo,
) -> crate::error::Result<(Box<dyn Board + Send>, super::BoardRegistration)> {
    // Get serial ports
    let serial_ports = device.serial_ports()?;

    // EmberOne uses 2 serial ports like Bitaxe (control + data)
    if serial_ports.len() != 2 {
        return Err(Error::Hardware(format!(
            "EmberOne requires exactly 2 serial ports, found {}",
            serial_ports.len()
        )));
    }

    let control_port_path = serial_ports[0].clone();
    let data_port_path = serial_ports[1].clone();

    debug!(
        serial = ?device.serial_number,
        control = %control_port_path,
        data = %data_port_path,
        "EmberOne serial ports"
    );

    // Open control port
    let control_port = tokio_serial::new(&control_port_path, 115200)
        .open_native_async()
        .map_err(|e| Error::Hardware(format!("Failed to open control port: {}", e)))?;
    let control_channel = ControlChannel::new(control_port);

    // Create watch channel for board state, seeded with identity
    let serial = device.serial_number.clone();
    let initial_state = BoardState {
        name: format!("emberone-{}", serial.as_deref().unwrap_or("unknown")),
        model: "EmberOne".into(),
        serial,
        ..Default::default()
    };
    let (state_tx, state_rx) = watch::channel(initial_state);

    // Create and initialize board
    let mut board = EmberOne::new(device, control_channel, data_port_path, state_tx);

    board
        .initialize()
        .await
        .map_err(|e| Error::Hardware(format!("Failed to initialize board: {}", e)))?;

    let registration = super::BoardRegistration { state_rx };
    Ok((Box::new(board), registration))
}
