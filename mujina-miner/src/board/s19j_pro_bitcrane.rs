//! S19j Pro hashboard support via Bitcrane v3.
//!
//! The S19j Pro is a hashboard with 126 BM1362 ASIC chips, communicating via
//! USB using the bitcrane protocol. Power is provided by an APW12 PSU controlled
//! via bit-banged I2C.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{Mutex, watch};
use tokio_serial::SerialPortBuilderExt;
use tokio_util::codec::{FramedRead, FramedWrite};

use super::{
    Board, BoardDescriptor, BoardError, BoardInfo,
    pattern::{BoardPattern, Match, StringMatch},
};
use crate::{
    api_client::types::{BoardState, Fan, MinerState, TemperatureSensor},
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
    hw_trait::gpio::{GpioPin, PinValue},
    mgmt_protocol::{
        Apw12Psu, ControlChannel,
        bitcrane::{
            display::BitcraneDisplay,
            fan::{self, BitcraneFan},
            gpio::{BitcraneGpioController, BitcraneGpioPin, BitcraneGpioPinHandle},
            i2c::BitcraneI2c,
        },
    },
    peripheral::tmp75::{self, Tmp75},
    tracing::prelude::*,
    transport::{
        UsbDeviceInfo,
        serial::{SerialControl, SerialStream},
    },
};

const BOARD_MODEL: &str = "S19j Pro (Bitcrane v3)";
const BOARD_NAME_PREFIX: &str = "s19jpro-bitcrane";
const THREAD_NAME_PREFIX: &str = "S19jProBitcrane";

// Register this board type with the inventory system
inventory::submit! {
    BoardDescriptor {
        pattern: BoardPattern {
            vid: Match::Any,
            pid: Match::Any,
            bcd_device: Match::Any,
            manufacturer: Match::Specific(StringMatch::Exact("256F")),
            product: Match::Specific(StringMatch::Exact("bitcrane_S19jpro")),
            serial_pattern: Match::Any,
        },
        name: BOARD_MODEL,
        create_fn: |device| Box::pin(create_from_usb(device)),
    }
}

/// S19j Pro hashboard via Bitcrane v3.
pub struct S19jProBitcrane {
    device_info: UsbDeviceInfo,
    data_port_path: String,
    /// Control channel for board management (bitcrane protocol).
    control_channel: ControlChannel,
    /// Control handle for data channel (for baud rate changes).
    data_control: Option<SerialControl>,
    /// ASIC reset pin (RST0, active-low).
    reset_pin: Option<BitcraneGpioPinHandle>,
    /// APW12 PSU controller.
    psu: Option<Arc<Mutex<Apw12Psu>>>,
    /// TMP75 temperature sensors (2 per hashboard).
    temp_sensors: Option<(Tmp75, Tmp75)>,

    /// Channel for publishing board state to the API server.
    state_tx: watch::Sender<BoardState>,
    /// Receiver for miner state (hashrate for OLED display).
    miner_state_rx: Option<watch::Receiver<MinerState>>,
}

impl S19jProBitcrane {
    /// Create a new Bitcrane-backed S19j Pro board instance.
    pub fn new(
        device_info: UsbDeviceInfo,
        control_channel: ControlChannel,
        data_port_path: String,
        state_tx: watch::Sender<BoardState>,
    ) -> Self {
        Self {
            device_info,
            data_port_path,
            control_channel,
            data_control: None,
            reset_pin: None,
            psu: None,
            temp_sensors: None,
            state_tx,
            miner_state_rx: None,
        }
    }

    /// Initialize the board hardware.
    ///
    /// Sets up GPIO pins, initializes APW12 PSU, and holds ASICs in reset until mining starts.
    pub async fn initialize(&mut self) -> Result<(), BoardError> {
        // Get GPIO controller using bitcrane protocol
        let gpio = BitcraneGpioController::new(self.control_channel.clone());

        // Get RST0 pin for first hashboard
        let mut reset_pin = gpio.pin(BitcraneGpioPin::Rst0);

        // Initialize to safe state: chips in reset (RST0 low = reset asserted)
        debug!("Initializing {BOARD_MODEL}: chips in reset");
        reset_pin.write(PinValue::Low).await.map_err(|e| {
            BoardError::InitializationFailed(format!("Failed to assert reset: {}", e))
        })?;

        // Store GPIO pin for later use
        self.reset_pin = Some(reset_pin);

        // Initialize APW12 PSU
        debug!("Initializing APW12 PSU");
        let mut psu = Apw12Psu::new(self.control_channel.clone());

        // Enable PSU
        psu.set_enabled(true).await.map_err(|e| {
            BoardError::InitializationFailed(format!("Failed to enable PSU: {}", e))
        })?;

        // Wait for PSU to power up
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Disable watchdog (0x00 = disabled)
        psu.config_watchdog(0x00).await.map_err(|e| {
            BoardError::InitializationFailed(format!("Failed to configure PSU watchdog: {}", e))
        })?;

        // Set initial voltage for BM1362 chain
        const DEFAULT_VOUT: f32 = 12.6;
        psu.set_voltage(DEFAULT_VOUT).await.map_err(|e| {
            BoardError::InitializationFailed(format!("Failed to set PSU voltage: {}", e))
        })?;

        info!("APW12 PSU enabled, voltage set to {}V", DEFAULT_VOUT);

        // Wait for voltage to stabilize before chip enumeration
        // Longer delay needed for 126-chip chain to fully power up
        tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;

        // Verify voltage
        match psu.measure_voltage().await {
            Ok(v) => info!("APW12 measured voltage: {:.2}V", v),
            Err(e) => warn!("Failed to measure PSU voltage: {}", e),
        }

        self.psu = Some(Arc::new(Mutex::new(psu)));

        // Initialize TMP75 temperature sensors for hashboard 0
        let i2c = BitcraneI2c::new(self.control_channel.clone());
        let (temp0, temp1) = tmp75::sensors_for_hashboard(i2c, 0);
        self.temp_sensors = Some((temp0.clone(), temp1.clone()));

        // Initialize fans and set to 50% speed
        let fans = fan::all_fans(self.control_channel.clone());
        const DEFAULT_FAN_SPEED: u8 = 50;
        for fan in &fans {
            if let Err(e) = fan.set_speed(DEFAULT_FAN_SPEED).await {
                warn!(fan = %fan.name(), error = %e, "Failed to set fan speed");
            }
        }
        info!("Fans initialized at {}% speed", DEFAULT_FAN_SPEED);

        info!("{BOARD_MODEL} initialized successfully");
        Ok(())
    }
}

#[async_trait]
impl Board for S19jProBitcrane {
    fn board_info(&self) -> BoardInfo {
        BoardInfo {
            model: BOARD_MODEL.to_string(),
            firmware_version: None,
            serial_number: self.device_info.serial_number.clone(),
        }
    }

    async fn shutdown(&mut self) -> Result<(), BoardError> {
        // Set all fans to 0%
        for (i, fan) in fan::all_fans(self.control_channel.clone())
            .into_iter()
            .enumerate()
        {
            if let Err(e) = fan.set_speed(0).await {
                warn!(fan = i, "Failed to set fan to 0% on shutdown: {}", e);
            }
        }
        info!("Fans set to 0%");

        // Assert reset to stop ASICs
        if let Some(ref mut reset_pin) = self.reset_pin {
            if let Err(e) = reset_pin.write(PinValue::Low).await {
                warn!("Failed to assert reset on shutdown: {}", e);
            }
        }

        // Disable PSU
        if let Some(ref psu) = self.psu {
            if let Err(e) = psu.lock().await.set_enabled(false).await {
                warn!("Failed to disable PSU on shutdown: {}", e);
            }
        }

        info!("{BOARD_MODEL} shutdown complete");
        Ok(())
    }

    async fn create_hash_threads(&mut self) -> Result<Vec<Box<dyn HashThread>>, BoardError> {
        // Take GPIO pin from initialization
        let reset_pin = self.reset_pin.take().ok_or_else(|| {
            BoardError::InitializationFailed("Reset pin not initialized".to_string())
        })?;

        // Open data port
        let data_stream = SerialStream::new(&self.data_port_path, 115200).map_err(|e| {
            BoardError::InitializationFailed(format!("Failed to open data port: {}", e))
        })?;
        let (data_reader, data_writer, data_control) = data_stream.split();

        // Flush any stale data in the serial buffer before enumeration
        data_control.flush_input().map_err(|e| {
            BoardError::InitializationFailed(format!("Failed to flush serial buffer: {}", e))
        })?;

        self.data_control = Some(data_control);

        // Create framed reader/writer for BM13xx protocol
        let chip_rx = FramedRead::new(data_reader, bm13xx::FrameCodec);
        let chip_tx = FramedWrite::new(data_writer, bm13xx::FrameCodec);

        // Build thread name from board model and serial
        let thread_name = match &self.device_info.serial_number {
            Some(serial) => format!("{THREAD_NAME_PREFIX}-{}", &serial[..8.min(serial.len())]),
            None => THREAD_NAME_PREFIX.to_string(),
        };

        // Build chain configuration for S19j Pro: 42 series domains × 3 chips = 126 BM1362 chips
        // Use APW12 PSU for voltage regulation
        let voltage_regulator: Option<Arc<Mutex<dyn VoltageRegulator + Send>>> = self
            .psu
            .as_ref()
            .map(|psu| Arc::clone(psu) as Arc<Mutex<dyn VoltageRegulator + Send>>);

        let config = ChainConfig {
            name: thread_name,
            // S19j Pro: 42 series domains, 3 chips per domain (126 total)
            topology: TopologySpec::uniform_domains(42, 3, false),
            chip_config: chip_config::bm1362(),
            peripherals: ChainPeripherals {
                asic_enable: Arc::new(Mutex::new(S19jProBitcraneAsicEnable { reset_pin })),
                voltage_regulator,
                chip_uart_baud: None,
                ramp_coordinator: None,
                chain_index: 0,
                thermal_cap_mhz: None,
            },
            post_broadcast_chip_baud: None,
        };

        // Create the hash thread
        let thread = thread_v2::BM13xxThread::new(chip_rx, chip_tx, config).map_err(|e| {
            BoardError::InitializationFailed(format!("Failed to create hash thread: {}", e))
        })?;

        // Spawn telemetry task now that miner_state_rx is available
        // (set_miner_state_rx is called by backplane before create_hash_threads)
        let (temp0, temp1) = self.temp_sensors.take().ok_or_else(|| {
            BoardError::InitializationFailed("Temperature sensors not initialized".to_string())
        })?;
        let fans = fan::all_fans(self.control_channel.clone());
        let display = BitcraneDisplay::new(self.control_channel.clone());
        let state_tx = self.state_tx.clone();
        let miner_state_rx = self.miner_state_rx.clone();
        tokio::spawn(async move {
            telemetry_task(temp0, temp1, fans, display, state_tx, miner_state_rx).await;
        });

        Ok(vec![Box::new(thread)])
    }

    fn set_miner_state_rx(&mut self, rx: watch::Receiver<MinerState>) {
        self.miner_state_rx = Some(rx);
    }
}

/// Telemetry task that periodically reads temperature sensors and fan RPMs.
///
/// Runs indefinitely, updating state_tx with readings every 2 seconds.
/// Also updates the OLED display with hashrate every 10 seconds.
async fn telemetry_task(
    temp0: Tmp75,
    temp1: Tmp75,
    fans: [BitcraneFan; 4],
    display: BitcraneDisplay,
    state_tx: watch::Sender<BoardState>,
    miner_state_rx: Option<watch::Receiver<MinerState>>,
) {
    const TELEMETRY_INTERVAL: Duration = Duration::from_secs(2);
    const DISPLAY_UPDATE_INTERVAL: u32 = 5; // Update display every N telemetry cycles

    let mut cycle_count: u32 = 0;

    loop {
        // Read both temperature sensors
        let temp0_result = temp0.read_temperature().await;
        let temp1_result = temp1.read_temperature().await;

        // Build temperature sensor readings
        let temperatures = vec![
            TemperatureSensor {
                name: temp0.name().to_string(),
                temperature_c: temp0_result
                    .inspect_err(|e| debug!(sensor = %temp0.name(), error = %e, "Temp read failed"))
                    .ok(),
            },
            TemperatureSensor {
                name: temp1.name().to_string(),
                temperature_c: temp1_result
                    .inspect_err(|e| debug!(sensor = %temp1.name(), error = %e, "Temp read failed"))
                    .ok(),
            },
        ];

        // Read all fan RPMs
        let mut fan_states = Vec::with_capacity(4);
        for fan in &fans {
            let rpm_result = fan.read_rpm().await;
            fan_states.push(Fan {
                name: fan.name().to_string(),
                rpm: rpm_result
                    .inspect_err(|e| debug!(fan = %fan.name(), error = %e, "Fan RPM read failed"))
                    .ok(),
                percent: None,            // We don't track current duty cycle yet
                target_percent: Some(50), // We set 50% at init
            });
        }

        // Update board state
        state_tx.send_modify(|state| {
            state.temperatures = temperatures;
            state.fans = fan_states;
        });

        // Update OLED display periodically with actual hashrate from scheduler
        if cycle_count % DISPLAY_UPDATE_INTERVAL == 0 {
            let hashrate_gh = miner_state_rx
                .as_ref()
                .map(|rx| rx.borrow().hashrate as f64 / 1_000_000_000.0) // H/s to GH/s
                .unwrap_or(0.0);

            if let Err(e) = display.display_hashrate(hashrate_gh).await {
                debug!(error = %e, "Failed to update OLED display");
            }
        }
        cycle_count = cycle_count.wrapping_add(1);

        tokio::time::sleep(TELEMETRY_INTERVAL).await;
    }
}

/// Adapter implementing `AsicEnable` for S19j Pro's GPIO-based reset control.
///
/// Controls RST0 pin via bitcrane protocol (active-low reset for ASIC chips).
/// Power is handled externally.
struct S19jProBitcraneAsicEnable {
    reset_pin: BitcraneGpioPinHandle,
}

#[async_trait]
impl AsicEnable for S19jProBitcraneAsicEnable {
    async fn enable(&mut self) -> anyhow::Result<()> {
        // Release reset (RST0 is active-low, so High = running)
        self.reset_pin
            .write(PinValue::High)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to release reset: {}", e))?;

        // Wait for all 126 chips (42 series domains × 3 chips) to come out of
        // reset and stabilize before enumeration begins
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;

        Ok(())
    }

    async fn disable(&mut self) -> anyhow::Result<()> {
        // Assert reset (RST0 low = reset asserted)
        self.reset_pin
            .write(PinValue::Low)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to assert reset: {}", e))
    }
}

// Factory function to create S19j Pro board from USB device info
async fn create_from_usb(
    device: UsbDeviceInfo,
) -> crate::error::Result<(Box<dyn Board + Send>, super::BoardRegistration)> {
    // Get serial ports
    let serial_ports = device.serial_ports()?;

    // S19j Pro uses 4 serial ports: control + 3 hashboard data channels
    // For now, just use the first hashboard (ports 0=control, 1=data)
    if serial_ports.len() != 4 {
        return Err(Error::Hardware(format!(
            "S19j Pro requires exactly 4 serial ports, found {}",
            serial_ports.len()
        )));
    }

    let control_port_path = serial_ports[0].clone();
    let data_port_path = serial_ports[1].clone(); // First hashboard

    debug!(
        serial = ?device.serial_number,
        control = %control_port_path,
        data = %data_port_path,
        "S19j Pro Bitcrane serial ports"
    );

    // Open control port
    let control_port = tokio_serial::new(&control_port_path, 115200)
        .open_native_async()
        .map_err(|e| Error::Hardware(format!("Failed to open control port: {}", e)))?;
    let control_channel = ControlChannel::new(control_port);

    // Create watch channel for board state, seeded with identity
    let serial = device.serial_number.clone();
    let initial_state = BoardState {
        name: format!(
            "{BOARD_NAME_PREFIX}-{}",
            serial.as_deref().unwrap_or("unknown")
        ),
        model: BOARD_MODEL.into(),
        serial,
        ..Default::default()
    };
    let (state_tx, state_rx) = watch::channel(initial_state);

    // Create and initialize board
    let mut board = S19jProBitcrane::new(device, control_channel, data_port_path, state_tx);

    board
        .initialize()
        .await
        .map_err(|e| Error::Hardware(format!("Failed to initialize board: {}", e)))?;

    let registration = super::BoardRegistration { state_rx };
    Ok((Box::new(board), registration))
}
