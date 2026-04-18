use std::{
    collections::HashSet,
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

#[cfg(target_os = "linux")]
use std::fs;

use tokio::{
    io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    sync::watch,
};
use tokio_serial::{SerialPort, SerialPortBuilderExt, SerialStream};
use tokio_util::sync::CancellationToken;

use crate::{
    api::registry::BoardRegistry,
    api_client::types::MinerState,
    config::GtTouchDisplayConfig,
    error::{Error, Result},
    tracing::prelude::*,
};

const GT_TOUCH_USB_VID: &str = "303a";
const GT_TOUCH_USB_PID: &str = "4001";
const GT_TOUCH_PRODUCT: &str = "GT Touch CDC";

pub struct ServiceContext {
    pub miner_state_rx: watch::Receiver<MinerState>,
    pub board_registry: Arc<Mutex<BoardRegistry>>,
    pub default_device_model: Option<String>,
    pub default_asic_model: Option<String>,
    pub default_pool_url: Option<String>,
    pub default_pool_user: Option<String>,
    pub default_mode: Option<String>,
    pub default_voltage: Option<f32>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Subscription {
    Hashrate,
    Temperature,
    Power,
    FanSpeed,
    FanSpeedPercent,
    Shares,
    BestDifficulty,
    Voltage,
    BlockHeight,
}

impl Subscription {
    fn from_parameter(parameter: &str) -> Option<Self> {
        match parameter {
            "hashrate" => Some(Self::Hashrate),
            "temperature" => Some(Self::Temperature),
            "power" => Some(Self::Power),
            "fan_speed" => Some(Self::FanSpeed),
            "fan_speed_percent" => Some(Self::FanSpeedPercent),
            "shares" => Some(Self::Shares),
            "best_difficulty" => Some(Self::BestDifficulty),
            "voltage" => Some(Self::Voltage),
            "block_height" => Some(Self::BlockHeight),
            _ => None,
        }
    }
}

#[derive(Default)]
struct SessionState {
    subscriptions: HashSet<Subscription>,
}

#[derive(Debug)]
enum IncomingMessage {
    Subscribe(Subscription),
    Request { parameter: String },
    Set { parameter: String, value: String },
}

pub async fn task(
    config: GtTouchDisplayConfig,
    context: ServiceContext,
    shutdown: CancellationToken,
) {
    if !config.enabled {
        return;
    }

    let reconnect_delay = Duration::from_millis(config.reconnect_delay_ms);

    loop {
        if shutdown.is_cancelled() {
            return;
        }

        match connect(&config).await {
            Ok(port) => {
                if let Err(error) =
                    serve_connection(port, &config, &context, shutdown.clone()).await
                {
                    warn!(error = %error, "GT Touch connection ended");
                }
            }
            Err(error) => {
                warn!(error = %error, "Failed to connect to GT Touch display");
            }
        }

        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(reconnect_delay) => {}
        }
    }
}

async fn connect(config: &GtTouchDisplayConfig) -> Result<SerialStream> {
    let path = resolve_serial_path(config)?;
    let path_string = path.to_string_lossy().into_owned();

    let mut port = tokio_serial::new(path_string, config.baud_rate)
        .open_native_async()
        .map_err(Error::from)?;

    port.write_data_terminal_ready(true)?;

    info!(path = %path.display(), "GT Touch display connected");
    Ok(port)
}

async fn serve_connection(
    port: SerialStream,
    config: &GtTouchDisplayConfig,
    context: &ServiceContext,
    shutdown: CancellationToken,
) -> Result<()> {
    let (read_half, mut write_half) = tokio::io::split(port);
    let mut lines = BufReader::new(read_half).lines();
    let mut interval = tokio::time::interval(Duration::from_millis(config.update_interval_ms));
    let mut session = SessionState::default();

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            line = lines.next_line() => {
                let Some(line) = line? else {
                    return Err(Error::Io(io::Error::new(io::ErrorKind::UnexpectedEof, "GT Touch disconnected")));
                };

                if line.trim().is_empty() {
                    continue;
                }

                match parse_line(&line) {
                    Ok(IncomingMessage::Subscribe(subscription)) => {
                        session.subscriptions.insert(subscription);
                        publish_subscription(&mut write_half, subscription, &sample_state(context)).await?;
                    }
                    Ok(IncomingMessage::Request { parameter }) if parameter == "systemInfo" => {
                        send_system_info(&mut write_half, &sample_state(context), context).await?;
                    }
                    Ok(IncomingMessage::Request { parameter }) => {
                        debug!(parameter = %parameter, "Ignoring unsupported GT Touch request");
                    }
                    Ok(IncomingMessage::Set { parameter, value }) => {
                        warn!(parameter = %parameter, value = %value, "GT Touch setting changes are not implemented yet");
                    }
                    Err(error) => {
                        debug!(line = %line, error = %error, "Ignoring malformed GT Touch line");
                    }
                }
            }
            _ = interval.tick() => {
                let state = sample_state(context);
                for subscription in session.subscriptions.iter().copied() {
                    publish_subscription(&mut write_half, subscription, &state).await?;
                }
            }
        }
    }
}

fn sample_state(context: &ServiceContext) -> MinerState {
    let mut state = context.miner_state_rx.borrow().clone();
    state.boards = context
        .board_registry
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .boards();
    state
}

async fn send_system_info<W: AsyncWrite + Unpin>(
    writer: &mut W,
    state: &MinerState,
    context: &ServiceContext,
) -> Result<()> {
    let device_model = state
        .boards
        .first()
        .map(|board| board.model.clone())
        .or_else(|| context.default_device_model.clone());
    let asic_model = context.default_asic_model.clone();
    let (pool, pool_port) = state
        .sources
        .iter()
        .find_map(|source| source.url.as_deref())
        .or(context.default_pool_url.as_deref())
        .map(split_pool_url)
        .unwrap_or_default();
    let pool_user = context.default_pool_user.clone();
    let mode = context
        .default_mode
        .clone()
        .or_else(|| Some("normal".to_string()));
    let voltage = first_voltage(state).or(context.default_voltage);

    if let Some(value) = device_model {
        send_response(writer, "deviceModel", &value).await?;
    }
    if let Some(value) = asic_model {
        send_response(writer, "asicModel", &value).await?;
    }
    if let Some(value) = pool {
        send_response(writer, "pool", &value).await?;
    }
    if let Some(value) = pool_port {
        send_response(writer, "poolPort", &value).await?;
    }
    if let Some(value) = pool_user {
        send_response(writer, "poolUser", &value).await?;
    }
    if let Some(value) = mode {
        send_response(writer, "mode", &value).await?;
    }
    if let Some(value) = voltage {
        send_response(writer, "voltage", &format_gt_touch_voltage(value)).await?;
    }

    Ok(())
}

async fn publish_subscription<W: AsyncWrite + Unpin>(
    writer: &mut W,
    subscription: Subscription,
    state: &MinerState,
) -> Result<()> {
    match subscription {
        Subscription::Hashrate => {
            send_response(
                writer,
                "hashrate",
                &format!("{:.2}", state.hashrate as f64 / 1_000_000_000.0),
            )
            .await?;
        }
        Subscription::Temperature => {
            if let Some(value) = max_temperature(state) {
                send_response(writer, "chipTemp", &format!("{value:.2}")).await?;
            }
        }
        Subscription::Power => {
            if let Some(value) = total_power(state) {
                send_response(writer, "power", &format!("{value:.1}")).await?;
            }
        }
        Subscription::FanSpeed => {
            if let Some(value) = max_fan_rpm(state) {
                send_response(writer, "fan_speed", &value.to_string()).await?;
            }
        }
        Subscription::FanSpeedPercent => {
            if let Some(value) = max_fan_percent(state) {
                send_response(writer, "fan_speed_percent", &value.to_string()).await?;
            }
        }
        Subscription::Shares => {
            send_response(writer, "shares", &state.shares_submitted.to_string()).await?;
        }
        Subscription::BestDifficulty => {
            if let Some(value) = state.best_difficulty {
                send_response(writer, "best_difficulty", &value.to_string()).await?;
            }
        }
        Subscription::Voltage => {
            if let Some(value) = first_voltage(state) {
                send_response(writer, "voltage", &format_gt_touch_voltage(value)).await?;
            }
        }
        Subscription::BlockHeight => {}
    }

    Ok(())
}

async fn send_response<W: AsyncWrite + Unpin>(
    writer: &mut W,
    parameter: &str,
    value: &str,
) -> Result<()> {
    let body = format!("BAP,RES,{parameter},{value}");
    let checksum = checksum(&body);
    let line = format!("${body}*{checksum:02X}\r\n");
    writer.write_all(line.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

fn parse_line(line: &str) -> Result<IncomingMessage> {
    let trimmed = line.trim();

    if let Some(parameter) = trimmed.strip_prefix("$BAP,SUB,") {
        return Subscription::from_parameter(parameter)
            .map(IncomingMessage::Subscribe)
            .ok_or_else(|| {
                Error::Protocol(format!("unsupported subscription parameter {parameter}"))
            });
    }

    let (body, received_checksum) = split_checked_line(trimmed)?;
    let expected_checksum = checksum(body);
    if expected_checksum != received_checksum {
        return Err(Error::Protocol(format!(
            "checksum mismatch: expected {expected_checksum:02X}, got {received_checksum:02X}"
        )));
    }

    let mut parts = body.splitn(4, ',');
    if parts.next() != Some("BAP") {
        return Err(Error::Protocol("missing BAP prefix".into()));
    }

    let Some(command) = parts.next() else {
        return Err(Error::Protocol("missing command".into()));
    };
    let Some(parameter) = parts.next() else {
        return Err(Error::Protocol("missing parameter".into()));
    };

    match command {
        "REQ" => Ok(IncomingMessage::Request {
            parameter: parameter.to_string(),
        }),
        "SET" => Ok(IncomingMessage::Set {
            parameter: parameter.to_string(),
            value: parts.next().unwrap_or_default().to_string(),
        }),
        other => Err(Error::Protocol(format!("unsupported command {other}"))),
    }
}

fn split_checked_line(line: &str) -> Result<(&str, u8)> {
    let line = line
        .strip_prefix('$')
        .ok_or_else(|| Error::Protocol("missing $ prefix".into()))?;
    let (body, checksum_hex) = line
        .rsplit_once('*')
        .ok_or_else(|| Error::Protocol("missing checksum separator".into()))?;
    let checksum = u8::from_str_radix(checksum_hex, 16)
        .map_err(|_| Error::Protocol("invalid checksum hex".into()))?;
    Ok((body, checksum))
}

fn checksum(body: &str) -> u8 {
    body.as_bytes().iter().fold(0, |acc, byte| acc ^ byte)
}

fn resolve_serial_path(config: &GtTouchDisplayConfig) -> Result<PathBuf> {
    if let Some(path) = &config.serial_path {
        return Ok(path.clone());
    }

    #[cfg(target_os = "linux")]
    {
        auto_detect_serial_path().ok_or_else(|| {
            Error::Config(
                "GT Touch display not found; set hardware.amlogic_control_board.gt_touch_display.serial_path".into(),
            )
        })
    }

    #[cfg(not(target_os = "linux"))]
    {
        Err(Error::Config(
            "GT Touch auto-detection is only available on Linux; configure serial_path explicitly"
                .into(),
        ))
    }
}

#[cfg(target_os = "linux")]
fn auto_detect_serial_path() -> Option<PathBuf> {
    let entries = fs::read_dir("/sys/class/tty").ok()?;
    let mut candidates = Vec::new();

    for entry in entries.flatten() {
        let name = entry.file_name();
        let tty = name.to_string_lossy();
        if !tty.starts_with("ttyACM") && !tty.starts_with("ttyUSB") {
            continue;
        }

        let device_path = entry.path().join("device");
        if gt_touch_matches_sysfs_path(&device_path) {
            candidates.push(PathBuf::from("/dev").join(tty.as_ref()));
        }
    }

    candidates.sort();
    candidates.into_iter().next()
}

#[cfg(target_os = "linux")]
fn gt_touch_matches_sysfs_path(device_path: &Path) -> bool {
    let Ok(canonical) = device_path.canonicalize() else {
        return false;
    };

    for ancestor in canonical.ancestors() {
        let vid = read_trimmed(ancestor.join("idVendor"));
        let pid = read_trimmed(ancestor.join("idProduct"));
        let product = read_trimmed(ancestor.join("product"));

        if vid.as_deref() == Some(GT_TOUCH_USB_VID) && pid.as_deref() == Some(GT_TOUCH_USB_PID) {
            return true;
        }

        if product.as_deref() == Some(GT_TOUCH_PRODUCT) {
            return true;
        }
    }

    false
}

#[cfg(target_os = "linux")]
fn read_trimmed(path: PathBuf) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|value| value.trim().to_string())
}

fn max_temperature(state: &MinerState) -> Option<f32> {
    state
        .boards
        .iter()
        .flat_map(|board| board.temperatures.iter())
        .filter_map(|sensor| sensor.temperature_c)
        .reduce(f32::max)
}

fn total_power(state: &MinerState) -> Option<f32> {
    let total: f32 = state
        .boards
        .iter()
        .flat_map(|board| board.powers.iter())
        .filter_map(|power| power.power_w)
        .sum();

    if total > 0.0 { Some(total) } else { None }
}

fn first_voltage(state: &MinerState) -> Option<f32> {
    state
        .boards
        .iter()
        .flat_map(|board| board.powers.iter())
        .filter_map(|power| power.voltage_v)
        .next()
}

fn format_gt_touch_voltage(voltage_v: f32) -> String {
    format!("{:.2}", voltage_v * 100.0)
}

fn max_fan_rpm(state: &MinerState) -> Option<u32> {
    state
        .boards
        .iter()
        .flat_map(|board| board.fans.iter())
        .filter_map(|fan| fan.rpm)
        .max()
}

fn max_fan_percent(state: &MinerState) -> Option<u8> {
    state
        .boards
        .iter()
        .flat_map(|board| board.fans.iter())
        .filter_map(|fan| fan.percent.or(fan.target_percent))
        .max()
}

fn split_pool_url(url: &str) -> (Option<String>, Option<String>) {
    let Some(scheme_sep) = url.find("://") else {
        return (Some(url.to_string()), None);
    };

    let scheme_end = scheme_sep + 3;
    let Some(port_sep) = url[scheme_end..].rfind(':') else {
        return (Some(url.to_string()), None);
    };
    let port_sep = scheme_end + port_sep;
    let host_and_scheme = &url[..port_sep];
    let port = &url[(port_sep + 1)..];

    if port.chars().all(|c| c.is_ascii_digit()) {
        (Some(host_and_scheme.to_string()), Some(port.to_string()))
    } else {
        (Some(url.to_string()), None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_checked_responses() {
        let body = "BAP,RES,hashrate,1234.56";
        assert_eq!(checksum(body), 0x02);
    }

    #[test]
    fn parses_subscription_lines() {
        let message = parse_line("$BAP,SUB,hashrate").expect("valid SUB");
        assert!(matches!(
            message,
            IncomingMessage::Subscribe(Subscription::Hashrate)
        ));
    }

    #[test]
    fn parses_new_subscription_lines() {
        let message = parse_line("$BAP,SUB,fan_speed_percent").expect("valid SUB");
        assert!(matches!(
            message,
            IncomingMessage::Subscribe(Subscription::FanSpeedPercent)
        ));

        let message = parse_line("$BAP,SUB,voltage").expect("valid SUB");
        assert!(matches!(
            message,
            IncomingMessage::Subscribe(Subscription::Voltage)
        ));
    }

    #[test]
    fn parses_checked_request_lines() {
        let message = parse_line("$BAP,REQ,systemInfo*3E").expect("valid REQ");
        match message {
            IncomingMessage::Request { parameter } => assert_eq!(parameter, "systemInfo"),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn splits_pool_url_and_port() {
        let (pool, port) = split_pool_url("stratum+tcp://pool.256foundation.org:3333");
        assert_eq!(
            pool.as_deref(),
            Some("stratum+tcp://pool.256foundation.org")
        );
        assert_eq!(port.as_deref(), Some("3333"));
    }

    #[test]
    fn keeps_pool_url_without_port() {
        let (pool, port) = split_pool_url("stratum+tcp://pool.256foundation.org");
        assert_eq!(
            pool.as_deref(),
            Some("stratum+tcp://pool.256foundation.org")
        );
        assert_eq!(port, None);
    }

    #[test]
    fn finds_max_metrics() {
        let mut state = MinerState::default();
        state.boards = vec![BoardState {
            name: "board".into(),
            model: "Test".into(),
            serial: None,
            fans: vec![
                crate::api_client::types::Fan {
                    name: "fan0".into(),
                    rpm: Some(5400),
                    percent: Some(75),
                    target_percent: None,
                },
                crate::api_client::types::Fan {
                    name: "fan1".into(),
                    rpm: Some(6200),
                    percent: None,
                    target_percent: Some(68),
                },
            ],
            temperatures: vec![
                crate::api_client::types::TemperatureSensor {
                    name: "temp0".into(),
                    temperature_c: Some(61.5),
                },
                crate::api_client::types::TemperatureSensor {
                    name: "temp1".into(),
                    temperature_c: Some(67.0),
                },
            ],
            powers: vec![crate::api_client::types::PowerMeasurement {
                name: "psu".into(),
                voltage_v: Some(12.3),
                current_a: Some(10.0),
                power_w: Some(123.0),
            }],
            threads: vec![],
        }];

        assert_eq!(max_fan_rpm(&state), Some(6200));
        assert_eq!(max_fan_percent(&state), Some(75));
        assert_eq!(max_temperature(&state), Some(67.0));
        assert_eq!(total_power(&state), Some(123.0));
        assert_eq!(first_voltage(&state), Some(12.3));
        assert_eq!(format_gt_touch_voltage(12.0), "1200.00");
    }
}
