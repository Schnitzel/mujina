//! Short-lived modes a commissioning tool asks for over the API: making one
//! miner findable in a rack of identical ones, and finding out which Shelly
//! relay feeds which miner's PSU.
//!
//! Both are plain deadlines. The board's telemetry loop reads them each tick, so
//! neither can get stuck on: when the deadline passes, normal behaviour resumes.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tracing::{debug, info};

static EPOCH: OnceLock<Instant> = OnceLock::new();
static IDENTIFY_UNTIL_MS: AtomicU64 = AtomicU64::new(0);
static PSU_PROBE_UNTIL_MS: AtomicU64 = AtomicU64::new(0);
static LED_GPIOS: OnceLock<Vec<u32>> = OnceLock::new();

/// Longest window any caller can ask for.
pub const MAX_WINDOW_SECS: u64 = 120;
const LED_BLINK_PERIOD: Duration = Duration::from_millis(250);

fn now_ms() -> u64 {
    EPOCH.get_or_init(Instant::now).elapsed().as_millis() as u64
}

fn deadline_after(secs: u64) -> u64 {
    now_ms() + secs.clamp(1, MAX_WINDOW_SECS) * 1000
}

/// Tell this module which status LEDs the board has. Boards without LEDs simply
/// never call it, and identify still spins the fans.
pub fn register_leds(gpios: Vec<u32>) {
    let _ = LED_GPIOS.set(gpios);
}

/// Start an identify window: fans to 100 % and blinking LEDs.
pub fn start_identify(secs: u64) {
    let already_running = identify_active();
    IDENTIFY_UNTIL_MS.store(deadline_after(secs), Ordering::Relaxed);
    info!(secs, "Identify requested");
    if !already_running {
        tokio::spawn(blink_leds());
    }
}

/// Whether an identify window is open. The telemetry loop pins the fans on this.
pub fn identify_active() -> bool {
    now_ms() < IDENTIFY_UNTIL_MS.load(Ordering::Relaxed)
}

/// Open a window in which the PSU is read every tick, skipping the backoff that
/// otherwise applies to an unpowered supply. Lets a controller see a PSU wake up
/// within seconds of closing a relay.
pub fn start_psu_probe(secs: u64) {
    PSU_PROBE_UNTIL_MS.store(deadline_after(secs), Ordering::Relaxed);
    debug!(secs, "PSU probe window opened");
}

pub fn psu_probe_active() -> bool {
    now_ms() < PSU_PROBE_UNTIL_MS.load(Ordering::Relaxed)
}

async fn blink_leds() {
    let gpios = LED_GPIOS.get().cloned().unwrap_or_default();
    if gpios.is_empty() {
        return;
    }
    let previous: Vec<Option<String>> = gpios.iter().map(|g| prepare_gpio(*g)).collect();
    let mut on = false;
    while identify_active() {
        on = !on;
        for (i, gpio) in gpios.iter().enumerate() {
            // Alternate so the pair flashes even if one LED is out of sight.
            let lit = if i % 2 == 0 { on } else { !on };
            write_gpio(*gpio, if lit { "1" } else { "0" });
        }
        tokio::time::sleep(LED_BLINK_PERIOD).await;
    }
    for (gpio, value) in gpios.iter().zip(previous) {
        if let Some(value) = value {
            write_gpio(*gpio, value.trim());
        }
    }
}

/// Export the GPIO if needed and make it an output, keeping its current level.
/// Returns that level so it can be restored afterwards. LEDs are cosmetic, so
/// failures here are logged and ignored.
fn prepare_gpio(gpio: u32) -> Option<String> {
    let dir = format!("/sys/class/gpio/gpio{gpio}");
    if !std::path::Path::new(&dir).exists() {
        if let Err(e) = std::fs::write("/sys/class/gpio/export", gpio.to_string()) {
            debug!(gpio, error = %e, "LED GPIO export failed");
            return None;
        }
    }
    let value = std::fs::read_to_string(format!("{dir}/value")).ok();
    if let Err(e) = std::fs::write(format!("{dir}/direction"), "out") {
        debug!(gpio, error = %e, "LED GPIO direction failed");
        return None;
    }
    value
}

fn write_gpio(gpio: u32, value: &str) {
    if let Err(e) = std::fs::write(format!("/sys/class/gpio/gpio{gpio}/value"), value) {
        debug!(gpio, error = %e, "LED GPIO write failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_are_clamped_and_expire() {
        assert!(deadline_after(100_000) <= now_ms() + MAX_WINDOW_SECS * 1000);
        PSU_PROBE_UNTIL_MS.store(now_ms().saturating_sub(1), Ordering::Relaxed);
        assert!(!psu_probe_active());
        start_psu_probe(5);
        assert!(psu_probe_active());
    }
}
