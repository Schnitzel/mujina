//! OLED display support via bitcrane protocol.
//!
//! The bitcrane v3 has an optional OLED display module that can show hashrate
//! and other status information.
//!
//! Protocol:
//! - PAGE_DISPLAY = 0x0A
//! - CMD 0x10 = Set display text
//! - Data: UTF-8 string, max 32 chars
//! - If string contains a comma, text before the comma is shown in large font
//!   on top, text after in small font on bottom

use tracing::debug;

use crate::hw_trait::Result;
use crate::mgmt_protocol::ControlChannel;
use crate::mgmt_protocol::bitaxe_raw::{Packet, Page};

/// Command to set display text
const CMD_SET_DISPLAY: u8 = 0x10;

/// Maximum display string length
const MAX_DISPLAY_LEN: usize = 32;

/// OLED display controller via bitcrane protocol.
pub struct BitcraneDisplay {
    channel: ControlChannel,
}

impl BitcraneDisplay {
    /// Create a new OLED display controller.
    pub fn new(channel: ControlChannel) -> Self {
        Self { channel }
    }

    /// Display a text string on the OLED.
    ///
    /// If the string contains a comma, the text before the comma is shown
    /// in large font on the top line, and the text after in small font on the
    /// bottom line.
    ///
    /// Example: "10.5,TH/s" -> "10.5" large on top, "TH/s" small on bottom
    pub async fn display(&self, text: &str) -> Result<()> {
        // Truncate to max length
        let text = if text.len() > MAX_DISPLAY_LEN {
            &text[..MAX_DISPLAY_LEN]
        } else {
            text
        };

        debug!(text = %text, "OLED display update");

        // Build packet with text data
        let data: Vec<u8> = text.bytes().collect();
        let packet = Packet::new(0xAB, Page::Display, CMD_SET_DISPLAY, data);

        self.channel.send_packet(packet).await?;
        Ok(())
    }

    /// Display hashrate on the OLED.
    ///
    /// Formats the hashrate appropriately based on magnitude:
    /// - GH/s for values < 1000 GH/s
    /// - TH/s for values >= 1000 GH/s (1 TH/s)
    /// - PH/s for values >= 1000 TH/s (1 PH/s)
    pub async fn display_hashrate(&self, hashrate_gh: f64) -> Result<()> {
        let text = if hashrate_gh < 1000.0 {
            format!("{:.1},GH/s", hashrate_gh)
        } else if hashrate_gh < 1_000_000.0 {
            format!("{:.2},TH/s", hashrate_gh / 1000.0)
        } else {
            format!("{:.3},PH/s", hashrate_gh / 1_000_000.0)
        };
        self.display(&text).await
    }
}
