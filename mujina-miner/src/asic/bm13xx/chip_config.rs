//! Chip configuration for BM13xx ASIC chips.
//!
//! All BM13xx chip models share the same configurable fields; only the
//! values differ. Use [`bm1362()`], [`bm1366()`], or [`bm1370()`] to get
//! appropriate defaults, then modify fields as needed.
//!
//! # PLL Calculation
//!
//! PLL configuration is handled by [`ChipConfig::calculate_pll()`].
//! The chip-family-specific constraints live in [`PllParams`] on the
//! config:
//!
//! - **BM1362 / BM1370**: FBDIV 0xA0–0xEF (160–239), `postdiv1 ≥ postdiv2`.
//!   Verified by S19j Pro (BM1362) and S21 Pro (BM1370) serial captures.
//! - **BM1366 / BM1368**: FBDIV 0x90–0xEB (144–235), strict
//!   `postdiv1 > postdiv2`. Verified by bitaxeorg/ESP-Miner
//!   `components/asic/bm1366.c` (`pll_get_parameters(... 144, 235, ...)`).
//!
//! The VCO threshold is identical across families: `flag = 0x50` when
//! `VCO >= 2400 MHz`, else `0x40`.
//!
//! # Per-family register values
//!
//! BM1362 and BM1366 use the same set of init *register addresses*
//! (0xA8, 0x18, 0x3C, 0x54, 0x58, 0x2C, 0xA8) but different *raw values*
//! at each address. The bytes are kept on [`ChainInitRegs`] inside the
//! config so the [`crate::asic::bm13xx::sequencer::Sequencer`] can stay
//! chip-family-agnostic.
//!
//! Sources for the BM1366 values: byte-for-byte from ESP-Miner BM1366.c
//! (init4, init5, init135, init136, init138, init139, init171, and the
//! per-chip writes from the `for each chip` loop).

use super::protocol::{Frequency, IoDriverStrength, PllConfig};

/// PLL search-space constraints that differ between BM13xx chip
/// families.
#[derive(Debug, Clone, Copy)]
pub struct PllParams {
    /// Minimum allowable feedback divider (inclusive).
    pub fbdiv_min: u8,
    /// Maximum allowable feedback divider (inclusive).
    pub fbdiv_max: u8,
    /// When `true`, `postdiv1 > postdiv2` strictly; when `false`,
    /// `postdiv1 >= postdiv2` is accepted (BM1362 / BM1370 behaviour).
    pub postdiv_strict: bool,
}

impl PllParams {
    /// Default PLL constraints for BM1362 / BM1370. Verified from S19j
    /// Pro and S21 Pro serial captures.
    pub const BM1362_BM1370: PllParams = PllParams {
        fbdiv_min: 0xA0,
        fbdiv_max: 0xEF,
        postdiv_strict: false,
    };

    /// PLL constraints for BM1366 / BM1368 per ESP-Miner BM1366.c
    /// (`pll_get_parameters(target, 144, 235, ...)`). BM1366 additionally
    /// requires strict `postdiv1 > postdiv2`.
    pub const BM1366_BM1368: PllParams = PllParams {
        fbdiv_min: 0x90,
        fbdiv_max: 0xEB,
        postdiv_strict: true,
    };
}

/// Per-chip-family init-register raw values, encoded as `u32` written to
/// the BM13xx address space (the protocol's `to_le_bytes()` placement
/// produces the same wire bytes as ESP-Miner's hand-rolled arrays).
///
/// Fields that are `Option<_>` represent register writes that some chip
/// families need but others skip entirely. Skipping is encoded as
/// `None` so the [`Sequencer`](crate::asic::bm13xx::sequencer::Sequencer)
/// doesn't have to know which family is current.

/// Per-domain-boundary UartRelay (reg 0x2C) configuration.
///
/// On the BHB56902 (S19k Pro) hashboard, the stock Bitmain / LuxOS init
/// writes reg 0x2C only to the first and last chip of each voltage domain,
/// with a value that varies per domain. Captured wire bytes (4 bytes per
/// chip):
///
/// ```text
///   wire bytes = [byte0, byte1, byte2, byte3]
///   byte1      = high_base - domain_idx * high_step
///   byte0, byte2, byte3 are fixed.
/// ```
///
/// For BHB56902: `byte0 = 0x00`, `byte1` ranges `0x5b → 0x15` (step 0x07
/// per domain, 11 domains), `byte2 = 0x00`, `byte3 = 0x03`. The resulting
/// LE u32 for domain 0 is `0x03005B00`; for domain 10 it is `0x03001500`.
#[derive(Debug, Clone, Copy)]
pub struct UartRelayPerDomain {
    /// Byte 0 of the wire payload (lowest-address byte in the LE u32).
    pub byte0: u8,
    /// `byte1` value written to domain 0 (closest to the controlboard).
    pub byte1_base: u8,
    /// Amount `byte1` decreases per downstream domain.
    pub byte1_step: u8,
    /// Byte 2 of the wire payload.
    pub byte2: u8,
    /// Byte 3 of the wire payload (highest-address byte in the LE u32).
    pub byte3: u8,
}

impl UartRelayPerDomain {
    /// Compute the u32 value to write for the domain at `domain_idx`
    /// (0 = closest to the host). Wire bytes are `value.to_le_bytes()`.
    pub fn value_for(&self, domain_idx: usize) -> u32 {
        let b1 = self
            .byte1_base
            .wrapping_sub((domain_idx as u8).wrapping_mul(self.byte1_step));
        u32::from_le_bytes([self.byte0, b1, self.byte2, self.byte3])
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ChainInitRegs {
    // ---- enumeration phase (broadcast) ----
    /// Reg 0xA8 (InitControl), broadcast before `ChainInactive`.
    /// Wire bytes for BM1362 are `00 00 00 00`, for BM1366 `00 07 00 00`.
    pub init_control_broadcast: u32,
    /// Reg 0x18 (MiscControl), broadcast before `ChainInactive`.
    /// Wire bytes for BM1362 are `B0 00 C1 00`, for BM1366 `FF 0F C1 00`.
    pub misc_control_broadcast: u32,
    // ---- reg config (broadcast) ----
    /// Reg 0x3C (Core) broadcast writes that enable the hashing cores.
    /// BM1362 writes two values; BM1366 also two but different bytes.
    pub core_broadcast: [u32; 2],
    /// Reg 0x54 (AnalogMux) broadcast configuration.
    pub analog_mux_broadcast: u32,
    /// Reg 0x58 (IoDriverStrength) broadcast — BM1366 sets a chip-
    /// specific raw pattern (`02 11 11 11`) here. BM1362 reads the
    /// per-chain default from `chip_config.io_driver` separately, so
    /// this is only `Some(_)` for chip families that override it.
    pub io_driver_broadcast_raw: Option<u32>,
    // ---- reg config (per chip) ----
    /// Reg 0x2C (UartRelay) per-chip — a single value broadcast-like to
    /// every chip in the chain. Used by simple boards (Bitaxe Supra: one
    /// domain, all chips get the same value). Mutually exclusive with
    /// [`uart_relay_perdomain`].
    pub uart_relay_perchip: Option<u32>,
    /// Reg 0x2C (UartRelay) per-domain-boundary — write to the first and
    /// last chip of each voltage domain with a per-domain value. Confirmed
    /// pattern for BHB56902 (S19k Pro) via the LuxOS capture in
    /// `captures/luxos-bhb56902-findings.md`. Mutually exclusive with
    /// [`uart_relay_perchip`].
    pub uart_relay_perdomain: Option<UartRelayPerDomain>,
    /// Reg 0xA8 (InitControl) per-chip after broadcast phase.
    pub init_control_perchip: u32,
    /// Reg 0x18 (MiscControl) per-chip after broadcast phase.
    pub misc_control_perchip: u32,
    /// Reg 0x3C (Core) per-chip writes. Three values; the third has
    /// a small post-write delay (handled in the sequencer).
    pub core_perchip: [u32; 3],
}

/// Configuration for a BM13xx ASIC chip.
///
/// All chip models share the same fields. Use [`bm1362()`], [`bm1366()`],
/// or [`bm1370()`] to get appropriate defaults, then modify fields as
/// needed.
#[derive(Debug, Clone)]
pub struct ChipConfig {
    /// Chip model identifier (e.g., 0x1362, 0x1366, 0x1370).
    /// Verified during enumeration to ensure correct chip type.
    pub chip_id: u16,

    /// Minimum supported frequency for this chip model.
    pub min_freq: Frequency,

    /// Maximum supported frequency for this chip model.
    pub max_freq: Frequency,

    /// IO driver strength for signal integrity. Some chip families
    /// (BM1366) override this with a raw broadcast value in
    /// [`ChainInitRegs::io_driver_broadcast_raw`] instead.
    pub io_driver: IoDriverStrength,

    /// Nonce range configuration value (chip-family-specific).
    ///
    /// Controls how chips divide the 32-bit nonce search space.
    /// Empirically determined values differ by chip family rather than
    /// chain length.
    pub nonce_range: u32,

    /// PLL search-space constraints.
    pub pll_params: PllParams,

    /// Chip-family-specific init register values used by the sequencer.
    pub init_regs: ChainInitRegs,

    /// Optional override for the final TicketMask broadcast emitted at
    /// the end of `default_reg_config_perchip`. `Some(2)` matches the
    /// LuxOS BHB56902 capture (`reg 0x14 = 0x000000C0`, wire byte
    /// `0xC0` decodes to `zero_bits=2` in our encoding). `None` keeps
    /// the dynamically-scaled mask from the broadcast phase.
    pub post_perchip_ticket_zero_bits: Option<u8>,

    /// Target operating frequency for the chain's `execute_frequency_ramp`
    /// in MHz. `None` falls back to the historical 500 MHz hardcode.
    ///
    /// For BHB56902 (S19k Pro / BM1366) the factory ATE setpoint is
    /// 645 MHz at 13.9 V (per EEPROM `frequency_mhz` / `voltage_v`
    /// and Braiins OS's `ResolvedChainConfig`). Lower targets leave
    /// significant hashrate on the table.
    pub target_frequency_mhz: Option<f32>,
}

/// Crystal oscillator frequency for BM13xx chips (25 MHz).
const CRYSTAL_MHZ: f32 = 25.0;

impl ChipConfig {
    /// Check if a chip ID matches this configuration.
    pub fn verify_chip_id(&self, id: u16) -> bool {
        self.chip_id == id
    }

    /// Calculate optimal PLL configuration for target frequency.
    ///
    /// Searches for PLL divider values that produce the closest match
    /// to the target frequency. When multiple configs achieve the same
    /// accuracy, prefers the one with the lowest VCO frequency to stay
    /// in the VCO's optimal range (~2000–2300 MHz). The FBDIV search
    /// range and the postdiv constraint come from
    /// [`ChipConfig::pll_params`] so BM1366 uses 0x90–0xEB with strict
    /// `>`, while BM1362 / BM1370 use 0xA0–0xEF with `>=`.
    pub fn calculate_pll(&self, freq: Frequency) -> PllConfig {
        let target_freq = freq.mhz();
        let mut best_config = PllConfig::new(self.pll_params.fbdiv_min, 2, 0x55); // Default
        let mut min_error = f32::MAX;
        let mut best_vco = f32::MAX;

        // Search for optimal PLL settings
        // ref_divider: 1 or 2
        // post_divider1: 1-7, must satisfy the family's postdiv constraint
        // post_divider2: 1-7
        // fb_divider: pll_params.fbdiv_min..=pll_params.fbdiv_max

        for ref_div in [2, 1] {
            for post_div1 in (1..=7).rev() {
                for post_div2 in (1..=7).rev() {
                    let postdiv_ok = if self.pll_params.postdiv_strict {
                        post_div1 > post_div2
                    } else {
                        post_div1 >= post_div2
                    };
                    if !postdiv_ok {
                        continue;
                    }
                    // Calculate required feedback divider
                    let fb_div_f = (post_div1 * post_div2) as f32
                        * target_freq
                        * ref_div as f32
                        / CRYSTAL_MHZ;
                    let fb_div = fb_div_f.round() as u8;

                    if (self.pll_params.fbdiv_min..=self.pll_params.fbdiv_max).contains(&fb_div) {
                        // Calculate actual frequency with these settings
                        let actual_freq = CRYSTAL_MHZ * fb_div as f32
                            / (ref_div as f32 * post_div1 as f32 * post_div2 as f32);
                        let error = (target_freq - actual_freq).abs();
                        let vco = CRYSTAL_MHZ * fb_div as f32 / ref_div as f32;

                        if error < 1.0
                            && (error < min_error || (error == min_error && vco < best_vco))
                        {
                            min_error = error;
                            best_vco = vco;
                            // Encode post dividers as per hardware format
                            let post_div = ((post_div1 - 1) << 4) | (post_div2 - 1);
                            best_config = PllConfig::new(fb_div, ref_div, post_div);
                        }
                    }
                }
            }
        }

        best_config
    }
}

/// BM1362 defaults (EmberOne, S19 J Pro).
///
/// Sources:
/// - S19 J Pro serial captures
/// - skot/bm1397-docs
/// - emberone-miner reference values for [`ChainInitRegs`].
pub fn bm1362() -> ChipConfig {
    ChipConfig {
        chip_id: 0x1362,
        min_freq: Frequency::from_mhz(50.0),
        // 560 MHz ceiling gives headroom above the 550 MHz stock operating
        // point (target_frequency_mhz below). The board-level thermal_cap
        // (s19x_amlogic.rs, 560) matches; both must clear 550 or the dial
        // clamps below the target.
        max_freq: Frequency::from_mhz(560.0),
        io_driver: IoDriverStrength::normal(),
        nonce_range: 0x8118_0000, // From emberone-miner (12 chips)
        pll_params: PllParams::BM1362_BM1370,
        init_regs: ChainInitRegs {
            // Reg 0xA8 broadcast: wire bytes `00 00 00 00` (emberone-miner)
            init_control_broadcast: 0x0000_0000,
            // Reg 0x18 broadcast: wire bytes `B0 00 C1 00`
            misc_control_broadcast: 0x00C1_00B0,
            // Reg 0x3C broadcasts: wire bytes `40 85 00 80` and `08 80 00 80`
            core_broadcast: [0x8000_8540, 0x8000_8008],
            // Reg 0x54 broadcast: wire bytes `00 00 00 03`
            analog_mux_broadcast: 0x0300_0000,
            // BM1362 leaves IoDriverStrength to the per-chain default.
            io_driver_broadcast_raw: None,
            // No reg 0x2C write on BM1362 — neither per-chip nor per-domain.
            uart_relay_perchip: None,
            uart_relay_perdomain: None,
            // Reg 0xA8 per-chip: wire bytes `00 00 00 02`
            init_control_perchip: 0x0200_0000,
            // Reg 0x18 per-chip: wire bytes `B0 00 C1 00`
            misc_control_perchip: 0x00C1_00B0,
            // Reg 0x3C per-chip: enable hashing cores
            core_perchip: [0x8000_8540, 0x8000_8008, 0x8000_82AA],
        },
        // TRIED `Some(2)` (BM1366's already-validated low-difficulty
        // override, see below) to fix hashrate_1min's swinginess by giving
        // the estimator more samples -- REVERTED. On .222 (3x S19j Pro,
        // 126 chips/chain) it roughly HALVED effective hashrate and active
        // chip census (126 -> ~67 per board), with no explicit error logged
        // (silent UART frame loss reads as "fewer successful responses,"
        // not a detected fault). Working theory: BM1366's 77-chip chain
        // tolerates this reporting rate fine, but BM1362's 126-chip chain
        // pushes materially more aggregate nonce-report traffic across the
        // SAME shared UART link at the same per-chip rate, congesting it
        // enough to drown out both the active-census ReadRegister polls and
        // real nonce responses. (My reasoning for trying it -- "more chips
        // means the per-chip rate is proportionally safer" -- had this
        // backwards: more chips at the same per-chip report rate means MORE
        // aggregate bus traffic, not less.) BM1362 boards keep the
        // dynamically-scaled TicketMask from the broadcast phase; no
        // capture-derived override yet. A safe fix likely needs a milder
        // BM1362-specific value (e.g. zero_bits=1, roughly half the extra
        // volume) validated incrementally against real chip-count/hashrate
        // held steady, not reused wholesale from a smaller chain.
        post_perchip_ticket_zero_bits: None,
        // S19j Pro stock operating frequency. LuxOS/Braiins run BHB42601 at
        // ~550 MHz / ~13.5 V for the spec ~104 TH/s / ~3068 W. The previous
        // `None` defaulted to 500 MHz (unwrap_or(500) in thread_v2), which
        // measured ~90 TH/s / ~2.76 kW on real hardware (.241, 3×BHB42601) —
        // correct efficiency but ~14% short on power/hashrate. Paired with the
        // 13.5 V floor in s19x_amlogic.rs.
        target_frequency_mhz: Some(550.0),
    }
}

/// BM1366 defaults (Antminer S19k Pro / BHB56902, Bitaxe Supra).
///
/// Sources (byte-for-byte):
/// - bitaxeorg/ESP-Miner `components/asic/bm1366.c`
///   (init4, init5, init135, init136, init138, init139, init171, and
///   the per-chip register loop).
/// - PLL constraints from `pll_get_parameters(target, 144, 235, ...)`
///   plus BM1366's additional strict `postdiv1 > postdiv2` requirement
///   documented in skot/bm1397-docs.
///
/// `nonce_range` is left at the BM1362 reference value as an initial
/// placeholder; the field is overwritten by S19k Pro board
/// initialisation once we have a serial capture from real hardware.
pub fn bm1366() -> ChipConfig {
    ChipConfig {
        chip_id: 0x1366,
        min_freq: Frequency::from_mhz(50.0),
        // 660 MHz ceiling leaves a 15 MHz pad above the BHB56902
        // factory setpoint of 645 MHz so the ramp can reach target
        // without clamping. ESP-Miner's BM1366 PLL constraint table
        // tops out at fb_div=0xEB which yields up to ~734 MHz with
        // the minimum postdiv combo (4/1), so 660 MHz is well within
        // chip-supported PLL space.
        max_freq: Frequency::from_mhz(660.0),
        io_driver: IoDriverStrength::normal(),
        nonce_range: 0x5a10_0000, // BHB56902 (S19k Pro), from LuxOS capture
                                  // wire bytes `00 00 10 5a`
        pll_params: PllParams::BM1366_BM1368,
        init_regs: ChainInitRegs {
            // Reg 0xA8 broadcast (init4): wire bytes `00 07 00 00`
            init_control_broadcast: 0x0000_0700,
            // Reg 0x18 broadcast (init5): wire bytes `FF 0F C1 00`
            misc_control_broadcast: 0x00C1_0FFF,
            // Reg 0x3C broadcasts (init135 / init136):
            //   wire bytes `80 00 85 40` and `80 00 80 20`
            core_broadcast: [0x4085_0080, 0x2080_0080],
            // Reg 0x54 broadcast (init138): wire bytes `00 00 00 03`
            analog_mux_broadcast: 0x0300_0000,
            // Reg 0x58 broadcast: wire bytes `02 11 41 11` per live LuxOS
            // capture on BHB56902 (`captures/luxos-bhb56902-chain-init.log`:
            // `55aa 5109 00 58 02114111 13`). Earlier ESP-Miner reference
            // value was `02 11 11 11`; the third nibble was wrong.
            io_driver_broadcast_raw: Some(0x1141_1102),
            // Reg 0x2C is handled per-domain-boundary on this hashboard
            // (see `uart_relay_perdomain` below). The per-chip path is left
            // for chip families / boards that genuinely broadcast a single
            // value to every chip.
            uart_relay_perchip: None,
            // Reg 0x2C per-domain-boundary, per the BHB56902 LuxOS capture.
            // LuxOS writes 22 unicast frames (11 voltage domains × first +
            // last chip per domain). The value's middle byte encodes the
            // chip-count downstream of this domain (plus a fixed offset):
            //   `high = high_base - domain_idx * high_step`
            // For BHB56902: domain 0 (closest to host) = 0x5b, domain 10
            // = 0x15, step = 0x07. Low word fixed at 0x0003.
            uart_relay_perdomain: Some(UartRelayPerDomain {
                byte0: 0x00,
                byte1_base: 0x5b,
                byte1_step: 0x07,
                byte2: 0x00,
                byte3: 0x03,
            }),
            // Reg 0xA8 per-chip: wire bytes `00 07 01 F0`
            init_control_perchip: 0xF001_0700,
            // Reg 0x18 per-chip: wire bytes `F0 00 C1 00`
            misc_control_perchip: 0x00C1_00F0,
            // Reg 0x3C per-chip: same three values as the broadcasts
            // plus the BM1362-style finalizer 0x82AA.
            //   `80 00 85 40`, `80 00 80 20`, `80 00 82 AA`
            core_perchip: [0x4085_0080, 0x2080_0080, 0xAA82_0080],
        },
        // LuxOS post-per-chip TicketMask = `0x000000C0` (`zero_bits=2`)
        // per the BHB56902 capture. Re-enabling now that the earlier
        // limiters are gone (13.9V chain voltage, 3.125 Mbaud, fd
        // keepalive, multi-pass verify → 77/77 chips). The prior
        // attempts that landed on 20.7 TH/s with 55/77 chips were
        // pre-voltage-fix, so the regression observed then was a
        // chip-count loss, not a TicketMask issue.
        post_perchip_ticket_zero_bits: Some(2),
        // 575 MHz matches LuxOS's actual operating point on this
        // hashboard. The EEPROM ATE setpoint is 645 MHz but LuxOS
        // auto-detunes ("k Pro adjusted board frequency: 565MHz" in
        // its log) and operates at 575 MHz, sustaining 39.15 TH/s
        // (Nominal 39.33). mujina at 575 MHz sustains 39.68 TH/s on
        // the dummy source — at parity with LuxOS per MHz (slightly
        // above, within measurement noise). 645 MHz delivered LESS
        // (38.35 TH/s) than 575 — chips appear right at the edge of
        // stability at 645 while comfortable at 575.
        target_frequency_mhz: Some(575.0),
    }
}

/// BM1370 defaults (Bitaxe Gamma, S21 Pro).
///
/// Sources:
/// - S21 Pro serial captures
/// - Bitaxe Gamma logic analyzer captures
pub fn bm1370() -> ChipConfig {
    ChipConfig {
        chip_id: 0x1370,
        min_freq: Frequency::from_mhz(50.0),
        max_freq: Frequency::from_mhz(600.0),
        io_driver: IoDriverStrength::normal(),
        nonce_range: 0xB51E_0000, // From S21 Pro captures
        pll_params: PllParams::BM1362_BM1370,
        // BM1370 shares the BM1362 init register layout (Bitaxe Gamma
        // captures vs the S19j Pro captures show identical
        // broadcast + per-chip register sequences modulo PLL bytes).
        init_regs: bm1362().init_regs,
        post_perchip_ticket_zero_bits: None,
        target_frequency_mhz: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_chip_id_matches() {
        let config = bm1362();
        assert!(config.verify_chip_id(0x1362));
        assert!(!config.verify_chip_id(0x1370));
        assert!(!config.verify_chip_id(0x1366));

        let bm1366_cfg = bm1366();
        assert!(bm1366_cfg.verify_chip_id(0x1366));
        assert!(!bm1366_cfg.verify_chip_id(0x1362));
    }

    /// Test cases from serial captures showing PLL values sent by
    /// esp-miner.
    ///
    /// Sources:
    /// - Bitaxe Gamma logic analyzer captures
    /// - bitaxeorg/esp-miner
    #[test]
    fn pll_calculation_produces_valid_frequencies() {
        // Note: esp-miner uses first-found algorithm while we find optimal settings
        // Format: (target_mhz, [fb_div, ref_div, post_div] from esp-miner)
        let test_cases = vec![
            (62.5, [0xd2, 0x02, 0x65]),  // 62.50MHz
            (75.0, [0xd2, 0x02, 0x64]),  // 75.00MHz
            (100.0, [0xe0, 0x02, 0x63]), // 100.00MHz
            (400.0, [0xe0, 0x02, 0x60]), // 400.00MHz
            (500.0, [0xa2, 0x02, 0x30]), // 500.00MHz -> esp-miner gives 506.25MHz
        ];

        let config = bm1370();

        for (target_mhz, esp_miner_raw) in test_cases {
            let freq = Frequency::from_mhz(target_mhz);
            let pll = config.calculate_pll(freq);

            // Calculate actual frequencies for both esp-miner and our values
            let esp_post_div1 = ((esp_miner_raw[2] >> 4) & 0xf) + 1;
            let esp_post_div2 = (esp_miner_raw[2] & 0xf) + 1;
            let esp_actual_mhz = 25.0 * esp_miner_raw[0] as f32
                / (esp_miner_raw[1] as f32 * esp_post_div1 as f32 * esp_post_div2 as f32);

            let our_post_div1 = ((pll.post_div >> 4) & 0xf) + 1;
            let our_post_div2 = (pll.post_div & 0xf) + 1;
            let our_actual_mhz = 25.0 * pll.fb_div as f32
                / (pll.ref_div as f32 * our_post_div1 as f32 * our_post_div2 as f32);

            // Calculate errors
            let esp_error = (target_mhz - esp_actual_mhz).abs();
            let our_error = (target_mhz - our_actual_mhz).abs();

            println!("Target: {:.2}MHz", target_mhz);
            println!(
                "  esp-miner: fb={:#04x} ref={} post={:#04x} -> {:.2}MHz (error: {:.4}MHz)",
                esp_miner_raw[0], esp_miner_raw[1], esp_miner_raw[2], esp_actual_mhz, esp_error
            );
            println!(
                "  Our calc:  fb={:#04x} ref={} post={:#04x} -> {:.2}MHz (error: {:.4}MHz)",
                pll.fb_div, pll.ref_div, pll.post_div, our_actual_mhz, our_error
            );

            // Verify our calculation produces valid PLL parameters
            assert!(
                pll.fb_div >= 0xa0 && pll.fb_div <= 0xef,
                "fb_div out of range: {:#04x}",
                pll.fb_div
            );
            assert!(
                pll.ref_div == 1 || pll.ref_div == 2,
                "ref_div invalid: {}",
                pll.ref_div
            );

            // Verify our error is reasonable (within 1MHz)
            assert!(
                our_error < 1.0,
                "Frequency error too large: {:.2}MHz for target {}MHz",
                our_error,
                target_mhz
            );

            // Our algorithm should produce equal or better results
            // Allow small tolerance for floating point comparison
            assert!(
                our_error <= esp_error + 0.01,
                "Our algorithm produced worse result than esp-miner for {}MHz",
                target_mhz
            );
        }
    }

    /// Validate VCO flag is set correctly based on VCO frequency.
    ///
    /// The S19 J Pro capture shows flag=0x40 for VCO < 2400 MHz and flag=0x50
    /// for VCO >= 2400 MHz. VCO = fb_div * 25 / ref_div.
    ///
    /// Source: S19 J Pro capture analysis
    #[test]
    fn pll_vco_flag_set_correctly() {
        let config = bm1362();

        // Test across the frequency range and verify flag matches VCO threshold
        for freq_mhz in [100.0, 200.0, 300.0, 400.0, 500.0, 525.0] {
            let pll = config.calculate_pll(Frequency::from_mhz(freq_mhz));
            let vco = pll.fb_div as f32 * 25.0 / pll.ref_div as f32;
            let expected_flag = if vco >= 2400.0 { 0x50 } else { 0x40 };

            assert_eq!(
                pll.flag, expected_flag,
                "{}MHz: VCO={:.1} should use flag=0x{:02X}, got 0x{:02X}",
                freq_mhz, vco, expected_flag, pll.flag
            );
        }
    }

    /// Verify BM1362 and BM1370 produce identical PLL for same frequency.
    ///
    /// Empirically confirmed from S19 J Pro (BM1362) and S21 Pro (BM1370)
    /// captures which show identical frequency ramp sequences.
    #[test]
    fn bm1362_and_bm1370_pll_identical() {
        let bm1362_config = bm1362();
        let bm1370_config = bm1370();

        // Test frequencies from the capture ramp sequence
        for freq_mhz in [100.0, 200.0, 300.0, 400.0, 500.0] {
            let freq = Frequency::from_mhz(freq_mhz);
            let pll_1362 = bm1362_config.calculate_pll(freq);
            let pll_1370 = bm1370_config.calculate_pll(freq);

            assert_eq!(
                pll_1362, pll_1370,
                "BM1362 and BM1370 should produce identical PLL for {}MHz",
                freq_mhz
            );
        }
    }

    /// BM1366 PLL output respects the family's narrower FBDIV range
    /// (0x90–0xEB) and the strict `postdiv1 > postdiv2` constraint.
    ///
    /// Source: bitaxeorg/ESP-Miner `components/asic/bm1366.c`
    /// (`pll_get_parameters(target, 144, 235, ...)`).
    #[test]
    fn bm1366_pll_respects_family_constraints() {
        let config = bm1366();

        for freq_mhz in [62.5, 100.0, 200.0, 300.0, 400.0, 500.0, 600.0] {
            let pll = config.calculate_pll(Frequency::from_mhz(freq_mhz));
            assert!(
                (0x90..=0xEB).contains(&pll.fb_div),
                "BM1366 {freq_mhz}MHz: fb_div={:#04x} out of 0x90..=0xEB range",
                pll.fb_div
            );

            // Decode encoded postdiv to verify strict `>`.
            let post_div1 = ((pll.post_div >> 4) & 0xF) + 1;
            let post_div2 = (pll.post_div & 0xF) + 1;
            assert!(
                post_div1 > post_div2,
                "BM1366 {freq_mhz}MHz: postdiv1={post_div1} not strictly > postdiv2={post_div2}"
            );
        }
    }

    /// Quick sanity check on the BM1366 init register raw values: each
    /// LE encoding produces the exact wire bytes documented in
    /// ESP-Miner.
    #[test]
    fn bm1366_init_regs_match_esp_miner_wire_bytes() {
        let regs = bm1366().init_regs;
        // (raw_value, expected wire bytes, label)
        let cases: &[(u32, [u8; 4], &str)] = &[
            (regs.init_control_broadcast, [0x00, 0x07, 0x00, 0x00], "reg 0xA8 bcast"),
            (regs.misc_control_broadcast, [0xFF, 0x0F, 0xC1, 0x00], "reg 0x18 bcast"),
            (regs.core_broadcast[0], [0x80, 0x00, 0x85, 0x40], "reg 0x3C bcast #1"),
            (regs.core_broadcast[1], [0x80, 0x00, 0x80, 0x20], "reg 0x3C bcast #2"),
            (regs.analog_mux_broadcast, [0x00, 0x00, 0x00, 0x03], "reg 0x54 bcast"),
            (regs.io_driver_broadcast_raw.unwrap(), [0x02, 0x11, 0x41, 0x11], "reg 0x58 bcast"),
            (regs.init_control_perchip, [0x00, 0x07, 0x01, 0xF0], "reg 0xA8 per-chip"),
            (regs.misc_control_perchip, [0xF0, 0x00, 0xC1, 0x00], "reg 0x18 per-chip"),
            (regs.core_perchip[0], [0x80, 0x00, 0x85, 0x40], "reg 0x3C per-chip #1"),
            (regs.core_perchip[1], [0x80, 0x00, 0x80, 0x20], "reg 0x3C per-chip #2"),
            (regs.core_perchip[2], [0x80, 0x00, 0x82, 0xAA], "reg 0x3C per-chip #3"),
        ];
        for (raw, expected, label) in cases {
            assert_eq!(
                raw.to_le_bytes(),
                *expected,
                "{label}: raw 0x{raw:08X} → {:02X?}, expected {:02X?}",
                raw.to_le_bytes(),
                expected
            );
        }
    }

    /// Per-domain UartRelay values for BHB56902 must match the LuxOS
    /// capture: 11 domains, wire bytes `00 X 00 03` with `X` stepping
    /// 0x07 from 0x5b (domain 0) down to 0x15 (domain 10).
    #[test]
    fn bm1366_uart_relay_perdomain_matches_luxos_capture() {
        let spec = bm1366()
            .init_regs
            .uart_relay_perdomain
            .expect("BM1366 should have per-domain UartRelay configured");
        let expected_byte1: [u8; 11] = [
            0x5b, 0x54, 0x4d, 0x46, 0x3f, 0x38, 0x31, 0x2a, 0x23, 0x1c, 0x15,
        ];
        for (idx, b1) in expected_byte1.iter().enumerate() {
            let wire = spec.value_for(idx).to_le_bytes();
            assert_eq!(
                wire,
                [0x00, *b1, 0x00, 0x03],
                "domain {idx}: wire {wire:02X?} != expected [00 {b1:02X} 00 03]"
            );
        }
    }
}
