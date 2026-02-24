//! Sequence generation for BM13xx ASIC chain initialization and operation.
//!
//! Sequences are lists of protocol commands with timing information. The
//! sequence itself is pure data---it doesn't handle errors, retries, or
//! execution logic. The executor (hash thread implementation) is responsible
//! for sending commands, handling timeouts, and verifying responses.
//!
//! # Example
//!
//! ```ignore
//! let chip_config = bm13xx::bm1370();
//! let sequencer = Sequencer::new(chip_config);
//!
//! let topology = TopologySpec::single_domain(1);
//! let mut chain = Chain::from_topology(&topology);
//! chain.assign_addresses().unwrap();
//!
//! let steps = sequencer.build_enumeration(&chain);
//! for step in steps {
//!     protocol.send(&step.command)?;
//!     if let Some(delay) = step.wait_after {
//!         tokio::time::sleep(delay).await;
//!     }
//! }
//! ```

use std::time::Duration;

use super::chain::Chain;
use super::chip_config::ChipConfig;
use super::protocol::{
    Command, Frequency, Hashrate, IoDriverStrength, NonceRangeConfig, Register, ReportingInterval,
    ReportingRate, TicketMask, VersionMask,
};

/// A single step in a command sequence.
///
/// Wraps `protocol::Command` with timing information. The command is executed,
/// then the executor waits `wait_after` before proceeding to the next step.
#[derive(Debug, Clone)]
pub struct Step {
    pub command: Command,
    pub wait_after: Option<Duration>,
}

impl Step {
    /// Create a step with no delay.
    pub fn new(command: Command) -> Self {
        Self {
            command,
            wait_after: None,
        }
    }

    /// Create a step with a delay after execution.
    pub fn with_delay(command: Command, delay: Duration) -> Self {
        Self {
            command,
            wait_after: Some(delay),
        }
    }
}

/// Custom sequence generator function type.
type SequenceFn = Box<dyn Fn(&Chain, &ChipConfig) -> Vec<Step> + Send + Sync>;

/// Sequence generator for BM13xx chain operations.
///
/// The sequencer holds chip configuration and provides methods to build
/// command sequences for various phases of operation. Build methods receive
/// `&Chain` to access chip addresses and topology.
///
/// # Customization
///
/// Most boards use default sequences with modified `ChipConfig` values.
/// For unusual boards needing completely different command sequences,
/// override closures can replace the default logic.
pub struct Sequencer {
    chip_config: ChipConfig,
    enumeration_fn: Option<SequenceFn>,
    domain_config_fn: Option<SequenceFn>,
    reg_config_fn: Option<SequenceFn>,
}

impl Sequencer {
    /// Create a sequencer with chip configuration.
    pub fn new(chip_config: ChipConfig) -> Self {
        Self {
            chip_config,
            enumeration_fn: None,
            domain_config_fn: None,
            reg_config_fn: None,
        }
    }

    /// Override the enumeration sequence generator.
    pub fn with_enumeration(
        mut self,
        f: impl Fn(&Chain, &ChipConfig) -> Vec<Step> + Send + Sync + 'static,
    ) -> Self {
        self.enumeration_fn = Some(Box::new(f));
        self
    }

    /// Override the domain configuration sequence generator.
    pub fn with_domain_config(
        mut self,
        f: impl Fn(&Chain, &ChipConfig) -> Vec<Step> + Send + Sync + 'static,
    ) -> Self {
        self.domain_config_fn = Some(Box::new(f));
        self
    }

    /// Override the register configuration sequence generator.
    pub fn with_reg_config(
        mut self,
        f: impl Fn(&Chain, &ChipConfig) -> Vec<Step> + Send + Sync + 'static,
    ) -> Self {
        self.reg_config_fn = Some(Box::new(f));
        self
    }

    /// Access the chip configuration.
    pub fn chip_config(&self) -> &ChipConfig {
        &self.chip_config
    }

    // --- Initialization phases ---

    /// Build enumeration sequence (ChainInactive, SetChipAddress, etc.)
    ///
    /// The executor counts responses to verify expected chip count.
    pub fn build_enumeration(&self, chain: &Chain) -> Vec<Step> {
        if let Some(f) = &self.enumeration_fn {
            f(chain, &self.chip_config)
        } else {
            self.default_enumeration(chain)
        }
    }

    /// Build domain configuration sequence (IO driver, UART relay).
    ///
    /// Only needed for boards with `TopologySpec.needs_domain_config() == true`.
    pub fn build_domain_config(&self, chain: &Chain) -> Vec<Step> {
        if let Some(f) = &self.domain_config_fn {
            f(chain, &self.chip_config)
        } else {
            self.default_domain_config(chain)
        }
    }

    /// Build per-chip register configuration sequence.
    pub fn build_reg_config(&self, chain: &Chain) -> Vec<Step> {
        if let Some(f) = &self.reg_config_fn {
            f(chain, &self.chip_config)
        } else {
            self.default_reg_config(chain)
        }
    }

    /// Build broadcast-only register configuration (Phase 1).
    ///
    /// This should be called before frequency ramp. Contains Core broadcast
    /// writes, TicketMask, AnalogMux, IoDriverStrength, and initial PLL.
    /// TicketMask difficulty is scaled by chip count to maintain ~1 nonce/sec.
    pub fn build_reg_config_broadcast(&self, chain: &Chain) -> Vec<Step> {
        self.default_reg_config_broadcast(chain)
    }

    /// Build per-chip register configuration (Phase 2).
    ///
    /// This should be called AFTER frequency ramp. Contains per-chip
    /// InitControl, MiscControl, and Core writes with 500ms delays.
    pub fn build_reg_config_perchip(&self, chain: &Chain) -> Vec<Step> {
        self.default_reg_config_perchip(chain)
    }

    /// Build frequency ramp sequence from initial PLL frequency to target.
    ///
    /// Ramps in 6.25 MHz steps with 10ms delay between each step for PLL
    /// lock. The initial frequency is ~56.25 MHz (set during register
    /// configuration).
    ///
    /// Returns `(Frequency, Step)` pairs so the caller can compute the
    /// matching voltage for each step when coordinating a voltage-frequency
    /// ramp.
    ///
    /// The target frequency is clamped to `chip_config.max_freq`.
    pub fn build_frequency_ramp(&self, target: Frequency) -> Vec<(Frequency, Step)> {
        const INITIAL_FREQ_MHZ: f32 = 56.25;
        const STEP_MHZ: f32 = 6.25;
        const STEP_DELAY: Duration = Duration::from_millis(100);

        let mut steps = vec![];

        // Clamp target to chip's max frequency
        let target_mhz = target.mhz().min(self.chip_config.max_freq.mhz());

        // Skip if already at or below initial frequency
        if target_mhz <= INITIAL_FREQ_MHZ {
            return steps;
        }

        // Start with initial frequency (emberone-miner does this explicitly)
        let initial_freq = Frequency::from_mhz(INITIAL_FREQ_MHZ);
        let initial_pll = self.chip_config.calculate_pll(initial_freq);
        steps.push((
            initial_freq,
            Step::with_delay(
                Command::WriteRegister {
                    broadcast: true,
                    chip_address: 0x00,
                    register: Register::PllDivider(initial_pll),
                },
                STEP_DELAY,
            ),
        ));

        // Step from initial to target in 6.25 MHz increments
        let mut current_mhz = INITIAL_FREQ_MHZ + STEP_MHZ;
        while current_mhz < target_mhz {
            let freq = Frequency::from_mhz(current_mhz);
            let pll = self.chip_config.calculate_pll(freq);
            steps.push((
                freq,
                Step::with_delay(
                    Command::WriteRegister {
                        broadcast: true,
                        chip_address: 0x00,
                        register: Register::PllDivider(pll),
                    },
                    STEP_DELAY,
                ),
            ));
            current_mhz += STEP_MHZ;
        }

        // Final step: set exact target frequency
        let final_freq = Frequency::from_mhz(target_mhz);
        let pll = self.chip_config.calculate_pll(final_freq);
        steps.push((
            final_freq,
            Step::with_delay(
                Command::WriteRegister {
                    broadcast: true,
                    chip_address: 0x00,
                    register: Register::PllDivider(pll),
                },
                STEP_DELAY,
            ),
        ));

        steps
    }

    // --- Default sequence implementations ---

    fn default_enumeration(&self, chain: &Chain) -> Vec<Step> {
        let mut steps = vec![];

        // Send VersionMask to enable version rolling and configure response format.
        // BM13xx chips need this before they respond with proper 11-byte frames.
        // Send 3 times with delays to ensure all chips receive it (matches Bitaxe).
        for _ in 0..3 {
            steps.push(Step::with_delay(
                Command::WriteRegister {
                    broadcast: true,
                    chip_address: 0x00,
                    register: Register::VersionMask(VersionMask::full_rolling()),
                },
                Duration::from_millis(5),
            ));
        }

        // InitControl (0xA8) = 0x00 broadcast - prepare chips for enumeration
        // emberone-miner does this before ChainInactive. The value 0x02 comes later
        // in per-chip configuration.
        steps.push(Step::new(Command::WriteRegister {
            broadcast: true,
            chip_address: 0x00,
            register: Register::InitControl {
                raw_value: 0x0000_0000,
            },
        }));

        // MiscControl (0x18) broadcast - enables clock and core functionality
        // Wire bytes: B0 00 C1 00
        steps.push(Step::new(Command::WriteRegister {
            broadcast: true,
            chip_address: 0x00,
            register: Register::MiscControl {
                raw_value: 0x00C1_00B0,
            },
        }));

        steps.push(Step::with_delay(
            Command::ChainInactive,
            Duration::from_millis(10),
        ));

        // SetChipAddress for each chip
        for (_, chip) in chain.chips() {
            steps.push(Step::with_delay(
                Command::SetChipAddress {
                    chip_address: chip.address,
                },
                Duration::from_micros(100),
            ));
        }

        steps
    }

    fn default_domain_config(&self, chain: &Chain) -> Vec<Step> {
        let mut steps = vec![];

        // Configure IO driver strength on last chip of each domain
        for domain in chain.domains() {
            let last_chip_id = chain.domain_last(domain.id);
            let last_chip = chain.chip(last_chip_id);

            steps.push(Step::new(Command::WriteRegister {
                broadcast: false,
                chip_address: last_chip.address,
                register: Register::IoDriverStrength(IoDriverStrength::domain_boundary()),
            }));
        }

        steps
    }

    fn default_reg_config(&self, chain: &Chain) -> Vec<Step> {
        let mut steps = vec![];

        // Phase 1: Broadcast configuration
        // Core configuration - first two writes are broadcast
        // BM1362 values from emberone-miner reference
        steps.push(Step::new(Command::WriteRegister {
            broadcast: true,
            chip_address: 0x00,
            register: Register::Core {
                raw_value: 0x8000_8540,
            },
        }));
        steps.push(Step::new(Command::WriteRegister {
            broadcast: true,
            chip_address: 0x00,
            register: Register::Core {
                raw_value: 0x8000_8008,
            },
        }));

        // TicketMask controls which nonces chips report.
        // Scale hashrate by chip count to maintain ~1 nonce/sec across the chain.
        // Base: ~83 GH/s per chip (1 TH/s for 12 chips)
        let hashrate_gh = 83.0 * chain.chip_count() as f64;
        let reporting_interval = ReportingInterval::from_rate(
            Hashrate::gibihashes_per_sec(hashrate_gh),
            ReportingRate::nonces_per_sec(1.0),
        );
        steps.push(Step::new(Command::WriteRegister {
            broadcast: true,
            chip_address: 0x00,
            register: Register::TicketMask(TicketMask::new(reporting_interval)),
        }));

        // AnalogMux (0x54) configuration from emberone-miner
        // Wire bytes: 00 00 00 03
        steps.push(Step::new(Command::WriteRegister {
            broadcast: true,
            chip_address: 0x00,
            register: Register::AnalogMux {
                raw_value: 0x0300_0000,
            },
        }));

        // Broadcast IO driver strength to all chips
        steps.push(Step::new(Command::WriteRegister {
            broadcast: true,
            chip_address: 0x00,
            register: Register::IoDriverStrength(self.chip_config.io_driver),
        }));

        // Basic PLL configuration at ~56 MHz (emberone-miner's starting frequency)
        // For 56.38 MHz: fb_div=221 (0xDD), ref_div=2, postdiv1=7, postdiv2=7
        // postdiv_encoded = ((7-1)<<4) | (7-1) = 0x66
        use super::protocol::PllConfig;
        steps.push(Step::new(Command::WriteRegister {
            broadcast: true,
            chip_address: 0x00,
            register: Register::PllDivider(PllConfig::new(0xDD, 2, 0x66)),
        }));

        // Phase 2: Per-chip configuration with delays
        // InitControl, MiscControl, and Core x3 per-chip with brief delay
        // to allow cores to stabilize before moving to next chip.
        for (_, chip) in chain.chips() {
            // InitControl (0xA8) = 0x02 per-chip
            // Wire bytes: 00 00 00 02
            steps.push(Step::new(Command::WriteRegister {
                broadcast: false,
                chip_address: chip.address,
                register: Register::InitControl {
                    raw_value: 0x0200_0000,
                },
            }));

            // MiscControl (0x18) per-chip
            // Wire bytes: B0 00 C1 00
            steps.push(Step::new(Command::WriteRegister {
                broadcast: false,
                chip_address: chip.address,
                register: Register::MiscControl {
                    raw_value: 0x00C1_00B0,
                },
            }));

            // Core (0x3C) x3 per-chip - enables hashing cores
            steps.push(Step::new(Command::WriteRegister {
                broadcast: false,
                chip_address: chip.address,
                register: Register::Core {
                    raw_value: 0x8000_8540,
                },
            }));
            steps.push(Step::new(Command::WriteRegister {
                broadcast: false,
                chip_address: chip.address,
                register: Register::Core {
                    raw_value: 0x8000_8008,
                },
            }));
            // Third Core write - critical for enabling actual hashing
            steps.push(Step::with_delay(
                Command::WriteRegister {
                    broadcast: false,
                    chip_address: chip.address,
                    register: Register::Core {
                        raw_value: 0x8000_82AA,
                    },
                },
                Duration::from_millis(50), // Brief delay for core stabilization
            ));
        }

        // Phase 3: Final broadcast configuration
        // NonceRange enables actual hashing (HCN - Hash Control Number)
        steps.push(Step::new(Command::WriteRegister {
            broadcast: true,
            chip_address: 0x00,
            register: Register::NonceRange(NonceRangeConfig::from_raw(
                self.chip_config.nonce_range,
            )),
        }));

        // Final VersionMask to confirm version rolling is enabled
        steps.push(Step::new(Command::WriteRegister {
            broadcast: true,
            chip_address: 0x00,
            register: Register::VersionMask(VersionMask::full_rolling()),
        }));

        steps
    }

    /// Broadcast-only register configuration (Phase 1).
    ///
    /// Called before frequency ramp. Sets up Core broadcast writes, TicketMask,
    /// AnalogMux, IoDriverStrength, and initial PLL at ~56 MHz.
    fn default_reg_config_broadcast(&self, chain: &Chain) -> Vec<Step> {
        let mut steps = vec![];

        // Core configuration - first two writes are broadcast
        steps.push(Step::new(Command::WriteRegister {
            broadcast: true,
            chip_address: 0x00,
            register: Register::Core {
                raw_value: 0x8000_8540,
            },
        }));
        steps.push(Step::new(Command::WriteRegister {
            broadcast: true,
            chip_address: 0x00,
            register: Register::Core {
                raw_value: 0x8000_8008,
            },
        }));

        // TicketMask controls which nonces chips report.
        // Scale hashrate by chip count to maintain ~1 nonce/sec across the chain.
        // Base: ~83 GH/s per chip (1 TH/s for 12 chips)
        let hashrate_gh = 83.0 * chain.chip_count() as f64;
        let reporting_interval = ReportingInterval::from_rate(
            Hashrate::gibihashes_per_sec(hashrate_gh),
            ReportingRate::nonces_per_sec(1.0),
        );
        steps.push(Step::new(Command::WriteRegister {
            broadcast: true,
            chip_address: 0x00,
            register: Register::TicketMask(TicketMask::new(reporting_interval)),
        }));

        // AnalogMux (0x54)
        steps.push(Step::new(Command::WriteRegister {
            broadcast: true,
            chip_address: 0x00,
            register: Register::AnalogMux {
                raw_value: 0x0300_0000,
            },
        }));

        // IO driver strength
        steps.push(Step::new(Command::WriteRegister {
            broadcast: true,
            chip_address: 0x00,
            register: Register::IoDriverStrength(self.chip_config.io_driver),
        }));

        // NOTE: No PLL write here - emberone-miner sets initial PLL at start
        // of frequency ramp, not in broadcast config.

        steps
    }

    /// Per-chip register configuration (Phase 2).
    ///
    /// Called AFTER frequency ramp. Per-chip InitControl, MiscControl, and
    /// Core writes with brief delays enable the hashing cores at the target
    /// frequency.
    fn default_reg_config_perchip(&self, chain: &Chain) -> Vec<Step> {
        let mut steps = vec![];

        for (_, chip) in chain.chips() {
            // InitControl (0xA8) = 0x02 per-chip
            steps.push(Step::new(Command::WriteRegister {
                broadcast: false,
                chip_address: chip.address,
                register: Register::InitControl {
                    raw_value: 0x0200_0000,
                },
            }));

            // MiscControl (0x18) per-chip
            steps.push(Step::new(Command::WriteRegister {
                broadcast: false,
                chip_address: chip.address,
                register: Register::MiscControl {
                    raw_value: 0x00C1_00B0,
                },
            }));

            // Core (0x3C) x3 per-chip - enables hashing cores
            steps.push(Step::new(Command::WriteRegister {
                broadcast: false,
                chip_address: chip.address,
                register: Register::Core {
                    raw_value: 0x8000_8540,
                },
            }));
            steps.push(Step::new(Command::WriteRegister {
                broadcast: false,
                chip_address: chip.address,
                register: Register::Core {
                    raw_value: 0x8000_8008,
                },
            }));
            // Third Core write - critical for enabling actual hashing
            steps.push(Step::with_delay(
                Command::WriteRegister {
                    broadcast: false,
                    chip_address: chip.address,
                    register: Register::Core {
                        raw_value: 0x8000_82AA,
                    },
                },
                Duration::from_millis(50),
            ));
        }

        // Final broadcast: NonceRange and VersionMask
        steps.push(Step::new(Command::WriteRegister {
            broadcast: true,
            chip_address: 0x00,
            register: Register::NonceRange(NonceRangeConfig::from_raw(
                self.chip_config.nonce_range,
            )),
        }));

        steps.push(Step::new(Command::WriteRegister {
            broadcast: true,
            chip_address: 0x00,
            register: Register::VersionMask(VersionMask::full_rolling()),
        }));

        steps
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asic::bm13xx::chain::ChipId;
    use crate::asic::bm13xx::chip_config::bm1370;
    use crate::asic::bm13xx::topology::TopologySpec;

    mod fixtures {
        /// Antminer S21 Pro: 65 BM1370 chips in 13 voltage domains.
        pub mod s21_pro {
            pub const CHIP_COUNT: usize = 65;
            pub const DOMAIN_COUNT: usize = 13;
            pub const CHIPS_PER_DOMAIN: usize = 5;
        }
    }

    #[test]
    fn enumeration_addresses_match_chain_model() {
        use fixtures::s21_pro::*;

        let chip_config = bm1370();
        let sequencer = Sequencer::new(chip_config);

        let topology = TopologySpec::uniform_domains(DOMAIN_COUNT, CHIPS_PER_DOMAIN, true);
        let mut chain = Chain::from_topology(&topology);
        chain.assign_addresses().unwrap();

        let steps = sequencer.build_enumeration(&chain);

        // Extract SetChipAddress commands and verify addresses match chain model
        let address_commands: Vec<_> = steps
            .iter()
            .filter_map(|step| match step.command {
                Command::SetChipAddress { chip_address } => Some(chip_address),
                _ => None,
            })
            .collect();

        assert_eq!(address_commands.len(), CHIP_COUNT);
        for (i, &chip_address) in address_commands.iter().enumerate() {
            assert_eq!(
                chip_address,
                chain.chip(ChipId(i)).address,
                "Address mismatch at chip {}",
                i
            );
        }
    }

    #[test]
    fn domain_config_targets_last_chip_of_each_domain() {
        use fixtures::s21_pro::*;

        let chip_config = bm1370();
        let sequencer = Sequencer::new(chip_config);

        let topology = TopologySpec::uniform_domains(DOMAIN_COUNT, CHIPS_PER_DOMAIN, true);
        let mut chain = Chain::from_topology(&topology);
        chain.assign_addresses().unwrap();

        let steps = sequencer.build_domain_config(&chain);

        assert_eq!(steps.len(), DOMAIN_COUNT);

        // Verify each step targets the last chip of its domain
        for (domain_idx, step) in steps.iter().enumerate() {
            if let Command::WriteRegister {
                chip_address,
                register: Register::IoDriverStrength(_),
                ..
            } = &step.command
            {
                let expected_chip_idx = (domain_idx + 1) * CHIPS_PER_DOMAIN - 1;
                let expected_address = (expected_chip_idx * 2) as u8;
                assert_eq!(
                    *chip_address, expected_address,
                    "Domain {} end chip address mismatch",
                    domain_idx
                );
            } else {
                panic!("Expected WriteRegister IoDriverStrength");
            }
        }
    }

    #[test]
    fn custom_enumeration_override() {
        let chip_config = bm1370();
        let sequencer = Sequencer::new(chip_config).with_enumeration(|_chain, _cfg| {
            // Custom: just ChainInactive, no SetChipAddress
            vec![Step::new(Command::ChainInactive)]
        });

        let topology = TopologySpec::single_domain(5);
        let mut chain = Chain::from_topology(&topology);
        chain.assign_addresses().unwrap();

        let steps = sequencer.build_enumeration(&chain);

        // Custom implementation returns only 1 step
        assert_eq!(steps.len(), 1);
    }

    #[test]
    fn frequency_ramp_generates_steps_to_target() {
        use crate::asic::bm13xx::chip_config::bm1362;

        let sequencer = Sequencer::new(bm1362());

        // Ramp to 290 MHz (half way from 56.25 to 525)
        let steps = sequencer.build_frequency_ramp(Frequency::from_mhz(290.0));

        // From 56.25 MHz to 290 MHz in 6.25 MHz steps:
        // Steps at: 56.25 (initial), 62.5, 68.75, ..., 287.5, plus final step at 290
        // That's 1 (initial) + 37 intermediate steps + 1 final = 39
        assert_eq!(steps.len(), 39, "Expected 39 frequency ramp steps");

        // Verify all steps are PLL writes with delay and have frequency paired
        for (freq, step) in &steps {
            assert!(
                freq.mhz() > 0.0,
                "Each step should have a positive frequency"
            );
            assert!(
                matches!(
                    &step.command,
                    Command::WriteRegister {
                        register: Register::PllDivider(_),
                        broadcast: true,
                        ..
                    }
                ),
                "Each step should be a broadcast PllDivider write"
            );
            assert!(step.wait_after.is_some(), "Each step should have a delay");
        }

        // First step should be at initial frequency, last at target
        assert!(
            (steps.first().unwrap().0.mhz() - 56.25).abs() < 0.01,
            "First step should be at ~56.25 MHz"
        );
        assert!(
            (steps.last().unwrap().0.mhz() - 290.0).abs() < 0.01,
            "Last step should be at ~290 MHz"
        );
    }

    #[test]
    fn frequency_ramp_empty_at_or_below_initial() {
        use crate::asic::bm13xx::chip_config::bm1362;

        let sequencer = Sequencer::new(bm1362());

        // Target at initial frequency: no ramp needed
        let steps = sequencer.build_frequency_ramp(Frequency::from_mhz(56.25));
        assert!(steps.is_empty(), "Should skip ramp at initial frequency");

        // Target below initial: no ramp needed
        let steps = sequencer.build_frequency_ramp(Frequency::from_mhz(50.0));
        assert!(steps.is_empty(), "Should skip ramp below initial frequency");
    }

    #[test]
    fn frequency_ramp_clamped_to_max_freq() {
        use crate::asic::bm13xx::chip_config::bm1362;

        let sequencer = Sequencer::new(bm1362());

        // Request 600 MHz (above max of 525 MHz)
        let steps_600 = sequencer.build_frequency_ramp(Frequency::from_mhz(600.0));

        // Request exactly 525 MHz
        let steps_525 = sequencer.build_frequency_ramp(Frequency::from_mhz(525.0));

        // Both should produce the same number of steps (clamped to max)
        assert_eq!(
            steps_600.len(),
            steps_525.len(),
            "Requesting above max should clamp to max"
        );

        // Last frequency in both should be 525 MHz (the max)
        assert!(
            (steps_600.last().unwrap().0.mhz() - 525.0).abs() < 0.01,
            "Clamped ramp should end at max frequency"
        );
    }
}
