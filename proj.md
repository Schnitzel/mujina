# Mujina S19j Pro support on Antminer Amlogic control board

Project to add native S19j Pro support to Mujina running directly on an Antminer Amlogic control board.

## Current assessment

This project direction makes sense, but the original outline was missing a few important architecture steps.

What already exists:

- `amlogic-cb-tools` proves out the low-level Linux interfaces for the control board:
    - 3 hashboard serial ports
    - hashboard EEPROM and TMP75 temperature access over I2C
    - APW12 PSU control on a separate I2C bus
    - fan PWM + tach support
- Mujina already has the mining pipeline needed above the hardware layer:
    - stratum client
    - work generation and scheduling
    - BM1362 chain initialization
    - ASIC response handling
    - share submission
- Mujina already has a working S19j Pro implementation through Bitcrane v3, which is a strong reference design for chain topology, reset sequencing, telemetry, and board state updates.

What is still missing:

- a native Amlogic-backed board implementation inside Mujina
- a way to instantiate that board without relying on USB discovery
- multi-hashboard orchestration for the 3 onboard serial channels
- shared control of PSU, fans, and board-level telemetry
- bring-up, tuning, and fault handling for the full miner

## Recommended architecture

Treat the Amlogic control board as **one Mujina board with three hash threads**, not as three separate boards.

Why this is the best fit:

- the control board owns shared peripherals: PSU, fans, LEDs, presence detect, and likely safety policy
- Mujina `Board` implementations already support returning multiple `HashThread`s
- `BoardState` already has `threads`, `fans`, `temperatures`, and `powers`, so one board can expose the whole miner cleanly
- this avoids awkward sharing of PSU and fan control across three separate board instances

## Main design decisions

1. Split the current S19j Pro implementation into:
     - `s19j_pro_bitcrane.rs` for the existing USB/Bitcrane path
     - `s19j_pro_amlogic.rs` for the native Linux/Amlogic path
2. Extract the S19j Pro common logic where practical:
     - chain topology
     - BM1362 chip config
     - common safety timings
     - reusable telemetry/state helpers
3. Add an Amlogic-specific startup path in Mujina.
     - The current backplane only reacts to USB and CPU events.
     - The Amlogic control board is local hardware, so it likely needs either:
         - a new transport event type, or
         - a virtual board registration path similar to `cpu_miner`
4. Start with a minimal, reliable bring-up path:
     - detect hashboards
     - power on PSU
     - set safe fan speed
     - reset one board
     - enumerate BM1362 chain
     - repeat for all 3 boards
     - only then add more advanced control and tuning

## Open questions to resolve early

- How should the Amlogic board be enabled in Mujina?
    - Use a config file. It should describe the miner hardware layout and enable the Amlogic board path explicitly.
- Should all 3 hashboards be required, or should Mujina tolerate 1-2 populated boards?
    - Support any number of hashboards. Initially, use the config file to declare the expected number and type of connected hashboards.
- Should temperature and EEPROM reads happen before mining starts as a health gate?
    - yes
- What fan policy is required for safe startup if PSU comes up before all tach inputs are valid?
    - Put the fan count and default startup speed in the config file.
- Is there a board LED or other local status indicator worth integrating into the initial milestone?
    - Yes. There are two LEDs, one red and one green. An implementation example exists in `amlogic-cb-tools/src/bin/controlboard-misc.rs`.

### Decisions from the answers above

- Native Amlogic support should be config-driven, not USB-discovered.
- The config file should describe expected hashboards, fan setup, and startup defaults.
- Partial population is a supported mode, not an error by default.
- EEPROM and temperature checks should be part of pre-mining validation.
- LED support is in scope for early Amlogic bring-up.

## Phase 0 framing output

### Initial success target

Phase 0 should target a narrow, testable first milestone:

- Mujina starts in native Amlogic mode from config.
- Mujina initializes PSU, fans, and one configured S19j Pro hashboard safely.
- Mujina performs EEPROM and temperature health checks before mining.
- Mujina enumerates one BM1362 chain and reaches stable mining on one board.
- The same config model can later scale to two or three hashboards without redesign.

### Proposed config schema

The config should describe the control board as one shared hardware unit with per-hashboard entries.

Proposed top-level shape inside Mujina config:

- `hardware.amlogic_control_board.enabled`
- `hardware.amlogic_control_board.board_name`
- `hardware.amlogic_control_board.psu`
- `hardware.amlogic_control_board.startup`
- `hardware.amlogic_control_board.fans[]`
- `hardware.amlogic_control_board.leds`
- `hardware.amlogic_control_board.hashboards[]`

Proposed example TOML:

```toml
[hardware.amlogic_control_board]
enabled = true
board_name = "s19jpro-amlogic"

[hardware.amlogic_control_board.psu]
i2c_device = "/dev/i2c-1"
address = 16
write_register = 17
enable_gpio = 437

[hardware.amlogic_control_board.startup]
default_fan_percent = 50
initial_voltage = 12.6
psu_settle_ms = 2000
reset_assert_ms = 100
reset_release_ms = 2000

[hardware.amlogic_control_board.startup.health_gate]
read_eeprom_before_mining = true
read_temperatures_before_mining = true
fail_on_missing_expected_hashboard = false

[[hardware.amlogic_control_board.fans]]
index = 0
pwm_chip = 0
pwm_channel = 0
tach_gpio = 447
pulses_per_rev = 2

[[hardware.amlogic_control_board.fans]]
index = 1
pwm_chip = 0
pwm_channel = 0
tach_gpio = 448
pulses_per_rev = 2

[[hardware.amlogic_control_board.fans]]
index = 2
pwm_chip = 0
pwm_channel = 1
tach_gpio = 449
pulses_per_rev = 2

[[hardware.amlogic_control_board.fans]]
index = 3
pwm_chip = 0
pwm_channel = 1
tach_gpio = 450
pulses_per_rev = 2

[hardware.amlogic_control_board.leds]
green_gpio = 453
red_gpio = 438

[[hardware.amlogic_control_board.hashboards]]
index = 0
model = "s19j_pro"
serial_path = "/dev/ttyS1"
reset_gpio = 454
detect_gpio = 439
temp_i2c_device = "/dev/i2c-0"
eeprom_i2c_device = "/dev/i2c-0"
required = false

[[hardware.amlogic_control_board.hashboards]]
index = 1
model = "s19j_pro"
serial_path = "/dev/ttyS2"
reset_gpio = 455
detect_gpio = 440
temp_i2c_device = "/dev/i2c-0"
eeprom_i2c_device = "/dev/i2c-0"
required = false

[[hardware.amlogic_control_board.hashboards]]
index = 2
model = "s19j_pro"
serial_path = "/dev/ttyS3"
reset_gpio = 456
detect_gpio = 441
temp_i2c_device = "/dev/i2c-0"
eeprom_i2c_device = "/dev/i2c-0"
required = false
```

### Hardware mapping assumptions to document

Initial Amlogic defaults currently inferred from `amlogic-cb-tools`:

- PSU bus: `/dev/i2c-1`
- PSU address: `0x10`
- PSU write register: `0x11`
- PSU enable GPIO: `437`
- Hashboard UARTs: `/dev/ttyS1`, `/dev/ttyS2`, `/dev/ttyS3`
- Hashboard reset GPIOs: `454`, `455`, `456`
- Hashboard detect GPIOs: `439`, `440`, `441`
- Hashboard temperature / EEPROM bus: `/dev/i2c-0`
- PWM chip: `pwmchip0`
- PWM channels: `0`, `1`
- Fan tach GPIOs: `447`, `448`, `449`, `450`
- LED GPIOs: green=`453`, red=`438`

These should be treated as config defaults, not permanently hard-coded assumptions.

## Execution plan

### Phase 0 - Project framing

- [x] Confirm the board model: one Amlogic control board -> three S19j Pro hashboard threads
- [x] Decide how native Amlogic mode is selected at runtime
- [x] Define the initial success target: stable mining on 1 board first, then 3 boards
- [x] Define the config schema for board layout, expected hashboards, fans, and startup defaults
- [x] Capture GPIO, I2C bus, serial port, and device-path assumptions in code comments and docs

Exit criteria:

- agreed runtime model
- agreed activation path
- known hardware map documented

### Phase 1 - Preserve and isolate the existing Bitcrane implementation

- [x] Rename `mujina-miner/src/board/s19j_pro.rs` to `mujina-miner/src/board/s19j_pro_bitcrane.rs`
- [x] Update `mujina-miner/src/board/mod.rs` exports accordingly
- [ ] Keep behavior unchanged for the existing Bitcrane path
- [ ] Extract any obviously shared S19j Pro constants/helpers into a common module if it reduces duplication cleanly

Exit criteria:

- Bitcrane S19j Pro still builds and behaves the same
- codebase clearly distinguishes Bitcrane vs Amlogic support

### Phase 2 - Create Amlogic hardware adapters inside Mujina

- [ ] Decide whether to reuse `amlogic-cb-tools` directly as a library dependency or copy the needed primitives into Mujina abstractions
- [ ] Add native Linux-backed adapters for:
    - [ ] GPIO
    - [ ] serial ports
    - [ ] I2C devices
    - [ ] PWM fan control
    - [ ] tachometer reads
- [ ] Wrap the tested logic from these `amlogic-cb-tools` utilities:
    - `hashboard_s19jpro`
    - `apw12-psu-tool`
    - `fan-tool`
- [ ] Keep the Mujina-side API async-friendly even if the first Linux primitives are blocking underneath

Exit criteria:

- Mujina has reusable Amlogic control primitives, not just standalone test binaries
- all required board I/O paths are available from the miner crate

### Phase 3 - Add a native Amlogic board creation path

- [x] Extend Mujina startup so a local Amlogic board can be created without USB discovery
- [ ] Choose one of these approaches and implement it:
    - [ ] new transport event type for local control-board discovery
    - [x] new virtual board type for `s19j_pro_amlogic`
- [x] Add config-driven runtime gating so this path only activates on intended systems
- [x] Define a stable board ID and API-visible board name for the control board

Exit criteria:

- starting Mujina on target hardware creates the Amlogic board automatically or via explicit config
- board registration reaches the API cleanly

### Phase 4 - Implement `s19j_pro_amlogic.rs`

- [x] Create `mujina-miner/src/board/s19j_pro_amlogic.rs`
- [ ] Model shared board resources:
    - [ ] PSU
    - [ ] fan controller
    - [ ] tach inputs
    - [ ] per-hashboard reset lines
    - [ ] per-hashboard presence detect
    - [ ] per-hashboard temp sensors
- [ ] Model per-hashboard resources:
    - [ ] serial path
    - [ ] reset GPIO
    - [ ] detect GPIO
    - [ ] TMP75 sensors
    - [ ] EEPROM access
- [ ] Implement safe `initialize()` ordering:
    - [ ] detect installed boards
    - [ ] read EEPROM and temperature sensors as a pre-mining health gate
    - [ ] force safe fan setting
    - [ ] hold all ASICs in reset
    - [ ] enable PSU
    - [ ] set initial voltage
    - [ ] wait for power stabilization
- [ ] Implement `create_hash_threads()` to start up to 3 BM1362 chains
- [ ] Use config expectations to validate board presence while keeping partial-population support non-fatal by default

Exit criteria:

- Mujina can initialize the control board and create hash threads for detected hashboards

### Phase 5 - Telemetry, safety, and shutdown

- [ ] Publish board-level state for:
    - [ ] fan RPMs
    - [ ] target fan percent
    - [ ] per-hashboard temperatures
    - [ ] PSU voltage / power readings where available
    - [ ] thread status per hashboard
- [ ] Add startup and runtime safety checks:
    - [ ] PSU enable/disable confirmation
    - [ ] fan tach sanity
    - [ ] over-temperature handling
    - [ ] missing-board / broken-serial fault reporting
- [ ] Add LED status behavior for startup, running, and fault states
- [ ] Implement shutdown ordering:
    - [ ] stop hashing
    - [ ] assert resets
    - [ ] lower/stop fans according to safe policy
    - [ ] disable PSU

Exit criteria:

- board can enter and leave service safely
- API reflects useful operational telemetry

### Phase 6 - Bring-up and validation

- [ ] Add a hardware bring-up checklist for the target unit
- [ ] Validate one hashboard end-to-end first
- [ ] Validate all three hashboards together
- [ ] Confirm pool mining, share submission, and stability under load
- [ ] Tune initial voltage, fan defaults, and reset delays for reliable startup
- [ ] Test degraded cases:
    - [ ] one board missing
    - [ ] one board not enumerating
    - [ ] PSU communication failure
    - [ ] fan tach failure

Exit criteria:

- stable long-duration mining on the target platform
- known failure modes handled predictably

### Phase 7 - Cleanup and production readiness

- [ ] Document runtime setup and environment variables
- [ ] Add operator docs for temperatures, fans, and troubleshooting
- [ ] Remove hard-coded assumptions where possible
- [ ] Convert temporary bring-up logs into structured tracing
- [ ] Decide whether Amlogic support remains experimental or becomes a primary supported board

Exit criteria:

- code is documented
- operational path is repeatable
- support level is explicit

## Suggested implementation order for the first coding pass

1. Preserve current Bitcrane support by renaming/splitting the existing file.
2. Add a native Amlogic activation path in the daemon/backplane.
3. Reuse `amlogic-cb-tools` knowledge to build Mujina-native hardware adapters.
4. Bring up one Amlogic hashboard in Mujina.
5. Expand to three hashboards.
6. Add telemetry and safety interlocks.
7. Tune, validate, and document.

## Immediate next steps

- [ ] Decide whether `amlogic-cb-tools` becomes a shared library dependency of Mujina or just a reference implementation
- [x] Implement the Bitcrane/Amlogic board split
- [x] Define the Amlogic config schema in Mujina
- [x] Add the Amlogic board instantiation path
- [x] Implement first-pass one-hashboard native initialization in `s19j_pro_amlogic.rs`
- [ ] Boot a single hashboard from Mujina on target hardware