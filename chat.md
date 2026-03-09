# Chat Log

This file is a running project conversation log maintained from assistant summaries inside VS Code.
It is not an automatic export of the full Copilot chat transcript.

## 2026-03-09

### Session Note

- Recovered the prior session output from [proj.md](./proj.md) when direct transcript history was unavailable.
- Established a convention to keep future chat notes in this file at the project root.
- Current preference: append concise session summaries here unless a verbatim transcript is explicitly requested and available.

### Bring-Up Note

- Built `mujina-minerd` for `aarch64-unknown-linux-musl` with `cargo zigbuild` and deployed it to the live Amlogic control board at `192.168.1.236`.
- Confirmed the APW12 PSU is reachable, enabled, watchdog-disabled, and measuring about `12.65 V` after a `12.6 V` setpoint.
- Native Mujina bring-up reaches thread creation on the board but BM1362 enumeration currently fails with `0/126` chips responding.
- Standalone `hashboard_s19jpro` also gets zero ASIC replies, so the current blocker is below Mujina's scheduler/runtime layer.
- PLUG GPIO scan shows all three detect lines high: `439`, `440`, and `441`.
- Per-slot TMP75 probing shows only slot `0` responds, mapping the connected board to hashboard `0` on `/dev/ttyS1`.

### HB2 Correction

- Manual on-device `hashboard_s19jpro check` results show the live ASIC chain is actually on HB2 (`/dev/ttyS3`), not HB0.
- `hashboard_s19jpro check 2` returns all `126` chip replies, while HB0 and HB1 return zero replies.
- HB2 does not respond on the assumed TMP75 or EEPROM I2C addresses, so the native Mujina health gate had to be disabled for those checks during bring-up.
- Added [mujina-hb2.toml](mujina-hb2.toml) as an HB2-only config and fixed `s19j_pro_amlogic.rs` so disabled temperature health gates no longer force TMP75 reads.
- Retesting Mujina with the HB2-only config now reaches successful BM1362 chain verification with `126` chips responding.

### Mapping Mismatch

- Current live board behavior shows the UART/reset path and the TMP75/EEPROM path are not aligned to the same logical slot.
- `hashboard_s19jpro check 2` produces all `126` ASIC replies on `/dev/ttyS3`.
- `hashboard_s19jpro temps 2` and `hashboard_s19jpro eeprom 2` fail with `No such device or address`.
- `hashboard_s19jpro temps 0` and `hashboard_s19jpro eeprom 0` succeed even though `hashboard_s19jpro check 0` gets zero ASIC replies.
- Working hypothesis: the connected hashboard's ASIC UART/reset path is wired to HB2 while its temp/EERPOM I2C endpoints are still reachable via the HB0 address map, or the current per-slot I2C address assumptions are otherwise wrong.
- Implication for Mujina: the config model likely needs to decouple serial/reset slot selection from temp/EERPOM sensor addressing instead of assuming one-to-one slot mapping.

### Shutdown Safety

- Updated Mujina so daemon exit now always cancels tracked tasks and waits for board teardown even if `run()` exits through an error path before the normal signal-handling tail.
- Added a native Amlogic PSU drop-safe in `s19j_pro_amlogic.rs` so initialization failures or unexpected board drops still drive PSU enable inactive instead of leaving the APW12 on.

### Address Map Correction

- Updated Mujina's native Amlogic default TMP75 and EEPROM lookup tables so HB0 now maps to TMP75 `0x4E/0x4A` and EEPROM `0x52`, while HB2 now maps to TMP75 `0x48/0x4C` and EEPROM `0x50`, matching the corrected `amlogic-cb-tools` mapping.

### Deployed Binary

- Built and deployed an updated `mujina-minerd` to the live board at `/home/root/mujina-minerd`.
- Fixed MUSL cross-build gating so Linux USB discovery is treated as unavailable on `aarch64-unknown-linux-musl`, allowing the native Amlogic daemon binary to build cleanly with `cargo zigbuild`.
- Built and deployed `mujina-cli` to `/home/root/mujina-cli` so miner stats can be queried directly on the control board without needing Cargo on-device.
- Added and deployed `/home/root/start.sh` as a convenience launcher for the HB2 config, pool URL, pool user, and externally reachable API bind on port `7785`.