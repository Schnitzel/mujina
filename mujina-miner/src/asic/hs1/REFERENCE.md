# HS-1 Operator Reference

This document describes the HS-1 (*Homo sapiens*, rev. 1) as a Bitcoin
mining element: the sheet protocol, the registers, and the operator
behavior behind them. Manufacturer documentation is not available and
is not expected. What appears here is the Mujina project's best
understanding, derived from published manual-hashing work, the SHA-256
standard, and field trials (see [Sources]).

The HS-1 is not competitive. It is documented for the same reason
[RFC 1149] documents avian carriers: the exercise fixes the shape of
the interface, and a backend that survives an operator running at
33 microhashes per second has no remaining assumptions about
hashrate to be surprised by.

Contents:

- [Overview]
- [Conventions]
- [Electrical and Thermal Characteristics]
- [The Search Space]
- [The Operator Chain]
- [Frame Format]
    - [Worksheet Frames]
    - [Reply Frames]
    - [Checksums]
    - [Byte Order]
- [Command Types]
    - [Issue Worksheet]
    - [Erratum]
    - [Stand Down]
    - [Chain Inactive]
- [Register Map]
    - [0x00 - OPERATOR_ID]
    - [0x08 - STEP_RATE]
    - [0x14 - TICKET_MASK]
    - [0x18 - STIMULANT_CONTROL]
    - [0xA4 - MIDSTATE_CONFIG]
    - [0xA8 - SOFT_RESET_CONTROL]
    - [On-Body Telemetry]
- [Initialization Sequence]
- [Hardware Errors]
- [Driver Guidance]
    - [Expected Hashrate Reports Zero]
    - [Share Targets and the Measurement Floor]
    - [Extranonce2 Distribution]
    - [Depletion and Removal Signals]
- [Performance]
- [Sources]

## Overview

The HS-1 is a general-purpose biological compute element, widely
available, requiring no driver signing and no vendor NDA. Configured
for mining, it evaluates SHA-256d over prospective block headers with
a pencil, searching for one whose hash falls below a target.

The part is single-core. It has no version rolling, no midstate
engine, no on-die CRC, and no serial interface. Work is delivered on
paper. The host renders a worksheet, places it in the outbox tray, and
polls the inbox tray for the completed sheet. The medium is the
protocol's defining constraint and the source of most of its
peculiarities.

One header hash costs the operator 336 elementary steps: 192
compression rounds (64 rounds over each of three 64-byte blocks --- two
for the 80-byte header plus padding, one for the 32-byte intermediate
digest plus padding) and 144 message-schedule expansions (48 per
block). At the nominal step rate this is 8.4 hours of continuous
arithmetic to test **one** nonce.

The HS-1 is the only part in this documentation set whose error rate
under normal operation exceeds fifty percent. [Hardware Errors] covers
the consequences, which dominate the design.

## Conventions

Bit and byte numbering follow the BM13xx reference: LSB-0 for bit
positions within deserialized values, transmission order from zero for
bytes.

Hexadecimal values carry the 0x prefix; unprefixed numbers are
decimal. Digests are written as eight space-separated 32-bit words
because that is how an operator writes them.

A few terms carry fixed meanings throughout:

- **operator**: one HS-1 part, seated.
- **sheet**: one worksheet, carrying exactly one candidate header and
    exactly one nonce. The unit of work assignment.
- **step**: one compression round or one message-schedule expansion.
    The part's clock period.
- **tray**: the outbox or inbox. The physical layer.
- **shift**: the interval an operator is available, bounded by
    biological constraints the host does not model.
- **erratum**: a reissued sheet following a detected arithmetic fault.
- **round-constant table**: the printed K values. Not optional; see
    [Initialization Sequence].

## Electrical and Thermal Characteristics

| Parameter                 | Min  | Typ     | Max  | Unit  |
|---------------------------|------|---------|------|-------|
| Supply (glucose)          | 3.9  | 5.5     | 7.8  | mmol/L|
| Core temperature          | 36.1 | 36.8    | 37.5 | °C    |
| Thermal throttle onset    | ---  | 37.5    | ---  | °C    |
| Thermal shutdown          | ---  | ---     | 42.0 | °C    |
| Power draw                | 80   | 97      | 400  | W     |
| Step period               | 60   | 90      | 600  | s     |
| Clock frequency           | 1.7  | 11.1    | 16.7 | mHz   |
| Sustained hashrate        | ---  | 33.1    | ---  | µH/s  |
| Continuous operation      | ---  | 8       | 16   | h     |

Notes:

- **Power draw** is whole-part basal consumption, roughly 2000 kcal
    per day. Unlike an ASIC, the figure is nearly independent of
    whether the part is hashing. Idling an HS-1 saves no meaningful
    energy, which makes `go_idle()` a scheduling operation rather than
    a power-management one.
- **Thermal shutdown** at 42 °C is destructive and not recoverable by
    power cycling. The host must never drive an HS-1 toward it.
- **Sustained hashrate** assumes continuous operation, which the
    "Continuous operation" row contradicts. A part on a single 8-hour
    shift per day averages 11.0 µH/s over calendar time. Datasheet
    convention is to quote the continuous figure; drivers should
    derate.
- The part self-clocks. STEP_RATE (0x08) is advisory, not a PLL.

## The Search Space

For one job, the HS-1 searches one dimension: the 32-bit nonce field.
It searches it one value at a time, and it does not finish.

At 33.1 µH/s a single operator sweeps 2^32 nonces in about 4.1 million
years. A difficulty-1 share is therefore not a routine event but a
geological one. At a network difficulty of 1.1 × 10^14, the expected
time to a block is 4.5 × 10^20 years, roughly 33 billion times the
current age of the universe.

The second dimension is unavailable. Version rolling requires holding
several independent compression states concurrently, and an operator
asked to interleave sixteen of them loses their place, producing
faults at a rate that exceeds the useful work (see [0xA4 -
MIDSTATE_CONFIG]).

Practical extension is therefore horizontal only: more operators, each
on a disjoint slice.

## The Operator Chain

Operators are addressed individually. There is no daisy chain and no
broadcast: paper does not fan out, and an operator asked to relay a
sheet to a neighbor introduces transcription faults on top of the
arithmetic faults already present. The host addresses each operator
directly with its own sheet.

Because every operator receives its own sheet, the host must divide
the search space at issue time rather than at configuration time. This
inverts the BM13xx arrangement, where the host divides the space once
during bring-up and then broadcasts identical jobs. See [Extranonce2
Distribution].

Operators grouped in one room constitute a domain. Domains share
ambient temperature, lighting, and interruption sources, and fail
together: a fire alarm removes every part in the domain
simultaneously. Drivers should not treat operators in one domain as
independent for availability purposes.

## Frame Format

Frames are sheets of paper. There is no preamble, no length field, and
no framing byte; the sheet boundary is the sheet.

### Worksheet Frames

The host renders one worksheet per candidate header. A sheet carries:

| Field           | Size      | Description                                |
|-----------------|-----------|--------------------------------------------|
| Sheet_ID        | 6 digits  | Monotonic, unique per operator             |
| Job_ID          | text      | The upstream job this sheet belongs to     |
| Nonce           | 4 bytes   | The single value under test                |
| Extranonce2     | 4-8 bytes | The operator's slice, fixed for this sheet |
| Header          | 80 bytes  | Serialized, in wire order                  |
| Block_1, Block_2| 64 bytes  | The header plus padding, pre-split         |
| Block_3         | partial   | Padding only; see below                    |
| Initial_H       | 32 bytes  | The SHA-256 IV, printed                    |
| K_Table         | 256 bytes | The 64 round constants, printed            |
| Target          | 32 bytes  | The acceptance threshold, big-endian       |

Block_1 and Block_2 are printed in full. Block_3 --- the second SHA-256
pass --- cannot be, because its first eight words are the output of the
first pass and are not known until the operator produces them. The
sheet prints what is known:

```text
  W[ 0] .. W[ 7]   <- your Part 1 result, one word per line
  W[ 8] = 80000000
  W[ 9] = 00000000
   ...              (W[9] through W[14] are zero)
  W[15] = 00000100
```

The last word is 0x100 because the second pass hashes exactly 256
bits. This padding is constant across every sheet ever issued and may
be printed once and reused.

The sheet ends with a reply block of blank fields. Filling it in and
moving the sheet to the inbox is the complete response path.

### Reply Frames

A reply is the same sheet with the reply block completed:

| Field    | Required | Description                                 |
|----------|----------|---------------------------------------------|
| sheet    | yes      | Echo of Sheet_ID                            |
| nonce    | yes      | Echo of the assigned nonce                  |
| pass1    | no       | Intermediate digest after the first SHA-256 |
| digest   | yes      | Final digest, natural byte order            |
| operator | no       | Free text, for the log                      |

`pass1` is optional but strongly recommended. Without it the host
learns only that a sheet is wrong; with it the host can localize the
fault to one of the two passes and reissue accordingly, which recovers
roughly two thirds of the wasted effort. See [Hardware Errors].

### Checksums

There are none. The HS-1 has no on-die integrity check, and an
operator asked to compute a CRC over their own work computes it with
the same faculties that produced the work, so an agreeing checksum
carries little information.

Integrity is therefore entirely the host's responsibility: the host
recomputes every reply from scratch. This is cheap. Verifying eight
and a half hours of operator effort costs the host about one
microsecond, a ratio of roughly 3 × 10^10 to 1. A driver should never
skip verification to save time; there is no time to save.

### Byte Order

This section is the most common source of field faults and should be
read to the operator during bring-up.

The operator works in SHA-256's natural order: bytes emerge from the
algorithm in the order the algorithm produces them, and that is the
order they are written into the `digest` field. The operator performs
no reversal at any point.

Bitcoin, however, displays block hashes reversed. The genesis block
hash is conventionally written

```text
  00000000 0019d668 9c085ae1 65831e93 4ff763ae 46a2a6c1 72b3f1b6 0a8ce26f
```

but the bytes SHA-256 actually produced, and the bytes an operator
would write, are

```text
  6fe28c0a b6f1b372 c1a6a246 ae63f74f 931e8365 e15a089c 68d61900 00000000
```

The leading zeros an operator has been told to look for are therefore
at the **end** of their own result. An operator who reaches the last
word of a correct solution and sees `00000000` has found something; an
operator who sees leading zeros in the first word has almost certainly
made an error, since that pattern is far rarer than the target
requires.

The host reverses the operator's digest before comparing it to Target.
The Target printed on the sheet is in display (big-endian) order,
because that is the form in which leading zeros are countable by eye,
and eye-countability is the only reason to print it at all.

## Command Types

### Issue Worksheet

Renders a sheet for one nonce and places it in the outbox. This is the
only command that causes hashing.

The nonce is chosen by the host, not the operator. Letting the
operator increment a nonce across sheets saves the host nothing and
introduces a state the host cannot audit.

### Erratum

Reissues a sheet the host has verified as wrong, with the fault
localized as far as the reply permits. An erratum carries the same
nonce as the original: the candidate has not been tested, only
mis-tested.

Errata should name the pass and, where a `pass1` value was supplied,
say which one diverged. "Your Part 2 is inconsistent with your Part 1"
is actionable. "Incorrect" is not, and an operator handed an
unlocalized erratum tends to redo all 336 steps, which is three times
the necessary work.

### Stand Down

Ends the current sheet without a reply. The operator retains the sheet
as scratch; the host discards the assignment.

Unlike an ASIC's idle state, Stand Down does not reduce power draw
(see [Electrical and Thermal Characteristics]). It exists so the
scheduler can reassign an operator when the chain tip moves, which it
will have done roughly 5000 times during a single sheet.

### Chain Inactive

Ends the shift. All outstanding sheets are abandoned. The host must
not treat an operator as available again until a new [Initialization
Sequence] completes, because the round-constant table, the seat, and
the operator's recollection of the byte-order rules are all
shift-scoped.

## Register Map

Registers are not memory-mapped. They are questions the host asks and
answers the host writes down. Reads are performed by asking; writes by
telling. Both are unreliable in the manner of all self-reported data.

| Register | Name               | Description                              |
|----------|--------------------|------------------------------------------|
| 0x00     | OPERATOR_ID        | Part identity and assigned address       |
| 0x08     | STEP_RATE          | Advisory step period                     |
| 0x14     | TICKET_MASK        | Reply threshold. Fixed at zero           |
| 0x18     | STIMULANT_CONTROL  | Step-rate and error-rate derating        |
| 0xA4     | MIDSTATE_CONFIG    | Version rolling. Not implemented         |
| 0xA8     | SOFT_RESET_CONTROL | Attention restoration                    |
| 0xB0     | TEMP_SENSOR        | Core temperature                         |
| 0xB8     | GLUCOSE_ADC        | Supply level                             |

### 0x00 - OPERATOR_ID

The operator's name, and the address the host assigns them. Unlike a
chip's CHIP_ID this field is not a model identifier: every HS-1 is a
different stepping, and none of the differences are documented.

Addresses are assigned at bring-up and are stable for one shift.

### 0x08 - STEP_RATE

The advisory step period, in seconds. Default 90.

This register does not control anything. The part self-clocks, and the
value the host writes is a request the operator may honor, exceed, or
quietly ignore. It exists so the driver has a number from which to
compute sheet deadlines and depletion warnings.

Values below 60 seconds per step are not sustainable. A part driven at
30 s/step will hold the rate for perhaps twenty steps and then fault at
a rate that makes the sheet worthless. There is no clock stretching
signal; the only evidence of overclocking is the error rate, arriving
8.4 hours later.

### 0x14 - TICKET_MASK

Nominally the threshold below which the operator does not bother
reporting a result. On a BM13xx this keeps the serial link from
flooding.

Fixed at zero on the HS-1. At 33.1 µH/s the part will not produce a
hash meeting any nonzero threshold within the operational life of the
part, the host, or the institution operating either. The operator
reports every sheet, and the inbox is not at risk of flooding.

Drivers should not expose this register.

### 0x18 - STIMULANT_CONTROL

Derates STEP_RATE and the fault rate together. The two move in
opposite directions and the register cannot separate them.

| Setting     | Step rate | Fault rate | Sustain     |
|-------------|-----------|------------|-------------|
| 0 (none)    | 1.00x     | 1.00x      | full shift  |
| 1 (nominal) | 1.15x     | 1.05x      | 4 h         |
| 2 (elevated)| 1.30x     | 1.40x      | 2 h         |
| 3 (maximum) | 1.35x     | 3.20x      | 40 min      |

Setting 3 is a net loss. A 1.35x step rate against a 3.2x fault rate
yields fewer verified sheets per shift than setting 0, and the
subsequent recovery interval removes the part for the remainder of the
day. It is documented so drivers do not rediscover it.

Every setting above 0 incurs a debt that is repaid at the start of the
following shift, at interest. The register has no read-back for
outstanding debt.

### 0xA4 - MIDSTATE_CONFIG

Reads zero. Writes are accepted and ignored.

Both features this register controls on a BM13xx are unavailable. The
part has no midstate engine: an operator cannot cache the compression
state after Block_1 and reuse it across sheets, because sheets differ
in the nonce, and the nonce lands in Block_2. (This is the one
optimization the host can perform on the operator's behalf --- Block_1
is identical across every sheet sharing a job and an extranonce2,
since the nonce and ntime both land in Block_2, and a driver that
prints Block_1's output once and reuses it across an operator's sheets
saves 112 of the 336 steps, a third of the shift. Doing so is
recommended, and the saving is real, and it changes nothing about the
conclusions in [Performance].)

Version rolling is likewise unavailable; see [The Search Space].

### 0xA8 - SOFT_RESET_CONTROL

Restores attention without ending the shift. Three levels:

| Value | Action                | Cost   |
|-------|-----------------------|--------|
| 0x01  | Verbal prompt         | 0 min  |
| 0x02  | Stand and stretch     | 5 min  |
| 0x04  | Leave the room        | 15 min |

A soft reset does not invalidate the current sheet. The operator
resumes at the step they left, provided they wrote down which step
that was, which is the single most valuable habit a driver can
establish during [Initialization Sequence].

Level 0x04 should be asserted on a schedule rather than in response to
observed faults. By the time faults are observable, the sheet
containing them is already 8 hours old.

### On-Body Telemetry

**0xB0 - TEMP_SENSOR.** Core temperature, °C. Read by asking, or by
instrument. The reading is not comparable across measurement sites,
and a driver should record which site produced it. Sustained readings
above 37.5 °C indicate the part is unwell rather than overworked;
mining is not capable of heating an HS-1.

**0xB8 - GLUCOSE_ADC.** Supply level. Falls monotonically during a
sheet and correlates with the fault rate more strongly than any other
observable. A driver that reads only one telemetry register should
read this one.

The correct response to a low reading is a supply event, not a soft
reset. The two are frequently confused because both restore the fault
rate temporarily.

## Initialization Sequence

Bring-up costs 2.8 hours and must complete before any sheet is issued.
Skipping it does not save the time; it defers the time into the first
sheet, where it is spent anyway and produces no verifiable result.

1. **Seat the operator.** Assign OPERATOR_ID (0x00). Record the
    measurement site for TEMP_SENSOR.

2. **Supply the round-constant table.** All 64 K values, printed. An
    operator deriving K from cube roots on demand is not mining; they
    are recomputing a constant 192 times per sheet, and the derivation
    is more error-prone than every other step combined.

3. **Supply the IV.** The eight initial H values, printed.

4. **Read the byte-order rules aloud.** See [Byte Order]. This step is
    skipped more often than any other and accounts for a
    disproportionate share of first-shift faults.

5. **Run the known-answer test.** The operator computes SHA-256 of the
    3-byte message `abc` --- one block, 112 steps, about 2.8 hours ---
    and the host compares against the [FIPS 180-4] vector:

    ```text
      ba7816bf 8f01cfea 414140de 5dae2223 b00361a3 96177a9c b410ff61 f20015ad
    ```

    A part that fails the known-answer test must not be issued a
    sheet. The test is one block; a sheet is three. A part that cannot
    complete one block correctly will not complete three, and the
    failure will cost 8.4 hours to discover instead of 2.8.

6. **Establish the step-marking habit.** The operator writes the
    current step number in the margin whenever they pause. This makes
    SOFT_RESET_CONTROL usable and is the highest-value instruction in
    the sequence.

7. **Write STEP_RATE (0x08)** and begin issuing sheets.

## Hardware Errors

The dominant failure mode. Faults are arithmetic: a dropped carry in a
32-bit modular addition, a rotation by the wrong distance, a
transcription error copying a word between steps.

Sheet yield against per-step fault probability, over 336 steps:

| Per-step fault rate | Clean sheets |
|---------------------|--------------|
| 0.01%               | 96.7%        |
| 0.1%                | 71.5%        |
| 1%                  | 3.4%         |
| 5%                  | 0.000003%    |

The 1% row is the important one. A one-percent per-step fault rate is
unremarkable for hand arithmetic, and it yields a clean sheet about
one time in thirty. At 8.4 hours per attempt, a part at that fault
rate produces one verified hash per ten operator-months.

Two mitigations are available, and drivers should implement both:

- **Localize with `pass1`.** A reply carrying the intermediate digest
    lets the host say which pass failed. The first pass is 224 of the
    336 steps; the second is 112. Reissuing only the failed pass
    recovers most of the effort, and reissuing a failed second pass
    costs a third of a fresh sheet.

- **Reissue rather than discard.** The nonce has not been tested, only
    mis-tested. An erratum carrying the same nonce preserves the
    search-space accounting; issuing a fresh nonce silently leaves a
    hole in the sweep, and after enough errata the driver's coverage
    claim is false.

Faults are not independent. They cluster near the end of a shift, near
low GLUCOSE_ADC readings, and immediately following an interruption.
A driver that models fault rate as constant will under-predict late-
shift faults by a wide margin.

## Driver Guidance

This section covers what a mujina backend must do differently for an
HS-1, against the `HashThread` interface in `../hash_thread.rs`.

### Expected Hashrate Reports Zero

`HashRate` counts whole hashes per second in a `u64`. An operator runs
at 3.3 × 10^-5 H/s. The value truncates to zero, and there is no
representation in the type that does otherwise.

This is not a defect in either the type or the part. It is the honest
answer at this scale, and a backend must emit it rather than rounding
up to 1 H/s to keep the arithmetic tidy --- a 30,000x overstatement
propagates into share-target selection and produces worse behavior
than the zero does.

The zero must still be emitted. `HashThreadEvent::ExpectedHashRate` is
what makes a thread eligible for work: the scheduler filters on
`expected.is_some()`, not on the value being positive. A backend that
declines to report because the number is zero never receives a sheet.

### Share Targets and the Measurement Floor

`Scheduler::compute_scheduler_target` normally clamps the pool's
target between a measurement floor of one share per second and a flood
ceiling of ten per second. Both bounds are computed from the thread's
hashrate, and both collapse when that hashrate is zero. The function
short-circuits and passes the source target through unmodified.

The effect is benign and slightly lucky: an operator is assigned the
pool's real difficulty rather than a target derived from a floor they
miss by eleven orders of magnitude. A driver should not attempt to
improve on this.

`is_difficulty_too_high` short-circuits on the same condition, and
returns `false`. The scheduler therefore never warns that the assigned
difficulty is unreasonable for the attached hashrate --- in the one
configuration where that warning would be most justified, it is
suppressed. This is worth knowing when reading logs from an HS-1 run:
the absence of the warning is not evidence that the difficulty is
appropriate.

### Extranonce2 Distribution

The scheduler splits the extranonce2 range evenly across all eligible
threads. Eligibility does not consider hashrate, so an operator seated
alongside a Bitaxe Gamma receives half the extranonce2 space.

The scheduler is being scrupulously fair between a part at 33 µH/s and
a part at 1.2 TH/s, and the resulting allocation wastes almost exactly
half the extranonce2 range for as long as both are attached. The
range is large enough that this costs nothing measurable, and a
backend should leave it alone rather than special-casing.

Drivers should not mix HS-1 and ASIC threads on one board regardless,
for the reason in [The Operator Chain]: they do not share a failure
domain, a bring-up sequence, or a telemetry model.

### Depletion and Removal Signals

Map the biological constraints onto the existing events:

- `WorkDepletionWarning` --- issue when the shift has less remaining
    time than the current sheet needs. `estimated_remaining_ms` should
    be the time to end of shift, not the time to end of sheet.
- `WorkExhausted` --- the shift has ended with the sheet incomplete.
    Report `en2_searched: 0`, which is accurate.
- `ThreadRemovalSignal::HardwareFault` --- illness, or a sustained
    fault rate above the useful threshold. The `description` field
    should carry the fault rate, not a diagnosis.
- `ThreadRemovalSignal::UserRequested` --- resignation.

A backend must not report an operator as removed merely because a
sheet is outstanding. The expected case is that every sheet is
outstanding for 8.4 hours.

## Performance

Single operator, nominal STEP_RATE, continuous operation:

| Metric                          | Value                        |
|---------------------------------|------------------------------|
| Steps per header hash           | 336                          |
| Time per header hash            | 8.4 h                        |
| Sustained hashrate              | 33.1 µH/s                    |
| Energy per hash                 | 2.93 MJ (0.81 kWh)           |
| Efficiency                      | 2.93 × 10^18 J/TH            |
| Efficiency vs. Antminer S21 Pro | 2.0 × 10^17 x worse          |
| Expected time to a diff-1 share | 4.1 million years            |
| Expected time to a block        | 4.5 × 10^20 years            |
| ... as a multiple of the age of the universe | 33 billion      |

Scaling:

| Target                             | Operators required     |
|------------------------------------|------------------------|
| One Antminer S21 Pro (234 TH/s)    | 7.1 × 10^18            |
| ... expressed as Earth populations | 860 million            |
| The current Bitcoin network        | 3.0 × 10^25            |

The efficiency figure is the one worth internalizing. A part drawing
97 W and producing one hash per 8.4 hours spends 0.81 kWh per hash.
The same energy runs an S21 Pro for about 8 seconds, during which it
performs roughly 2 × 10^15 hashes.

## Sources

- The SHA-256 specification and test vectors: [FIPS 180-4]
- Published manual-hashing work: [Shirriff], which established the
    order of magnitude and the observation that the paper is the
    bottleneck
- Transmission over unconventional carriers: [RFC 1149] and its
    quality-of-service successor [RFC 2549], for the framing of this
    document and for establishing that a documented interface to an
    absurd medium is still a documented interface
- The mujina `HashThread` interface and scheduler behavior described
    in [Driver Guidance], read from the implementation
- Field trials: informal, unblinded, and small-n. Every fault-rate
    figure in [Hardware Errors] should be treated as an order of
    magnitude rather than a measurement

[Overview]: #overview
[Conventions]: #conventions
[Electrical and Thermal Characteristics]: #electrical-and-thermal-characteristics
[The Search Space]: #the-search-space
[The Operator Chain]: #the-operator-chain
[Frame Format]: #frame-format
[Worksheet Frames]: #worksheet-frames
[Reply Frames]: #reply-frames
[Checksums]: #checksums
[Byte Order]: #byte-order
[Command Types]: #command-types
[Issue Worksheet]: #issue-worksheet
[Erratum]: #erratum
[Stand Down]: #stand-down
[Chain Inactive]: #chain-inactive
[Register Map]: #register-map
[0x00 - OPERATOR_ID]: #0x00---operator_id
[0x08 - STEP_RATE]: #0x08---step_rate
[0x14 - TICKET_MASK]: #0x14---ticket_mask
[0x18 - STIMULANT_CONTROL]: #0x18---stimulant_control
[0xA4 - MIDSTATE_CONFIG]: #0xa4---midstate_config
[0xA8 - SOFT_RESET_CONTROL]: #0xa8---soft_reset_control
[On-Body Telemetry]: #on-body-telemetry
[Initialization Sequence]: #initialization-sequence
[Hardware Errors]: #hardware-errors
[Driver Guidance]: #driver-guidance
[Expected Hashrate Reports Zero]: #expected-hashrate-reports-zero
[Share Targets and the Measurement Floor]: #share-targets-and-the-measurement-floor
[Extranonce2 Distribution]: #extranonce2-distribution
[Depletion and Removal Signals]: #depletion-and-removal-signals
[Performance]: #performance
[Sources]: #sources
[FIPS 180-4]: https://nvlpubs.nist.gov/nistpubs/FIPS/NIST.FIPS.180-4.pdf
[RFC 1149]: https://www.rfc-editor.org/rfc/rfc1149
[RFC 2549]: https://www.rfc-editor.org/rfc/rfc2549
[Shirriff]: http://www.righto.com/2014/09/mining-bitcoin-with-pencil-and-paper.html
