//! Bitcrane protocol implementation.
//!
//! The bitcrane protocol uses the same packet format as bitaxe-raw, but with
//! different GPIO command mappings for board-specific control pins.

pub mod apw12;
pub mod gpio;
