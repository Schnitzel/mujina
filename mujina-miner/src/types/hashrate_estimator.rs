//! Windowed hashrate estimation from share work.
//!
//! Estimates hashrate by accumulating work from shares within a
//! sliding time window. Each share records its expected work (from
//! `Target::to_work()`), and the estimator divides total work by
//! the span from the oldest sample to the current time.
//!
//! This gives an accurate estimate as soon as enough samples exist,
//! without waiting for the full window to fill. If shares stop
//! arriving, the span grows to include the silent period and the
//! estimate declines naturally.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use bitcoin::pow::Work;

use super::HashRate;
use crate::u256::U256;

/// Windowed hashrate estimator.
///
/// Tracks recent share work in a fixed-duration sliding window and
/// computes hashrate as `total_work / window_duration`.
pub struct HashrateEstimator {
    window: Duration,
    min_samples: usize,
    max_samples: usize,
    samples: VecDeque<(Instant, U256)>,
    total_work: U256,
    /// When `Some`, the estimator uses a cgminer-style exponentially-weighted
    /// moving average (the algorithm LuxOS/Braiins use for their short "5s"
    /// window) instead of the hard sliding window — smooth yet responsive. The
    /// `window` doubles as the EWMA time constant.
    ewma: Option<Ewma>,
}

/// cgminer `decay_time` exponentially-weighted moving average of hashrate. On
/// each advance it blends the accumulated work-rate into the running estimate
/// with a weight `fprop` that grows with the elapsed fraction of the time
/// constant, so a busy miner tracks quickly and a quiet one decays toward zero.
struct Ewma {
    /// Time constant (seconds) — the "5 s" / "1 m" / … window.
    interval_secs: f64,
    /// Current rolling hashrate estimate (hashes/sec).
    rate_hs: f64,
    /// Work (hashes) accumulated since the last advance.
    pending_hashes: f64,
    /// When the estimate was last advanced.
    last: Option<Instant>,
}

impl Ewma {
    fn new(interval: Duration) -> Self {
        Self {
            interval_secs: interval.as_secs_f64().max(1.0),
            rate_hs: 0.0,
            pending_hashes: 0.0,
            last: None,
        }
    }

    /// Accumulate a share's work (in hashes).
    fn record(&mut self, at: Instant, hashes: f64) {
        self.pending_hashes += hashes;
        if self.last.is_none() {
            self.last = Some(at);
        }
    }

    /// Advance the average to `now`, folding in the accumulated work, and return
    /// the current estimate (hashes/sec). Idempotent for `now == last`.
    fn rate_at(&mut self, now: Instant) -> f64 {
        if let Some(last) = self.last {
            let fsecs = now.saturating_duration_since(last).as_secs_f64();
            if fsecs > 0.0 {
                let fprop = 1.0 - 1.0 / (fsecs / self.interval_secs).exp();
                let ftotal = 1.0 + fprop;
                self.rate_hs += (self.pending_hashes / fsecs) * fprop;
                self.rate_hs /= ftotal;
                self.pending_hashes = 0.0;
                self.last = Some(now);
            }
        }
        self.rate_hs.max(0.0)
    }
}

impl HashrateEstimator {
    /// Create an estimator with the given measurement window.
    ///
    /// Uses reasonable defaults: settled threshold of 5 samples,
    /// capacity of `window_secs * 10` (assumes at most 10
    /// samples/sec).
    pub fn new(window: Duration) -> Self {
        let max_samples = window.as_secs() as usize * 10;
        Self::with_limits(window, 5, max_samples)
    }

    /// Create an estimator with explicit limits.
    pub fn with_limits(window: Duration, min_samples: usize, max_samples: usize) -> Self {
        Self {
            window,
            min_samples,
            max_samples,
            samples: VecDeque::new(),
            total_work: U256::ZERO,
            ewma: None,
        }
    }

    /// Create an estimator that uses a cgminer-style EWMA (the LuxOS/Braiins
    /// "5 s"-style rolling average) with `window` as the time constant. Smoother
    /// than the hard sliding window for short windows, while just as responsive.
    pub fn new_ewma(window: Duration) -> Self {
        let mut est = Self::with_limits(window, 5, window.as_secs() as usize * 10);
        est.ewma = Some(Ewma::new(window));
        est
    }

    /// Record work from a share at the current time.
    pub fn record(&mut self, work: Work) {
        self.record_at(Instant::now(), work);
    }

    /// Record work from a share at the given timestamp.
    pub fn record_at(&mut self, at: Instant, work: Work) {
        if let Some(e) = self.ewma.as_mut() {
            e.record(at, U256::from(work).saturating_to_u64() as f64);
            return;
        }
        let work = U256::from(work);
        self.prune_before(at.checked_sub(self.window).unwrap_or(at));
        self.samples.push_back((at, work));
        self.total_work += work;

        // Enforce capacity limit on top of time-based pruning
        while self.samples.len() > self.max_samples {
            if let Some((_, old_work)) = self.samples.pop_front() {
                self.total_work -= old_work;
            }
        }
    }

    /// Current hashrate estimate over the window.
    pub fn hashrate(&mut self) -> HashRate {
        self.hashrate_at(Instant::now())
    }

    /// Hashrate estimate at the given timestamp.
    ///
    /// Divides total work by the span from the oldest sample to `now`.
    /// This gives an accurate estimate as soon as samples exist rather
    /// than ramping up over the full window.
    pub fn hashrate_at(&mut self, now: Instant) -> HashRate {
        if let Some(e) = self.ewma.as_mut() {
            return HashRate::from(e.rate_at(now) as u64);
        }
        self.prune_before(now.checked_sub(self.window).unwrap_or(now));

        let secs = match self.samples.front() {
            Some(&(oldest, _)) => now.duration_since(oldest).as_secs(),
            None => return HashRate::from(0u64),
        };
        if secs == 0 {
            return HashRate::from(0u64);
        }

        HashRate::from((self.total_work / secs).saturating_to_u64())
    }

    /// Whether any samples exist within the window.
    pub fn has_samples(&self) -> bool {
        if let Some(e) = &self.ewma {
            return e.last.is_some();
        }
        !self.samples.is_empty()
    }

    /// Whether the estimate has settled enough to trust.
    ///
    /// Returns true once at least `min_samples` have been recorded
    /// in the current window. Before this point, callers should
    /// prefer a static estimate (e.g., from hardware capabilities).
    pub fn is_settled(&self) -> bool {
        if let Some(e) = &self.ewma {
            return e.last.is_some();
        }
        self.samples.len() >= self.min_samples
    }

    /// Returns the measured hashrate if the estimator has settled,
    /// or `None` if not enough samples have been collected yet.
    pub fn settled_hashrate(&mut self) -> Option<HashRate> {
        if self.is_settled() {
            Some(self.hashrate())
        } else {
            None
        }
    }

    /// Remove samples older than `cutoff`, subtracting their work.
    fn prune_before(&mut self, cutoff: Instant) {
        while let Some(&(t, work)) = self.samples.front() {
            if t >= cutoff {
                break;
            }
            self.total_work -= work;
            self.samples.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a Work value from a u64 hash count.
    fn work(n: u64) -> Work {
        // Work is 2^256 / (target + 1), but for testing we just need
        // a value that round-trips through U256. Work::from_256() is
        // not available, so build from le_bytes via Target::to_work()
        // with a known target.
        //
        // Simpler: use the fact that difficulty-1 target produces
        // work ≈ 2^32. We can scale from there, but for unit tests
        // it's easier to construct directly from bytes.
        let bytes = {
            let mut b = [0u8; 32];
            b[..8].copy_from_slice(&n.to_le_bytes());
            b
        };
        Work::from_le_bytes(bytes)
    }

    #[test]
    fn no_samples() {
        let est = HashrateEstimator::new(Duration::from_secs(300));
        assert!(!est.has_samples());
    }

    #[test]
    fn no_samples_hashrate_zero() {
        let mut est = HashrateEstimator::new(Duration::from_secs(300));
        let now = Instant::now();
        assert_eq!(u64::from(est.hashrate_at(now)), 0);
    }

    #[test]
    fn single_sample_zero_span() {
        let mut est = HashrateEstimator::new(Duration::from_secs(100));
        let now = Instant::now();

        // Single sample at `now` has zero time span, so rate is zero.
        est.record_at(now, work(1000));
        assert_eq!(u64::from(est.hashrate_at(now)), 0);
    }

    #[test]
    fn single_sample_with_elapsed() {
        let mut est = HashrateEstimator::new(Duration::from_secs(100));
        let base = Instant::now();

        // One sample at base, queried 10s later: 1000 / 10 = 100 H/s
        est.record_at(base, work(1000));
        assert_eq!(
            u64::from(est.hashrate_at(base + Duration::from_secs(10))),
            100
        );
    }

    #[test]
    fn multiple_samples_sum() {
        let mut est = HashrateEstimator::new(Duration::from_secs(100));
        let base = Instant::now();

        est.record_at(base, work(1000));
        est.record_at(base + Duration::from_secs(10), work(2000));
        est.record_at(base + Duration::from_secs(20), work(3000));

        // Total: 6000 work over 20s span = 300 H/s
        let rate = est.hashrate_at(base + Duration::from_secs(20));
        assert_eq!(u64::from(rate), 300);
    }

    #[test]
    fn expired_samples_pruned() {
        let mut est = HashrateEstimator::new(Duration::from_secs(100));
        let base = Instant::now();

        est.record_at(base, work(5000));
        est.record_at(base + Duration::from_secs(50), work(1000));

        // At base+150, the first sample (at base) is 150s old and
        // outside the 100s window. Only the second remains.
        let at = base + Duration::from_secs(150);
        let rate = est.hashrate_at(at);
        assert_eq!(u64::from(rate), 10); // 1000 / 100
        assert!(est.has_samples());
    }

    #[test]
    fn all_samples_expired() {
        let mut est = HashrateEstimator::new(Duration::from_secs(100));
        let base = Instant::now();

        est.record_at(base, work(5000));

        let at = base + Duration::from_secs(200);
        assert_eq!(u64::from(est.hashrate_at(at)), 0);
        assert!(!est.has_samples());
    }

    #[test]
    fn estimate_tracks_elapsed_time() {
        let mut est = HashrateEstimator::new(Duration::from_secs(100));
        let base = Instant::now();

        // Record 500 work at base. At base+5s the span is 5s,
        // so rate = 500/5 = 100 H/s. At base+50s the span is 50s,
        // so rate = 500/50 = 10 H/s.
        est.record_at(base, work(500));
        assert_eq!(
            u64::from(est.hashrate_at(base + Duration::from_secs(5))),
            100
        );
        assert_eq!(
            u64::from(est.hashrate_at(base + Duration::from_secs(50))),
            10
        );
    }

    #[test]
    fn zero_duration_window() {
        let mut est = HashrateEstimator::new(Duration::ZERO);
        let now = Instant::now();
        est.record_at(now, work(1000));
        // Zero-length window: can't divide, returns zero
        assert_eq!(u64::from(est.hashrate_at(now)), 0);
    }

    #[test]
    fn prune_on_record_prevents_unbounded_growth() {
        let mut est = HashrateEstimator::new(Duration::from_secs(10));
        let base = Instant::now();

        // Add 100 samples over 100 seconds (only last ~10 should remain)
        for i in 0..100 {
            est.record_at(base + Duration::from_secs(i), work(100));
        }

        // Window is 10s, so at most ~11 samples should remain
        // (samples from t=90..99 are within [89, 99] window)
        assert!(est.samples.len() <= 12);
    }

    #[test]
    fn capacity_enforced() {
        let mut est = HashrateEstimator::with_limits(
            Duration::from_secs(1000), // large window so time pruning doesn't interfere
            1,
            20,
        );
        let base = Instant::now();

        // Add 50 samples within the window
        for i in 0..50 {
            est.record_at(base + Duration::from_secs(i), work(100));
        }

        // Capacity is 20, so oldest samples were dropped
        assert_eq!(est.samples.len(), 20);

        // Retained samples span t=30..49 (19s), total work = 2000.
        // Rate = 2000 / 19 = 105 H/s.
        let rate = est.hashrate_at(base + Duration::from_secs(49));
        assert_eq!(u64::from(rate), 105);
    }

    #[test]
    fn not_settled_initially() {
        let est = HashrateEstimator::new(Duration::from_secs(100));
        assert!(!est.is_settled());
    }

    #[test]
    fn settled_after_min_samples() {
        // Default min_samples is 5
        let mut est = HashrateEstimator::new(Duration::from_secs(100));
        let base = Instant::now();

        for i in 0..4 {
            est.record_at(base + Duration::from_secs(i), work(100));
        }
        assert!(!est.is_settled());

        est.record_at(base + Duration::from_secs(4), work(100));
        assert!(est.is_settled());
    }

    #[test]
    fn settled_with_custom_threshold() {
        let mut est = HashrateEstimator::with_limits(Duration::from_secs(100), 3, 1000);
        let base = Instant::now();

        est.record_at(base, work(100));
        est.record_at(base + Duration::from_secs(1), work(100));
        assert!(!est.is_settled());

        est.record_at(base + Duration::from_secs(2), work(100));
        assert!(est.is_settled());
    }

    #[test]
    fn settled_hashrate_none_before_settled() {
        let mut est = HashrateEstimator::with_limits(Duration::from_secs(100), 3, 1000);
        let base = Instant::now();

        est.record_at(base, work(500));
        assert!(est.settled_hashrate().is_none());
    }

    #[test]
    fn ewma_converges_to_steady_rate() {
        // Feed a steady 100 H/s (100 work each second) and read each second.
        let mut est = HashrateEstimator::new_ewma(Duration::from_secs(5));
        let base = Instant::now();
        let mut rate = 0;
        for i in 0..60 {
            est.record_at(base + Duration::from_secs(i), work(100));
            rate = u64::from(est.hashrate_at(base + Duration::from_secs(i)));
        }
        // Should settle very close to 100 H/s.
        assert!((90..=110).contains(&rate), "ewma rate {rate} not near 100");
        assert!(est.is_settled() && est.has_samples());
    }

    #[test]
    fn ewma_is_smoother_than_window_on_a_burst() {
        // A single big burst then silence: the EWMA absorbs it gradually where a
        // hard window would spike. Read 3s after the burst.
        let base = Instant::now();
        let mut ewma = HashrateEstimator::new_ewma(Duration::from_secs(5));
        let mut win = HashrateEstimator::new(Duration::from_secs(5));
        ewma.record_at(base, work(100));
        win.record_at(base, work(100));
        ewma.record_at(base + Duration::from_millis(100), work(1_000_000));
        win.record_at(base + Duration::from_millis(100), work(1_000_000));
        let at = base + Duration::from_secs(3);
        assert!(u64::from(ewma.hashrate_at(at)) < u64::from(win.hashrate_at(at)));
    }

    #[test]
    fn ewma_decays_toward_zero_when_idle() {
        let mut est = HashrateEstimator::new_ewma(Duration::from_secs(5));
        let base = Instant::now();
        for i in 0..20 {
            est.record_at(base + Duration::from_secs(i), work(100));
            let _ = est.hashrate_at(base + Duration::from_secs(i));
        }
        let busy = u64::from(est.hashrate_at(base + Duration::from_secs(20)));
        // No more shares; 30s later (6 time-constants) it should be far lower.
        let idle = u64::from(est.hashrate_at(base + Duration::from_secs(50)));
        assert!(idle * 4 < busy, "idle {idle} did not decay from busy {busy}");
    }

    #[test]
    fn settled_hashrate_some_after_settled() {
        let mut est = HashrateEstimator::with_limits(Duration::from_secs(100), 3, 1000);
        let base = Instant::now();

        for i in 0..3 {
            est.record_at(base + Duration::from_secs(i), work(1000));
        }
        // 3000 work over 2s span = 1500 H/s
        let rate = est.hashrate_at(base + Duration::from_secs(2));
        assert_eq!(u64::from(rate), 1500);
    }
}
