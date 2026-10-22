//! A tiny, dependency-free deterministic generator (SplitMix64).
//!
//! Fault decisions must be reproducible: the same `seed` and the same request
//! sequence number always give the same roll. SplitMix64 is not
//! cryptographically secure and does not need to be.

/// SplitMix64 state.
#[derive(Debug, Clone)]
pub struct SplitMix64 {
    state: u64,
}

const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

impl SplitMix64 {
    /// Creates a generator from a raw seed.
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Generator for one decision: mixes the global seed with a stream id
    /// (listener) and a sequence number (request or query).
    pub fn for_decision(seed: u64, stream: u64, seq: u64) -> Self {
        let mut g = Self::new(seed ^ stream.wrapping_mul(GOLDEN));
        let mixed = g.next_u64() ^ seq.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        Self::new(mixed)
    }

    /// Next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(GOLDEN);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform float in `[0, 1)` built from the top 53 bits.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }

    /// Uniform integer in `[0, bound]` (inclusive). `bound == 0` returns 0.
    pub fn up_to(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            return 0;
        }
        if bound == u64::MAX {
            return self.next_u64();
        }
        // Rejection sampling avoids modulo bias.
        let span = bound + 1;
        let zone = u64::MAX - (u64::MAX % span);
        loop {
            let v = self.next_u64();
            if v < zone {
                return v % span;
            }
        }
    }

    /// `true` with probability `percent / 100`. Values outside `[0, 100]` are clamped.
    pub fn roll_percent(&mut self, percent: f64) -> bool {
        if percent >= 100.0 {
            return true;
        }
        if percent <= 0.0 {
            return false;
        }
        self.next_f64() * 100.0 < percent
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_inputs_same_sequence() {
        let mut a = SplitMix64::for_decision(42, 1, 7);
        let mut b = SplitMix64::for_decision(42, 1, 7);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seq_differs() {
        let a = SplitMix64::for_decision(42, 1, 7).next_u64();
        let b = SplitMix64::for_decision(42, 1, 8).next_u64();
        assert_ne!(a, b);
    }

    #[test]
    fn up_to_stays_in_range() {
        let mut g = SplitMix64::new(1);
        for _ in 0..10_000 {
            assert!(g.up_to(9) <= 9);
        }
        assert_eq!(g.up_to(0), 0);
    }

    #[test]
    fn roll_percent_extremes_and_rate() {
        let mut g = SplitMix64::new(3);
        assert!(g.roll_percent(100.0));
        assert!(!g.roll_percent(0.0));
        let hits = (0..100_000).filter(|_| g.roll_percent(25.0)).count();
        // 25% +/- 1% over 100k trials (about 5 standard deviations).
        assert!((24_000..26_000).contains(&hits), "hits = {hits}");
    }
}
