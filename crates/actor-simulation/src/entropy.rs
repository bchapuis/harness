//! The single seeded source of randomness for a simulation run (spec §4.6,
//! §18.2).

use std::sync::Arc;
use std::sync::Mutex;

use actor_core::Entropy;
use rand::RngCore;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

/// A seeded PRNG implementing [`Entropy`]. Cloning yields another handle to the
/// *same* stream, so every draw in a run — application randomness and the
/// scheduler's own tie-breaks alike — comes from one deterministic sequence.
#[derive(Clone)]
pub struct SimEntropy {
    rng: Arc<Mutex<ChaCha8Rng>>,
}

impl SimEntropy {
    /// Seed a fresh stream. The same seed reproduces the same run (spec §18.1).
    pub fn new(seed: u64) -> SimEntropy {
        SimEntropy {
            rng: Arc::new(Mutex::new(ChaCha8Rng::seed_from_u64(seed))),
        }
    }
}

impl Entropy for SimEntropy {
    fn next_u64(&self) -> u64 {
        self.rng.lock().expect("entropy mutex poisoned").next_u64()
    }

    /// Enabled under simulation: fires with probability
    /// `numerator / denominator`, drawn from the seeded stream (spec §18.3).
    fn buggify(&self, numerator: u64, denominator: u64) -> bool {
        assert!(denominator > 0, "buggify denominator must be positive");
        self.next_u64() % denominator < numerator
    }
}
