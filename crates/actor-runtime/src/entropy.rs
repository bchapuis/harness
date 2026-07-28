//! The production [`Entropy`]: an OS-seeded PRNG (spec §4.6).
//!
//! The stream is a [`ChaCha8Rng`] seeded once from the operating system, for
//! peer selection, SWIM's `k`, and backoff jitter. It is not a CSPRNG interface
//! and MUST NOT be used for secrets.

use std::sync::Arc;
use std::sync::Mutex;

use actor_core::Entropy;
use rand::RngCore;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

/// A wall-entropy [`Entropy`] for the production runtime. Cheap to clone (shares
/// the stream behind an `Arc<Mutex<_>>`, the trait's interior-mutability
/// contract).
#[derive(Clone)]
pub struct OsEntropy {
    rng: Arc<Mutex<ChaCha8Rng>>,
}

impl OsEntropy {
    /// Seed a fresh stream from the operating system's randomness source.
    pub fn new() -> OsEntropy {
        let mut seed = [0u8; 32];
        getrandom::getrandom(&mut seed).expect("OS entropy source unavailable");
        OsEntropy {
            rng: Arc::new(Mutex::new(ChaCha8Rng::from_seed(seed))),
        }
    }
}

impl Default for OsEntropy {
    fn default() -> OsEntropy {
        OsEntropy::new()
    }
}

impl Entropy for OsEntropy {
    fn next_u64(&self) -> u64 {
        self.rng.lock().expect("entropy mutex poisoned").next_u64()
    }
}
