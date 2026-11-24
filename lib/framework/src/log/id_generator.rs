use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::Hash as _;
use std::hash::Hasher as _;
use std::sync::LazyLock;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering;

use crate::network::hostname;

static ID_GENERATOR: LazyLock<IdGenerator> = LazyLock::new(IdGenerator::init);

pub fn next_id(current_time: i64) -> Id {
    ID_GENERATOR.next_with_millis(current_time)
}

/// 20 hex chars. `Copy`, so it can be read where the action itself can't be borrowed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Id([u8; Id::LEN]);

impl Id {
    pub(crate) const LEN: usize = 20;

    pub const fn as_str(&self) -> &str {
        // SAFETY: only built from HEX bytes, all ASCII
        unsafe { str::from_utf8_unchecked(&self.0) }
    }

    pub(crate) const fn as_bytes(&self) -> &[u8; Id::LEN] {
        &self.0
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

struct IdGenerator {
    counter: AtomicU16,
    machine_id: u32,
}

impl IdGenerator {
    fn init() -> IdGenerator {
        IdGenerator { counter: AtomicU16::new(rand::random()), machine_id: machine_identifier() }
    }

    fn next_with_millis(&self, current_time: i64) -> Id {
        const HEX: &[u8; 16] = b"0123456789ABCDEF";

        let counter = self.counter.fetch_add(1, Ordering::Relaxed);

        let bytes: [u8; 10] = [
            (current_time >> 32) as u8,
            (current_time >> 24) as u8,
            (current_time >> 16) as u8,
            (current_time >> 8) as u8,
            current_time as u8,
            (self.machine_id >> 16) as u8,
            (self.machine_id >> 8) as u8,
            self.machine_id as u8,
            (counter >> 8) as u8,
            counter as u8,
        ];

        let mut buf = [0_u8; Id::LEN];
        for (chunk, &b) in buf.as_chunks_mut::<2>().0.iter_mut().zip(&bytes) {
            chunk[0] = HEX[(b >> 4) as usize];
            chunk[1] = HEX[(b & 0xf) as usize];
        }
        Id(buf)
    }
}

fn machine_identifier() -> u32 {
    let mut hasher = DefaultHasher::new();
    hostname().hash(&mut hasher);
    rand::random::<u32>().hash(&mut hasher);
    hasher.finish() as u32
}

#[cfg(test)]
mod tests {
    use crate::log::id_generator::next_id;

    #[test]
    fn next_id_format() {
        let id = next_id(1_700_000_000_000);
        assert_eq!(id.as_str().len(), 20);
        assert!(id.as_str().starts_with("8BCFE56800"), "id={id}");
    }
}
