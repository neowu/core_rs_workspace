use std::collections::hash_map::DefaultHasher;
use std::hash::Hash as _;
use std::hash::Hasher as _;
use std::sync::LazyLock;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering;

use crate::network::hostname;

static ID_GENERATOR: LazyLock<IdGenerator> = LazyLock::new(IdGenerator::init);

pub fn next_id(current_time: i64) -> String {
    ID_GENERATOR.next_with_millis(current_time)
}

struct IdGenerator {
    counter: AtomicU16,
    machine_id: u32,
}

impl IdGenerator {
    fn init() -> Self {
        Self { counter: AtomicU16::new(rand::random()), machine_id: machine_identifier() }
    }

    #[allow(clippy::indexing_slicing, clippy::missing_asserts_for_indexing)]
    fn next_with_millis(&self, current_time: i64) -> String {
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

        let mut buf = [0_u8; 20];
        for (chunk, &b) in buf.as_chunks_mut::<2>().0.iter_mut().zip(&bytes) {
            chunk[0] = HEX[(b >> 4) as usize];
            chunk[1] = HEX[(b & 0xf) as usize];
        }
        // SAFETY: HEX bytes are all ASCII
        unsafe { String::from_utf8_unchecked(buf.to_vec()) }
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
        assert_eq!(id.len(), 20);
    }
}
