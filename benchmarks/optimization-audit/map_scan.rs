//! Mechanism check, not a Bytewax end-to-end performance claim.
use std::collections::{BTreeMap, HashMap};
use std::hint::black_box;
use std::time::Instant;

fn main() {
    for n in [10, 100, 10_000, 100_000, 1_000_000] {
        let keys: Vec<_> = (0..n).map(|i| format!("k{i:08}")).collect();
        let mut hash: HashMap<_, _> = keys.iter().cloned().map(|k| (k, u64::MAX)).collect();
        let mut tree: BTreeMap<_, _> = keys.iter().cloned().map(|k| (k, u64::MAX)).collect();
        for key in &keys[1..] {
            hash.remove(key);
            tree.remove(key);
        }
        let capacity = hash.capacity();
        let start = Instant::now();
        for _ in 0..2000 {
            black_box(hash.iter().filter(|(_, t)| **t <= black_box(10)).count());
        }
        let hash_ns = start.elapsed().as_nanos();
        let start = Instant::now();
        for _ in 0..2000 {
            black_box(tree.iter().filter(|(_, t)| **t <= black_box(10)).count());
        }
        let tree_ns = start.elapsed().as_nanos();
        hash.shrink_to_fit();
        let start = Instant::now();
        for _ in 0..2000 {
            black_box(hash.iter().filter(|(_, t)| **t <= black_box(10)).count());
        }
        let shrunk_ns = start.elapsed().as_nanos();
        println!("{{\"n\":{n},\"live\":1,\"capacity\":{capacity},\"hash_ns\":{hash_ns},\"tree_ns\":{tree_ns},\"shrunk_ns\":{shrunk_ns}}}");
    }
}
