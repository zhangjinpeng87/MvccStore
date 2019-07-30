///
/// Several kinds of mem-store
///

use util::collection::HashMap;
use super::{Key, Value};

type V = (u64, Value);

/// Hash table
pub struct HashMemStore {
    // key -> (ts, value)
    map: HashMap<Key, V>,
}

impl HashMemStore {
    pub fn new() -> Self {
        Self {
            map: HashMap::default(),
        }
    }

    pub fn insert(&mut self, key: Key, value: Value, ts: u64) -> Option<V> {
        self.map.insert(key, (ts, value))
    }

    pub fn contains_key(&self, key: &Key) -> bool {
        self.map.contains_key(key)
    }

    pub fn remove(&mut self, key: &Key) -> Option<V> {
        self.map.remove(key)
    }

    pub fn get(&self, key: &Key) -> Option<&V> {
        self.map.get(key)
    }
}

/// Skip list
pub struct SkipList {}
