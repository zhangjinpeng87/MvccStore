
use std::u64;
use std::string::String;
use super::super::{CF_WRITE, CF_DEFAULT, CF_LOCK, Key, Value, };
use super::super::MvccStorage;
use util::rocksdb::DB;

const TS_LEN: size = 8;

struct Storage {
    db: DB,
}

impl Storage {
    pub fn new(db: DB) -> Self {
        Self { db }
    }

    fn get_latest_write(&self, key: &Key) -> Result<Option<Write>, String> {

    }
}

impl MvccStorage for Storage {
    fn prewrite(&self, key: Key, value: Value, op_type: OpType, start_ts: u64) -> Result<(), String> {
        // Check lock
        let lock_cf = get_cf_handle(&self.db, CF_LOCK).unwrap();
        if let Some(_) = self.db.get_cf(lock_cf, &key) {
            return Err(String::from("key is locked"));
        }

        // Check write conflict
        let write_cf = get_cf_handle(&self.db, CF_WRITE).unwrap();
        if let Some(write) = self.get_latest_write(&key) {
            if write.commit_ts >= start_ts {
                return Err(String::from("write conflict"));
            }
        }

        // Write
        let lock = Lock::new(start_ts, op_type, None);
        let lock_value = lock.to_vec();
        self.db.put_cf(&key, &lock_value);
        let encoded_key = encode_ts_to_key(&key, start_ts);
        self.db.put(&encoded_key, &value);
    }

    fn commit(&mut self, key: Key, start_ts: u64, commit_ts: u64) -> Result<(), String> {

    }

    fn rollback(&mut self, key: Key, start_ts: u64) -> Result<(), String> {

    }

    fn get(&self, key: Key, ts: u64) -> Result<Option<Value>, String> {

    }

    fn scan(&self, start: Key, end: Key, ts: u64) -> Result<Option<Vec<Value>>, String> {

    }
}

struct Lock {
    pub start_ts: u64,
    pub op_type: OpType,
    pub short_value: Option<Value>,
}

impl Lock {
    pub fn new(start_ts: u64, op_type: OpType, short_value: Option<Value>) -> Self {
        Self {
            start_ts,
            op_type,
            short_value,
        }
    }

    pub fn from_bytes(b: &[u8]) -> Result<Self, String> {

    }

    pub fn to_vec(&self) -> Vec<u8> {

    }
}

struct Write {
    start_ts: u64,
    commit_ts: u64,
    op_type: OpType,
}

pub fn encode_ts_to_key(key: &Key, ts: u64) -> Key {
    let mut k = Key::with_capacity(key.len() + TS_LEN);
    k.append(key);
    k.append((!ts).to_be_bytes());
    k
}

pub fn decode_ts_from_key(key: &Key) -> u64 {
    !u64::from_be_bytes(key.as_slice()[key.len()-TS_LEN..TS_LEN])
}