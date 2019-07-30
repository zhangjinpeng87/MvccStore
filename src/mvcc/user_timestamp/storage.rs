
use std::string::String;
use std::u64;
use std::io::Write;

use byteorder::{BigEndian};

use super::memstore::HashMemStore;
use super::TxnStorage;
use util::rocksdb::{DB, WriteOption, ReadOptions};

const TIMESTAMP_LEN: usize = 8;

pub struct Storage {
    // Store pre-write result.
    // TODO: add wal for mem_store
    mem_store: HashMemStore,

    // Only committed value can write to DB.
    db: DB,
}

impl Storage {
    pub fn new(db: DB) -> Self {
        Self {
            mem_store: HashMemtable::new(),
            db,
        }
    }

    // Return committed value whose start ts equal to `start_ts`, or return None.
    fn get_committed_version(&self, key: &Key, start_ts: u64, commit_ts: u64) -> Option<Value> {
        let mut read_opt = ReadOptions::new();
        read_opt.set_timestamp(commit_ts);
        match self.db.Get(key, &read_opt) {
            Some(v) => {
                let ts = decode_ts_from_value(v);
                if ts == start_ts {
                    let mut res = Value::with_capacity(v.len() - TIMESTAMP_LEN);
                    Some(res.append(value.as_slice()[TIMESTAMP_LEN..]))
                } else {
                    return None;
                }
            }
            None => return None,
        }
    }
}

impl MvccStorage for Storage {
    fn prewrite(&mut self, key: Key, value: Value, ts: u64) -> Result<(), String> {
        // TODO: check write conflict

        if self.mem_store.contains_key(&key) {
            return Err(String::from("key is locked"));
        }
        let _ = self.mem_store.insert(key, value, ts);
        Ok(())
    }

    fn commit(&mut self, &key: Key, start_ts: u64, commit_ts: u64) -> Result<(), ()> {
        match self.memtable.remove(key) {
            Some((timestamp, value)) => {
                if timestamp == start_ts {
                    // Pre-write result is ok
                    let mut write_opt = WriteOptions::new();
                    write_opt.set_timestamp(commit_ts);
                    let encoded_value = encode_ts_to_value(start_ts, &value);
                    self.db.Put(key, &encoded_value, write_opt);
                    return Ok(());
                } else {
                    // Rollback-ed or committed by other txn
                    self.memtable.insert(key, value);
                }
            }
            None => {}
        }

        // Find to see if it is committed or rollback-ed
        if let Some(_) = self.get_committed_version(key, commit_ts) {
            return Err(String::from("committed by other txn"));
        } else {
            return Err(String::from("rollback-ed by other txn"));
        }
    }

    fn rollback(&mut self, key: Key, ts: u64) -> Result<(), ()> {

    }

    fn get(&self, key: &Key, ts: u64) -> Result<Option<Value>, String> {
        if let Some((start_ts, _)) = self.mem_store.get(key) {
            if start_ts < ts {
                return Err(String::from("key is locked"));
            }
        }

        let mut read_opt = ReadOptions::new();
        read_opt.set_timestamp(ts);
        match self.db.Get(read_opt, key) {
            Some(v) => {
                let mut res = Value::with_capacity(v.len() - TIMESTAMP_LEN);
                res.append(v.as_slice()[TIMESTAMP_LEN..]);
                Ok(Some(res))
            }
            None => Ok(None),
        }
    }

    fn scan(&self, start: Key, end: Key, ts: u64) -> Result<Option<Vec<Value>>, String> {

    }
}

fn encode_ts_to_value(ts: u64, value: &Value) -> Value {
    let mut res = Value::with_capacity(TIMESTAMP_LEN + value.len());
    res.append(ts.to_be_bytes());
    res.append(value);
    res
}

fn decode_ts_from_value(value: &Value) -> u64 {
    u64::from_be_bytes(value.as_slice()[..8])
}