
use std::string::String;

pub mod user_timestamp;
pub mod tikv;
pub mod unistore;
pub mod memstore;
pub mod types;

type Key = Vec<u8>;
type Value = Vec<u8>;

pub type CfName = &'static str;

// Column families used by UniStore and user-timestamp models
pub const CF_OLD: CfName = "old";

// Column families used by TiKV
pub const CF_DEFAULT: CfName = "default";
pub const CF_WRITE: CfName = "write";
pub const CF_LOCK: CfName = "lock";

pub enum OpType {
    PUT,
    DEL,
    LOCK,
    ROLLBACK,
}

pub trait MvccStorage {
    fn prewrite(&mut self, key: Key, value: Value, op_type: OpType, start_ts: u64) -> Result<(), String>;
    fn commit(&mut self, key: Key, start_ts: u64, commit_ts: u64) -> Result<(), String>;
    fn rollback(&mut self, key: Key, start_ts: u64) -> Result<(), String>;
    fn get(&self, key: Key, ts: u64) -> Result<Option<Value>, String>;
    fn scan(&self, start: Key, end: Key, ts: u64) -> Result<Option<Vec<Value>>, String>;
}
