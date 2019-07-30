
pub mod user_timestamp;
pub mod tikv;
pub mod unistore;
pub mod memstore;

type Key = Vec<u8>;
type Value = Vec<u8>;

pub type CfName = &'static str;
pub const CF_DEFAULT: CfName = "default";
pub const CF_OLD: CfName = "old";

pub trait MvccStorage {
    fn prewrite(&mut self, key: Key, value: Value, start_ts: u64) -> Result<(),()>;
    fn commit(&mut self, key: Key, start_ts: u64, commit_ts: u64) -> Result<(), ()>;
    fn rollback(&mut self, key: Key, start_ts: u64) -> Result<(), ()>;
    fn get(&self, key: Key, ts: u64) -> Result<Option<Value>, String>;
    fn scan(&self, start: Key, end: Key, ts: u64) -> Result<Option<Vec<Value>>, String>;
}
