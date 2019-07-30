
pub mod user_timestamp;
pub mod tikv;
pub mod unistore;

type Key = Vec<u8>;
type Value = Vec<u8>;

pub trait TxnStorage {
    fn prewrite(&mut self, key: Key, value: Value, ts: u64) -> Result<(),()>;
    fn commit(&mut self, key: Key, ts: u64) -> Result<(), ()>;
    fn rollback(&mut self, key: Key, ts: u64) -> Result<(), ()>;
    fn get(&self, key: Key, ts: u64) -> Result<Option<Value>, String>;
    fn scan(&self, start: Key, end: Key, ts: u64) -> Result<Option<Vec<Value>>, String>;
}
