# MvccStore

A toy storage, used to compare different mvcc storage models.

## Models

### RocksDB's user timestamp

- There is 2 column families: default and old
- Prewrite store in memory with WAL
- Only committed data write to RocksDB
- Use RocksDB's user timestamp to implement MVCC, RocksDB support `Get(key, ts)`
- Move old versions into old CF when compaction

### TiKV
- There is 3 column families: default, lock and write
- Prewrite store in default and lock CF
- Commit delete lock and put write
- GC scan write CF

### Unistore
- There is 2 column families: latest and old
- Prewrite store in memory with WAL
- Only committed data write to RocksDB
- Move old versions into old CF when update


## Workload
- insert
- update
- delete
- point select
- range scan
