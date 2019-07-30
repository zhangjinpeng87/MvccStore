
use std::cmp;
use std::fs::{self, File};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use rocksdb::{
    ColumnFamilyOptions, DBCompressionType, DBOptions, Env, SliceTransform, DB, WriteOptions,
};
use storage::CF_DEFAULT;
use sys_info;
use util::file::{calc_crc32, copy_and_sync};

pub use rocksdb::CFHandle;

// Zlib and bzip2 are too slow.
const COMPRESSION_PRIORITY: [DBCompressionType; 3] = [
    DBCompressionType::Lz4,
    DBCompressionType::Snappy,
    DBCompressionType::Zstd,
];

pub fn get_cf_handle<'a>(db: &'a DB, cf: &str) -> Result<&'a CFHandle, String> {
    db.cf_handle(cf)
        .ok_or_else(|| format!("cf {} not found.", cf))
}

pub fn open_opt(
    opts: DBOptions,
    path: &str,
    cfs: Vec<&str>,
    cfs_opts: Vec<ColumnFamilyOptions>,
) -> Result<DB, String> {
    DB::open_cf(opts, path, cfs.into_iter().zip(cfs_opts).collect())
}

pub struct CFOptions<'a> {
    cf: &'a str,
    options: ColumnFamilyOptions,
}

impl<'a> CFOptions<'a> {
    pub fn new(cf: &'a str, options: ColumnFamilyOptions) -> CFOptions<'a> {
        CFOptions { cf, options }
    }
}

fn check_and_open(
    path: &str,
    mut db_opt: DBOptions,
    cfs_opts: Vec<CFOptions>,
) -> Result<DB, String> {
    // If db not exist, create it.
    if !db_exist(path) {
        db_opt.create_if_missing(true);

        let mut cfs_v = vec![];
        let mut cf_opts_v = vec![];
        if let Some(x) = cfs_opts.iter().find(|x| x.cf == CF_DEFAULT) {
            cfs_v.push(x.cf);
            cf_opts_v.push(x.options.clone());
        }
        let mut db = DB::open_cf(db_opt, path, cfs_v.into_iter().zip(cf_opts_v).collect())?;
        for x in cfs_opts {
            if x.cf == CF_DEFAULT {
                continue;
            }
            db.create_cf((x.cf, x.options))?;
        }

        return Ok(db);
    }

    db_opt.create_if_missing(false);

    // Open db.
    let mut cfs_v: Vec<&str> = Vec::new();
    let mut cfs_opts_v: Vec<ColumnFamilyOptions> = Vec::new();
    for cf in &existed {
        cfs_v.push(cf);
        match cfs_opts.iter().find(|x| x.cf == *cf) {
            Some(x) => {
                let mut tmp = CFOptions::new(x.cf, x.options.clone());
                cfs_opts_v.push(tmp.options);
            }
            None => {
                cfs_opts_v.push(ColumnFamilyOptions::new());
            }
        }
    }
    let cfds = cfs_v.into_iter().zip(cfs_opts_v).collect();
    let mut db = DB::open_cf(db_opt, path, cfds).unwrap();

    Ok(db)
}

pub fn new_engine_opt(path: &str, opts: DBOptions, cfs_opts: Vec<CFOptions>) -> Result<DB, String> {
    check_and_open(path, opts, cfs_opts)
}

pub fn db_exist(path: &str) -> bool {
    let path = Path::new(path);
    if !path.exists() || !path.is_dir() {
        return false;
    }

    // If path is not an empty directory, we say db exists. If path is not an empty directory
    // but db has not been created, DB::list_column_families will failed and we can cleanup
    // the directory by this indication.
    fs::read_dir(&path).unwrap().next().is_some()
}

pub struct FixedSuffixSliceTransform {
    pub suffix_len: usize,
}

impl FixedSuffixSliceTransform {
    pub fn new(suffix_len: usize) -> FixedSuffixSliceTransform {
        FixedSuffixSliceTransform { suffix_len }
    }
}

impl SliceTransform for FixedSuffixSliceTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        let mid = key.len() - self.suffix_len;
        let (left, _) = key.split_at(mid);
        left
    }

    fn in_domain(&mut self, key: &[u8]) -> bool {
        key.len() >= self.suffix_len
    }

    fn in_range(&mut self, _: &[u8]) -> bool {
        true
    }
}

pub struct FixedPrefixSliceTransform {
    pub prefix_len: usize,
}

impl FixedPrefixSliceTransform {
    pub fn new(prefix_len: usize) -> FixedPrefixSliceTransform {
        FixedPrefixSliceTransform { prefix_len }
    }
}

impl SliceTransform for FixedPrefixSliceTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        &key[..self.prefix_len]
    }

    fn in_domain(&mut self, key: &[u8]) -> bool {
        key.len() >= self.prefix_len
    }

    fn in_range(&mut self, _: &[u8]) -> bool {
        true
    }
}

pub struct NoopSliceTransform;

impl SliceTransform for NoopSliceTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        key
    }

    fn in_domain(&mut self, _: &[u8]) -> bool {
        true
    }

    fn in_range(&mut self, _: &[u8]) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocksdb::{
        ColumnFamilyOptions, DBOptions, EnvOptions, IngestExternalFileOptions, SstFileWriter,
        Writable, DB,
    };
    use storage::CF_DEFAULT;
    use tempdir::TempDir;

    #[test]
    fn test_check_and_open() {
        let path = TempDir::new("_util_rocksdb_test_check_column_families").expect("");
        let path_str = path.path().to_str().unwrap();

        // create db when db not exist
        let mut cfs_opts = vec![CFOptions::new(CF_DEFAULT, ColumnFamilyOptions::new())];
        let mut opts = ColumnFamilyOptions::new();
        opts.set_level_compaction_dynamic_level_bytes(true);
        cfs_opts.push(CFOptions::new("cf_dynamic_level_bytes", opts.clone()));
        {
            let mut db = check_and_open(path_str, DBOptions::new(), cfs_opts).unwrap();
            column_families_must_eq(path_str, vec![CF_DEFAULT, "cf_dynamic_level_bytes"]);
        }

        // add cf1.
        let cfs_opts = vec![
            CFOptions::new(CF_DEFAULT, opts.clone()),
            CFOptions::new("cf_dynamic_level_bytes", opts.clone()),
            CFOptions::new("cf1", opts.clone()),
        ];
        {
            let mut db = check_and_open(path_str, DBOptions::new(), cfs_opts).unwrap();
            column_families_must_eq(path_str, vec![CF_DEFAULT, "cf_dynamic_level_bytes", "cf1"]);
        }

        // drop cf1.
        let cfs_opts = vec![
            CFOptions::new(CF_DEFAULT, ColumnFamilyOptions::new()),
            CFOptions::new("cf_dynamic_level_bytes", ColumnFamilyOptions::new()),
        ];
        {
            let mut db = check_and_open(path_str, DBOptions::new(), cfs_opts).unwrap();
            column_families_must_eq(path_str, vec![CF_DEFAULT, "cf_dynamic_level_bytes"]);
        }

        // never drop default cf
        let cfs_opts = vec![];
        check_and_open(path_str, DBOptions::new(), cfs_opts).unwrap();
        column_families_must_eq(path_str, vec![CF_DEFAULT]);
    }

    fn column_families_must_eq(path: &str, excepted: Vec<&str>) {
        let opts = DBOptions::new();
        let cfs_list = DB::list_column_families(&opts, path).unwrap();

        let mut cfs_existed: Vec<&str> = cfs_list.iter().map(|v| v.as_str()).collect();
        let mut cfs_excepted: Vec<&str> = excepted.iter().map(|v| *v).collect();
        cfs_existed.sort();
        cfs_excepted.sort();
        assert_eq!(cfs_existed, cfs_excepted);
    }
}
