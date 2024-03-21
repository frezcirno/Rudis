use crate::object::RudisObject;
use crate::server::Server;
use crate::shared;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use libc::pid_t;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Display;
use std::io::{Error, ErrorKind, Result, Write};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

const REDIS_RDB_TYPE_STRING: u8 = 0;
const REDIS_RDB_TYPE_LIST: u8 = 1;
const REDIS_RDB_TYPE_SET: u8 = 2;
const REDIS_RDB_TYPE_ZSET: u8 = 3;
const REDIS_RDB_TYPE_HASH: u8 = 4;
const REDIS_RDB_TYPE_HASH_ZIPMAP: u8 = 9;
const REDIS_RDB_TYPE_LIST_ZIPLIST: u8 = 10;
const REDIS_RDB_TYPE_SET_INTSET: u8 = 11;
const REDIS_RDB_TYPE_ZSET_ZIPLIST: u8 = 12;
const REDIS_RDB_TYPE_HASH_ZIPLIST: u8 = 13;

// 以 MS 计算的过期时间
const REDIS_RDB_OPCODE_EXPIRETIME_MS: u8 = 252;
// 以秒计算的过期时间
const REDIS_RDB_OPCODE_EXPIRETIME: u8 = 253;
// 选择数据库
const REDIS_RDB_OPCODE_SELECTDB: u8 = 254;
// 数据库的结尾（但不是 RDB 文件的结尾）
const REDIS_RDB_OPCODE_EOF: u8 = 255;

#[derive(Clone, Debug)]
pub struct AutoSave {
    pub seconds: u64,
    pub changes: u64,
}

impl Display for AutoSave {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.seconds, self.changes)
    }
}

#[derive(Debug)]
pub struct RdbState {
    pub last_save_time: u64,
    pub dirty: u64,
    pub dirty_before_bgsave: u64,
    pub save_params: Vec<AutoSave>,
    pub rdb_child_pid: Option<pid_t>,
}

impl RdbState {
    pub fn new() -> RdbState {
        RdbState {
            last_save_time: 0,
            dirty: 0,
            dirty_before_bgsave: 0,
            save_params: vec![],
            rdb_child_pid: None,
        }
    }
}

pub struct Rdb {
    buf: BytesMut,
}

impl Deref for Rdb {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        &self.buf
    }
}

impl DerefMut for Rdb {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buf
    }
}

impl Rdb {
    pub fn new() -> Rdb {
        Rdb {
            buf: BytesMut::new(),
        }
    }

    pub async fn load_file(file: &mut File) -> Result<Rdb> {
        let mut buf = BytesMut::with_capacity(64 * 1024 * 1024);
        let n_read = file.read_buf(&mut buf).await?;
        if n_read == 0 {
            return Err(Error::new(ErrorKind::UnexpectedEof, "RDB file is empty"));
        }
        if n_read == 64 * 1024 * 1024 {
            return Err(Error::new(ErrorKind::InvalidData, "RDB file too large"));
        }
        Ok(Rdb { buf })
    }

    fn save_object_type(&mut self, obj: &RudisObject) {
        match obj {
            RudisObject::String(_) => self.put_u8(REDIS_RDB_TYPE_STRING),
            RudisObject::List(_) => self.put_u8(REDIS_RDB_TYPE_LIST),
            RudisObject::Set(_) => self.put_u8(REDIS_RDB_TYPE_SET),
            RudisObject::Hash(_) => self.put_u8(REDIS_RDB_TYPE_HASH),
            RudisObject::ZSet(_) => self.put_u8(REDIS_RDB_TYPE_ZSET),
        }
    }

    fn save_string_object(&mut self, obj: &RudisObject) {
        match obj {
            RudisObject::String(s) => {
                self.put_u32(s.len() as u32);
                self.put_slice(&s);
            }
            _ => panic!(),
        }
    }

    fn load_string_object(&mut self) -> Result<BytesMut> {
        let len = self.get_u32() as usize;
        let mut buf = self.split_to(len);
        Ok(buf)
    }

    fn save_object(&mut self, obj: &RudisObject) {
        match obj {
            RudisObject::String(s) => {
                self.put_u32(s.len() as u32);
                self.put_slice(&s);
            }
            RudisObject::List(l) => {
                self.put_u32(l.len() as u32);
                for s in l.iter() {
                    self.put_u32(s.len() as u32);
                    self.put_slice(&s);
                }
            }
            RudisObject::Set(s) => {
                self.put_u32(s.len() as u32);
                for s in s.iter() {
                    self.put_u32(s.len() as u32);
                    self.put_slice(&s);
                }
            }
            RudisObject::ZSet(z) => {
                self.put_u32(z.len() as u32);
                for (k, v) in z.iter() {
                    self.put_u32(k.len() as u32);
                    self.put_slice(&k);
                    self.put_f64(*v);
                }
            }
            RudisObject::Hash(h) => {
                self.put_u32(h.len() as u32);
                for (k, v) in h.iter() {
                    self.put_u32(k.len() as u32);
                    self.put_slice(&k);
                    self.put_u32(v.len() as u32);
                    self.put_slice(&v);
                }
            }
        }
    }

    fn load_object(&mut self, obj_type: u8) -> Result<RudisObject> {
        match obj_type {
            REDIS_RDB_TYPE_STRING => {
                let s = self.load_string_object()?;
                Ok(RudisObject::new_string_from(s))
            }
            REDIS_RDB_TYPE_LIST => {
                let len = self.get_u32() as usize;
                let mut l = VecDeque::with_capacity(len);
                for _ in 0..len {
                    let s = self.load_string_object()?;
                    l.push_back(s);
                }
                Ok(RudisObject::new_list_from(l))
            }
            REDIS_RDB_TYPE_SET => {
                let len = self.get_u32() as usize;
                let mut s = HashSet::with_capacity(len);
                for _ in 0..len {
                    let st = self.load_string_object()?;
                    s.insert(st.freeze());
                }
                Ok(RudisObject::new_set_from(s))
            }
            REDIS_RDB_TYPE_ZSET => {
                let len = self.get_u32() as usize;
                let mut z = BTreeMap::new();
                for _ in 0..len {
                    let k = self.load_string_object()?.freeze();
                    let v = self.get_f64();
                    z.insert(k, v);
                }
                Ok(RudisObject::new_zset_from(z))
            }
            REDIS_RDB_TYPE_HASH => {
                let len = self.get_u32() as usize;
                let mut h = HashMap::with_capacity(len);
                for _ in 0..len {
                    let k = self.load_string_object()?.freeze();
                    let v = self.load_string_object()?;
                    h.insert(k, v);
                }
                Ok(RudisObject::new_hash_from(h))
            }
            _ => panic!(),
        }
    }

    pub fn save_key_value_pair(
        &mut self,
        key: &Bytes,
        value: &RudisObject,
        expire_at: Option<u64>,
        now: u64,
    ) {
        if let Some(expire_ms) = expire_at {
            if expire_ms < now {
                return;
            }
            self.put_u8(REDIS_RDB_OPCODE_EXPIRETIME_MS);
            self.put_u64(expire_ms);
        }

        self.save_object_type(value);

        // self.save_string_object(key);
        self.put_u32(key.len() as u32);
        self.put_slice(&key);

        self.save_object(value);
    }
}

impl Server {
    fn dump(&self) -> Rdb {
        let now: u64 = shared::now_ms();
        let mut rdb = Rdb::new();

        // write magic
        let magic = b"REDIS0006";
        rdb.put_slice(magic);

        // for db in self.iter() {
        let db = self.get(0);
        // write SELECTDB index
        rdb.put_u8(REDIS_RDB_OPCODE_SELECTDB);
        rdb.put_u32(db.index);

        for it in db.iter() {
            if it.is_expired() {
                continue;
            }
            rdb.save_key_value_pair(it.key(), &it.value, it.expire_at, now);
        }
        // }

        // write EOF
        rdb.put_u8(REDIS_RDB_OPCODE_EOF);
        rdb
    }

    pub async fn save(&self, file: &mut File) -> Result<()> {
        let rdb = self.dump();
        file.write_all(&rdb).await?;
        file.sync_all().await?;

        Ok(())
    }

    pub fn blocking_save(&self, rdb_filename: &str) -> Result<()> {
        let rdb = self.dump();
        let mut file = std::fs::File::create(rdb_filename)?;
        file.write_all(&rdb)?;
        file.sync_all()?;

        Ok(())
    }

    pub async fn background_save(&self) -> Result<()> {
        let rdb_filename = self.config.read().await.rdb_filename.clone();
        match unsafe { libc::fork() } {
            -1 => {
                return Err(Error::last_os_error());
            }
            0 => {
                // child process

                // close listener
                unsafe {
                    libc::close(self.listener_fd.load(Ordering::Relaxed));
                }

                let rdb = self.dump();

                let mut file = std::fs::File::create(rdb_filename).unwrap();
                file.write_all(&rdb).unwrap();
                file.sync_all().unwrap();

                std::process::exit(0);
            }
            child => {
                // parent process
                self.rdb_state.write().await.rdb_child_pid = Some(child);
            }
        }

        Ok(())
    }

    pub async fn rdb_load(self: &Arc<Self>, rdb: &mut Rdb) -> Result<()> {
        let now = shared::now_ms();

        // read magic
        let magic = b"REDIS0006";
        if &rdb[0..9] != magic {
            return Err(Error::new(ErrorKind::InvalidData, "Invalid RDB file magic"));
        }

        let mut db = None;

        loop {
            let mut expire_ms = None;
            let mut opcode = rdb.get_u8();

            match opcode {
                REDIS_RDB_OPCODE_EOF => {
                    break;
                }
                REDIS_RDB_OPCODE_SELECTDB => {
                    let db_index = rdb.get_u32() as usize;
                    if db_index >= 1 {
                        return Err(Error::new(ErrorKind::InvalidData, "Invalid DB index"));
                    }
                    db = Some(self.get(db_index));
                    continue;
                }
                REDIS_RDB_OPCODE_EXPIRETIME_MS => {
                    expire_ms = Some(rdb.get_u64());
                    opcode = rdb.get_u8();
                }
                REDIS_RDB_OPCODE_EXPIRETIME => {
                    expire_ms = Some(1000 * rdb.get_u32() as u64);
                    opcode = rdb.get_u8();
                }
                _ => {}
            }

            // now opcode is key type
            let key = rdb.load_string_object()?.freeze();
            let value = rdb.load_object(opcode)?;

            // check expire time
            if let Some(expire_ms) = expire_ms {
                if expire_ms < now {
                    continue;
                }
            }

            if let Some(db) = &mut db {
                db.insert(key, value, expire_ms);
            } else {
                return Err(Error::new(ErrorKind::InvalidData, "No SELECTDB"));
            }
        }

        Ok(())
    }
}

impl Server {
    pub fn background_save_done_handler(&self, rdb_state: &mut RwLockWriteGuard<'_, RdbState>) {
        log::info!("Background save done");

        rdb_state.rdb_child_pid = None;
    }
}

impl Server {
    pub async fn should_save(&self, rdb_state: &RwLockWriteGuard<'_, RdbState>) -> bool {
        let time_to_last_save = self.clock_ms.load(Ordering::Relaxed) - rdb_state.last_save_time;
        for saveparam in &self.config.read().await.save_params {
            if rdb_state.dirty >= saveparam.changes && time_to_last_save >= saveparam.seconds {
                return true;
            }
        }
        false
    }
}
