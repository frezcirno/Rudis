use bytes::{Bytes, BytesMut};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Display;
use std::io::{Error, ErrorKind, Result};
use std::ops::{Deref, DerefMut};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::db::Databases;
use crate::object::RudisObject;
use crate::shared;

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

pub struct AutoSave {
    pub(crate) seconds: u64,
    pub(crate) changes: u64,
}

impl Display for AutoSave {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.seconds, self.changes)
    }
}

pub struct Rdb {
    pub file: File,
}

impl Rdb {
    pub fn from_file(file: File) -> Rdb {
        Rdb { file }
    }

    async fn save_object_type(&mut self, obj: &RudisObject) -> Result<()> {
        match obj {
            RudisObject::String(_) => self.write_u8(REDIS_RDB_TYPE_STRING).await,
            RudisObject::List(_) => self.write_u8(REDIS_RDB_TYPE_LIST).await,
            RudisObject::Set(_) => self.write_u8(REDIS_RDB_TYPE_SET).await,
            RudisObject::Hash(_) => self.write_u8(REDIS_RDB_TYPE_HASH).await,
            RudisObject::ZSet(_) => self.write_u8(REDIS_RDB_TYPE_ZSET).await,
        }
    }

    async fn save_string_object(&mut self, obj: &RudisObject) -> Result<()> {
        match obj {
            RudisObject::String(s) => {
                self.write_u32(s.len() as u32).await?;
                self.write_all(&s).await?;
                Ok(())
            }
            _ => panic!(),
        }
    }

    async fn load_string_object(&mut self) -> Result<BytesMut> {
        let len = self.read_u32().await? as usize;
        let mut buf = BytesMut::with_capacity(len);
        self.read_exact(&mut buf).await?;
        Ok(buf)
    }

    async fn save_object(&mut self, obj: &RudisObject) -> Result<()> {
        match obj {
            RudisObject::String(s) => {
                self.write_u32(s.len() as u32).await?;
                self.write_all(&s).await?;
            }
            RudisObject::List(l) => {
                self.write_u32(l.len() as u32).await?;
                for s in l.iter() {
                    self.write_u32(s.len() as u32).await?;
                    self.write_all(&s).await?;
                }
            }
            RudisObject::Set(s) => {
                self.write_u32(s.len() as u32).await?;
                for s in s.iter() {
                    self.write_u32(s.len() as u32).await?;
                    self.write_all(&s).await?;
                }
            }
            RudisObject::ZSet(z) => {
                self.write_u32(z.len() as u32).await?;
                for (k, v) in z.iter() {
                    self.write_u32(k.len() as u32).await?;
                    self.write_all(&k).await?;
                    self.write_f64(*v).await?;
                }
            }
            RudisObject::Hash(h) => {
                self.write_u32(h.len() as u32).await?;
                for (k, v) in h.iter() {
                    self.write_u32(k.len() as u32).await?;
                    self.write_all(&k).await?;
                    self.write_u32(v.len() as u32).await?;
                    self.write_all(&v).await?;
                }
            }
        }

        Ok(())
    }

    async fn load_object(&mut self, obj_type: u8) -> Result<RudisObject> {
        match obj_type {
            REDIS_RDB_TYPE_STRING => {
                let s = self.load_string_object().await?;
                Ok(RudisObject::new_string_from(s))
            }
            REDIS_RDB_TYPE_LIST => {
                let len = self.read_u32().await? as usize;
                let mut l = VecDeque::with_capacity(len);
                for _ in 0..len {
                    let s = self.load_string_object().await?;
                    l.push_back(s);
                }
                Ok(RudisObject::new_list_from(l))
            }
            REDIS_RDB_TYPE_SET => {
                let len = self.read_u32().await? as usize;
                let mut s = HashSet::with_capacity(len);
                for _ in 0..len {
                    let st = self.load_string_object().await?;
                    s.insert(st.freeze());
                }
                Ok(RudisObject::new_set_from(s))
            }
            REDIS_RDB_TYPE_ZSET => {
                let len = self.read_u32().await? as usize;
                let mut z = BTreeMap::new();
                for _ in 0..len {
                    let k = self.load_string_object().await?.freeze();
                    let v = self.read_f64().await?;
                    z.insert(k, v);
                }
                Ok(RudisObject::new_zset_from(z))
            }
            REDIS_RDB_TYPE_HASH => {
                let len = self.read_u32().await? as usize;
                let mut h = HashMap::with_capacity(len);
                for _ in 0..len {
                    let k = self.load_string_object().await?.freeze();
                    let v = self.load_string_object().await?;
                    h.insert(k, v);
                }
                Ok(RudisObject::new_hash_from(h))
            }
            _ => panic!(),
        }
    }

    pub async fn save_key_value_pair(
        &mut self,
        key: &Bytes,
        value: &RudisObject,
        expire: Option<u64>,
        now: u64,
    ) -> Result<()> {
        if let Some(expire_ms) = expire {
            if expire_ms < now {
                return Ok(());
            }
            self.write_u8(REDIS_RDB_OPCODE_EXPIRETIME_MS).await?;
            self.write_u64(expire_ms).await?;
        }

        self.save_object_type(value).await?;

        // self.save_string_object(key).await?;
        self.write_u32(key.len() as u32).await?;
        self.write_all(&key).await?;

        self.save_object(value).await?;
        Ok(())
    }
}

impl Deref for Rdb {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl DerefMut for Rdb {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}

impl Databases {
    pub async fn save(&self, rdb: &mut Rdb) -> Result<()> {
        let now = shared::timestamp();

        // write magic
        let magic = b"REDIS0006";
        rdb.write_all(magic).await?;

        for db in self.iter() {
            // write SELECTDB index
            rdb.write_u8(REDIS_RDB_OPCODE_SELECTDB).await?;
            rdb.write_u32(db.index).await?;

            let db = db.lock().await;
            for (key, value) in db.dict.iter() {
                let expire = db.expires.get(key);
                rdb.save_key_value_pair(key, value, expire.copied(), now)
                    .await?;
            }
        }

        // write EOF
        rdb.write_u8(REDIS_RDB_OPCODE_EOF).await?;

        // flush
        rdb.flush().await?;
        rdb.sync_all().await?;

        Ok(())
    }

    pub async fn load(&mut self, rdb: &mut Rdb) -> Result<()> {
        let now = shared::timestamp();

        // read magic
        let mut buf = [0u8; 9];
        rdb.read_exact(&mut buf).await?;

        let magic = b"REDIS0006";
        if &buf[0..9] != magic {
            panic!("Invalid RDB file magic");
        }

        let mut db = None;

        loop {
            let mut expire_ms = None;
            let mut opcode = rdb.read_u8().await?;

            match opcode {
                REDIS_RDB_OPCODE_EOF => {
                    break;
                }
                REDIS_RDB_OPCODE_SELECTDB => {
                    let db_index = rdb.read_u32().await? as usize;
                    if db_index >= self.len() {
                        return Err(Error::new(ErrorKind::InvalidData, "Invalid DB index"));
                    }
                    db = Some(self.get(db_index));
                    continue;
                }
                REDIS_RDB_OPCODE_EXPIRETIME_MS => {
                    expire_ms = Some(rdb.read_u64().await?);
                    opcode = rdb.read_u8().await?;
                }
                REDIS_RDB_OPCODE_EXPIRETIME => {
                    expire_ms = Some(1000 * rdb.read_u32().await? as u64);
                    opcode = rdb.read_u8().await?;
                }
                _ => {}
            }

            // now opcode is key type
            let key = rdb.load_string_object().await?.freeze();
            let value = rdb.load_object(opcode).await?;

            // check expire time
            if let Some(expire_ms) = expire_ms {
                if expire_ms < now {
                    continue;
                }
            }

            if let Some(db) = &mut db {
                db.lock().await.insert(key, value, expire_ms);
            } else {
                return Err(Error::new(ErrorKind::InvalidData, "No SELECTDB"));
            }
        }

        Ok(())
    }
}
