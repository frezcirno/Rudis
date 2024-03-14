use crate::db::Databases;
use crate::object::{RudisHash, RudisList, RudisObject, RudisSet, RudisZSet};
use crate::shared;
use bytes::{Bytes, BytesMut};
use std::fmt::Display;
use std::io::Result;
use std::ops::{Deref, DerefMut};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

#[derive(PartialEq, Debug)]
pub(crate) enum AofState {
    On,
    Off,
    WaitRewrite,
}

impl Display for AofState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AofState::On => write!(f, "on"),
            AofState::Off => write!(f, "off"),
            AofState::WaitRewrite => write!(f, "wait-rewrite"),
        }
    }
}

impl Default for AofState {
    fn default() -> Self {
        AofState::Off
    }
}

#[derive(PartialEq, Debug)]
pub(crate) enum AofFsync {
    Always,
    Everysec,
    No,
}

impl Display for AofFsync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AofFsync::Always => write!(f, "always"),
            AofFsync::Everysec => write!(f, "everysec"),
            AofFsync::No => write!(f, "no"),
        }
    }
}

impl Default for AofFsync {
    fn default() -> Self {
        AofFsync::Everysec
    }
}

pub struct AofWriter {
    pub(crate) file: File,
    pub(crate) buffer: BytesMut,
}

impl AofWriter {
    pub async fn new(file: File) -> AofWriter {
        AofWriter {
            file,
            buffer: BytesMut::new(),
        }
    }

    pub async fn append(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
        if self.buffer.len() > 1024 * 1024 {
            self.flush().await;
        }
    }

    async fn save_bulk_string(&mut self, s: impl Into<&[u8]>) -> Result<()> {
        let s: &[u8] = s.into();
        self.write_u32(s.len() as u32).await?;
        self.write_all(&s).await?;
        Ok(())
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

    async fn rewrite_list(&mut self, key: &Bytes, list: &RudisList) -> Result<()> {
        for item in list.iter() {
            // "RPUSH key value"
            self.write_all(b"*3\r\n$5\r\nRPUSH\r\n").await?;
            self.save_bulk_string(&key[..]).await?;
            self.save_bulk_string(&item[..]).await?;
        }
        Ok(())
    }

    async fn rewrite_set(&mut self, key: &Bytes, set: &RudisSet) -> Result<()> {
        for member in set.iter() {
            // "SADD key member"
            self.write_all(b"*3\r\n$4\r\nSADD\r\n").await?;
            self.save_bulk_string(&key[..]).await?;
            self.save_bulk_string(&member[..]).await?;
        }
        Ok(())
    }

    async fn rewrite_hash(&mut self, key: &Bytes, hash: &RudisHash) -> Result<()> {
        for (field, value) in hash.iter() {
            // "HSET key field value"
            self.write_all(b"*4\r\n$4\r\nHSET\r\n").await?;
            self.save_bulk_string(&key[..]).await?;
            self.save_bulk_string(&field[..]).await?;
            self.save_bulk_string(&value[..]).await?;
        }
        Ok(())
    }

    async fn rewrite_zset(&mut self, key: &Bytes, zset: &RudisZSet) -> Result<()> {
        for (member, score) in zset.iter() {
            // "ZADD key score member"
            self.write_all(b"*4\r\n$4\r\nZADD\r\n").await?;
            self.save_bulk_string(&key[..]).await?;
            self.save_bulk_string(score.to_string().as_bytes()).await?;
            self.save_bulk_string(&member[..]).await?;
        }
        Ok(())
    }
}

impl Deref for AofWriter {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl DerefMut for AofWriter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}

impl Databases {
    pub async fn rewrite_aof(&mut self, file: File) -> Result<()> {
        let now = shared::timestamp();
        let mut aof = AofWriter::new(file).await;
        for db in self.iter() {
            // "SELECT index"
            aof.write_all(b"*2\r\n$6\r\nSELECT\r\n").await?;
            aof.save_bulk_string(db.index.to_string().as_bytes())
                .await?;

            let db = db.lock().await;
            for (key, value) in db.dict.iter() {
                let expire = db.expires.get(key);
                if expire.is_some_and(|e| *e < now) {
                    continue;
                }

                match value {
                    RudisObject::String(s) => {
                        // "SET key value"
                        aof.write_all(b"*3\r\n$3\r\nSET\r\n").await?;
                        aof.save_bulk_string(&key[..]).await?;
                        aof.save_bulk_string(&s[..]).await?;
                    }
                    RudisObject::List(l) => {
                        aof.rewrite_list(&key, l).await?;
                    }
                    RudisObject::Set(s) => {
                        aof.rewrite_set(&key, s).await?;
                    }
                    RudisObject::Hash(h) => {
                        aof.rewrite_hash(&key, h).await?;
                    }
                    RudisObject::ZSet(z) => {
                        aof.rewrite_zset(&key, z).await?;
                    }
                }

                if let Some(expire) = expire {
                    // "PEXPIREAT key timestamp"
                    aof.write_all(b"*3\r\n$9\r\nPEXPIREAT\r\n").await?;
                    aof.save_bulk_string(&key[..]).await?;
                    aof.save_bulk_string(expire.to_string().as_bytes()).await?;
                }
            }
        }
        aof.flush().await?;
        aof.sync_data().await?;

        Ok(())
    }

    pub async fn rewrite_aof_bg(&mut self) -> Result<()> {
        let file = File::create("appendonly.aof").await?;
        let mut dbs = self.clone();
        self.aof_rewrite_task = Some(tokio::spawn(async move {
            dbs.rewrite_aof(file).await;

            // clean up
            dbs.aof_rewrite_scheduled = false;
            dbs.aof_rewrite_task = None;
        }));
        Ok(())
    }

    pub async fn flush_append_only_file(&mut self) {
        // TODO: fsync etc.
    }
}
