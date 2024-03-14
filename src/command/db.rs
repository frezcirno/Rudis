use super::CommandParser;
use crate::db::Database;
use crate::object::RudisObject;
use crate::shared;
use crate::{connection::Connection, frame::Frame};
use bytes::Bytes;
use std::io::{Error, ErrorKind, Result};

#[derive(Debug)]
pub struct Del {
    pub keys: Vec<Bytes>,
}

impl Del {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        let mut keys = Vec::new();
        while let Some(key) = frame.next_string()? {
            keys.push(key);
        }

        if keys.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "DEL requires at least one key",
            ));
        }

        Ok(Self { keys })
    }

    pub async fn apply(self, db: &Database, dst: &mut Connection) -> Result<()> {
        let mut count = 0;
        {
            let mut db = db.lock().await;
            for key in self.keys {
                if db.remove(&key).is_some() {
                    count += 1;
                }
            }
        }

        dst.write_frame(&Frame::Integer(count)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Exists {
    pub key: Bytes,
}

impl Exists {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        let key = frame
            .next_string()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "EXISTS requires a key"))?;
        Ok(Self { key })
    }

    pub async fn apply(self, db: &Database, dst: &mut Connection) -> Result<()> {
        let response = {
            let mut db = db.lock().await;
            if db.lookup_read(&self.key).is_some() {
                Frame::Integer(1)
            } else {
                Frame::Integer(0)
            }
        };

        dst.write_frame(&response).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Select {
    pub index: u64,
}

impl Select {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        let index = frame
            .next_integer()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "SELECT requires a key"))?;
        Ok(Self { index })
    }
}

#[derive(Debug)]
pub struct Keys {
    pub pattern: Bytes,
}

impl Keys {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        let pattern = frame
            .next_string()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "KEYS requires a pattern"))?;
        Ok(Self { pattern })
    }

    pub async fn apply(self, db: &Database, dst: &mut Connection) -> Result<()> {
        let allkeys = &self.pattern[..] == b"*";
        let res = {
            let db = db.lock().await;
            Frame::Array(
                db.keys()
                    .iter()
                    .filter(|key| allkeys || self.pattern == key.as_ref())
                    .map(|key| Frame::Bulk(key.clone()))
                    .collect(),
            )
        };

        dst.write_frame(&res).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct DbSize {}

impl DbSize {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        Ok(Self {})
    }
}

#[derive(Debug)]
pub struct Type {
    pub key: Bytes,
}

impl Type {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        let key = frame
            .next_string()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "TYPE requires a key"))?;
        Ok(Self { key })
    }

    pub async fn apply(self, db: &Database, dst: &mut Connection) -> Result<()> {
        let response = {
            let mut db = db.lock().await;
            if let Some(obj) = db.lookup_read(&self.key) {
                match obj {
                    RudisObject::String(_) => Frame::Simple(Bytes::from_static(b"string")),
                    RudisObject::List(_) => Frame::Simple(Bytes::from_static(b"list")),
                    RudisObject::Set(_) => Frame::Simple(Bytes::from_static(b"set")),
                    RudisObject::ZSet(_) => Frame::Simple(Bytes::from_static(b"zset")),
                    RudisObject::Hash(_) => Frame::Simple(Bytes::from_static(b"hash")),
                }
            } else {
                Frame::Null
            }
        };

        dst.write_frame(&response).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Shutdown {
    pub save: bool,
}

impl Shutdown {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        // must specify SAVE or NOSAVE
        let save = match frame.next_string()? {
            Some(s) if s.eq_ignore_ascii_case(b"save") => true,
            Some(s) if s.eq_ignore_ascii_case(b"nosave") => false,
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "SHUTDOWN must specify either SAVE or NOSAVE",
                ))
            }
        };

        Ok(Self { save })
    }

    pub async fn apply(self, db: &Database, dst: &mut Connection) -> Result<()> {
        let response = Frame::Simple(Bytes::from_static(b"OK"));
        dst.write_frame(&response).await?;
        // TODO: actually shutdown
        Ok(())
    }
}

#[derive(Debug)]
pub struct Rename {
    pub key: Bytes,
    pub newkey: Bytes,
    // TODO: consider nx option
}

impl Rename {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        let key = frame
            .next_string()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "RENAME requires a key"))?;
        let newkey = frame
            .next_string()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "RENAME requires a newkey"))?;
        if key == newkey {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "RENAME requires different key and newkey",
            ));
        }
        Ok(Self { key, newkey })
    }

    pub async fn apply(self, db: &Database, dst: &mut Connection) -> Result<()> {
        let response = {
            let mut db = db.lock().await;
            if db.rename(&self.key, self.newkey) {
                Frame::Simple(Bytes::from_static(b"OK"))
            } else {
                Frame::Error(Bytes::from_static(b"ERR no such key"))
            }
        };

        dst.write_frame(&response).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Expire {
    pub key: Bytes,
    pub seconds: u64,
}

impl Expire {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        let key = frame
            .next_string()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "EXPIRE requires a key"))?;
        let seconds = frame
            .next_integer()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "EXPIRE requires a seconds"))?;
        Ok(Self { key, seconds })
    }

    pub async fn apply(self, db: &Database, dst: &mut Connection) -> Result<()> {
        let response = {
            let mut db = db.lock().await;
            if db.expire_at(&self.key, 1000 * self.seconds + shared::timestamp()) {
                Frame::Integer(1)
            } else {
                Frame::Integer(0)
            }
        };

        dst.write_frame(&response).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ExpireAt {
    pub key: Bytes,
    pub timestamp: u64,
}

impl ExpireAt {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        let key = frame
            .next_string()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "EXPIREAT requires a key"))?;
        let timestamp = frame
            .next_integer()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "EXPIREAT requires a timestamp"))?;
        Ok(Self { key, timestamp })
    }

    pub async fn apply(self, db: &Database, dst: &mut Connection) -> Result<()> {
        let response = {
            let mut db = db.lock().await;
            if db.expire_at(&self.key, 1000 * self.timestamp) {
                Frame::Integer(1)
            } else {
                Frame::Integer(0)
            }
        };

        dst.write_frame(&response).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct PExpire {
    pub key: Bytes,
    pub milliseconds: u64,
}

impl PExpire {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        let key = frame
            .next_string()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "PEXPIRE requires a key"))?;
        let milliseconds = frame.next_integer()?.ok_or_else(|| {
            Error::new(ErrorKind::InvalidInput, "PEXPIRE requires a milliseconds")
        })?;
        Ok(Self { key, milliseconds })
    }

    pub async fn apply(self, db: &Database, dst: &mut Connection) -> Result<()> {
        let response = {
            let mut db = db.lock().await;
            if db.expire_at(&self.key, self.milliseconds + shared::timestamp()) {
                Frame::Integer(1)
            } else {
                Frame::Integer(0)
            }
        };

        dst.write_frame(&response).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct PExpireAt {
    pub key: Bytes,
    pub timestamp: u64,
}

impl PExpireAt {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        let key = frame
            .next_string()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "PEXPIREAT requires a key"))?;
        let timestamp = frame
            .next_integer()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "PEXPIREAT requires a timestamp"))?;
        Ok(Self { key, timestamp })
    }

    pub async fn apply(self, db: &Database, dst: &mut Connection) -> Result<()> {
        let response = {
            let mut db = db.lock().await;
            if db.expire_at(&self.key, self.timestamp) {
                Frame::Integer(1)
            } else {
                Frame::Integer(0)
            }
        };

        dst.write_frame(&response).await?;
        Ok(())
    }
}