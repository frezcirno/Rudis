use super::CommandParser;
use crate::dbms::DatabaseRef;
use crate::object::RudisObject;
use crate::shared;
use crate::{connection::Connection, frame::Frame};
use bytes::{Bytes, BytesMut};
use std::io::{Error, ErrorKind, Result};

#[derive(Debug, Clone)]
pub struct Get {
    pub key: Bytes,
}

impl Get {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        let key = frame
            .next_string()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "GET requires a key"))?;
        Ok(Self { key })
    }

    pub async fn apply(self, db: &DatabaseRef, dst: &mut Connection) -> Result<()> {
        // Get the value from the shared database state
        let response = {
            let mut lock = db.write().await;
            if let Some(value) = lock.lookup_read(&self.key) {
                // If a value is present, it is written to the client in "bulk"
                // format.
                value.serialize()
            } else {
                // If there is no value, `Null` is written.
                Frame::Null
            }
        };

        // Write the response back to the client
        dst.write_frame(&response).await?;

        Ok(())
    }
}

const REDIS_SET_NO_FLAGS: u32 = 0;
const REDIS_SET_NX: u32 = 1 << 0; /* Set if key not exists. */
const REDIS_SET_XX: u32 = 1 << 1; /* Set if key exists. */

#[derive(Debug, Clone)]
pub struct Set {
    pub key: Bytes,
    pub val: BytesMut,
    pub flags: u32,
    pub expire: Option<u64>, // milliseconds
}

impl Set {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        if frame.remaining() < 2 {
            return Err(Error::new(ErrorKind::Other, shared::syntax_err.to_string()));
        }
        // The first two elements of the array are the key and value
        let key = frame.next_string()?.unwrap();
        let val = frame.next_string()?.unwrap();

        let mut flags = REDIS_SET_NO_FLAGS;
        let mut expire = None;

        while frame.has_next() {
            let val = frame.next_string()?.unwrap().to_ascii_lowercase();
            if &val == b"nx" {
                flags |= REDIS_SET_NX;
            } else if &val == b"xx" {
                flags |= REDIS_SET_XX;
            } else if &val == b"ex" {
                // expire time in seconds
                let time = {
                    if let Some(maybe_time) = frame.next_integer()? {
                        maybe_time
                    } else {
                        return Err(Error::new(ErrorKind::Other, shared::syntax_err.to_string()));
                    }
                };
                expire = Some(time * 1000);
            } else if &val == b"px" {
                // expire time in milliseconds
                let time = {
                    if let Some(maybe_time) = frame.next_integer()? {
                        maybe_time
                    } else {
                        return Err(Error::new(ErrorKind::Other, shared::syntax_err.to_string()));
                    }
                };
                expire = Some(time);
            } else {
                // error
                return Err(Error::new(ErrorKind::Other, shared::syntax_err.to_string()));
            }
        }

        Ok(Self {
            key,
            val: BytesMut::from(&val[..]),
            flags,
            expire,
        })
    }

    pub async fn apply(self, db: &DatabaseRef, dst: &mut Connection) -> Result<()> {
        // validate nx and xx
        let mut db = db.write().await;

        if self.flags & REDIS_SET_NX != 0 && db.contains_key(&self.key)
            || self.flags & REDIS_SET_XX != 0 && !db.contains_key(&self.key)
        {
            drop(db);

            dst.write_frame(&shared::null_bulk).await.unwrap();
        } else {
            let value = RudisObject::new_string_from(self.val);
            db.insert(self.key, value, self.expire);
            drop(db);

            dst.write_frame(&shared::ok).await?;
        }
        Ok(())
    }

    pub fn rewrite(&self) -> BytesMut {
        let mut out = BytesMut::new();
        shared::extend_array(&mut out, 3 + if self.expire.is_some() { 2 } else { 0 });
        shared::extend_bulk_string(&mut out, b"SET" as &[u8]);
        shared::extend_bulk_string(&mut out, &self.key[..]);
        shared::extend_bulk_string(&mut out, &self.val[..]);
        if let Some(expire) = self.expire {
            shared::extend_bulk_string(&mut out, b"PX" as &[u8]);
            shared::extend_bulk_string(&mut out, expire.to_string().as_bytes());
        }
        out
    }
}

#[derive(Debug, Clone)]
pub struct Append {
    pub key: Bytes,
    pub value: Bytes,
}

impl Append {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        let key = frame
            .next_string()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "APPEND requires a key"))?;
        let value = frame
            .next_string()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "APPEND requires a value"))?;
        if value.len() > 512 * 1024 * 1024 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "value is too large (maximum 512MB)",
            ));
        }
        Ok(Self { key, value })
    }

    pub async fn apply(self, db: &DatabaseRef, dst: &mut Connection) -> Result<()> {
        // Append the value to the shared database state
        let response = {
            let mut lock = db.write().await;
            if let Some(value) = lock.lookup_write(&self.key) {
                if let RudisObject::String(s) = value {
                    s.value.extend_from_slice(&self.value);
                    Frame::Integer(s.len() as u64)
                } else {
                    Frame::Error(Bytes::from_static(
                        b"Operation against a key holding the wrong kind of value",
                    ))
                }
            } else {
                let mut_val: BytesMut = BytesMut::from(&self.value[..]);
                lock.dict
                    .insert(self.key.clone(), RudisObject::new_string_from(mut_val));
                Frame::Integer(self.value.len() as u64)
            }
        };

        // Write the response back to the client
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub fn rewrite(&self) -> BytesMut {
        let mut out = BytesMut::new();
        shared::extend_array(&mut out, 3);
        shared::extend_bulk_string(&mut out, b"APPEND" as &[u8]);
        shared::extend_bulk_string(&mut out, &self.key[..]);
        shared::extend_bulk_string(&mut out, &self.value[..]);
        out
    }
}

#[derive(Debug, Clone)]
pub struct Strlen {
    pub key: Bytes,
}

impl Strlen {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        let key = frame
            .next_string()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "STRLEN requires a key"))?;
        Ok(Self { key })
    }

    pub async fn apply(self, db: &DatabaseRef, dst: &mut Connection) -> Result<()> {
        // Get the value from the shared database state
        let response = {
            let mut lock = db.write().await;
            if let Some(value) = lock.lookup_read(&self.key) {
                if let RudisObject::String(s) = value {
                    Frame::Integer(s.len() as u64)
                } else {
                    Frame::Error(Bytes::from_static(
                        b"Operation against a key holding the wrong kind of value",
                    ))
                }
            } else {
                Frame::Integer(0)
            }
        };

        // Write the response back to the client
        dst.write_frame(&response).await?;

        Ok(())
    }
}
