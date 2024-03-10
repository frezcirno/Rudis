use super::CommandParser;
use crate::connection::Connection;
use crate::db::Database;
use crate::shared;
use bytes::Bytes;
use std::io::{Error, ErrorKind, Result};

const REDIS_SET_NO_FLAGS: u32 = 0;
const REDIS_SET_NX: u32 = 1 << 0; /* Set if key not exists. */
const REDIS_SET_XX: u32 = 1 << 1; /* Set if key exists. */

#[derive(Debug)]
pub struct Set {
    pub key: Bytes,
    pub val: Bytes,
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
            val,
            flags,
            expire,
        })
    }

    pub async fn apply(self, db: &Database, dst: &mut Connection) -> Result<()> {
        {
            // validate nx and xx
            if self.flags & REDIS_SET_NX != 0 && db.has_key(&self.key)
                || self.flags & REDIS_SET_XX != 0 && !db.has_key(&self.key)
            {
                dst.write_frame(&shared::null_bulk).await.unwrap();
                return Ok(());
            }

            db.set_key(self.key, self.val, self.expire);
        }

        dst.write_frame(&shared::ok).await?;

        Ok(())
    }
}
