use super::CommandParser;
use crate::dbms::DatabaseRef;
use crate::object::RudisObject;
use crate::shared;
use crate::{connection::Connection, frame::Frame};
use bytes::{Bytes, BytesMut};
use std::io::{Error, ErrorKind, Result};

#[derive(Debug, Clone)]
pub struct SAdd {
    pub key: Bytes,
    pub members: Vec<Bytes>,
}

impl SAdd {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        let key = frame
            .next_string()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "SADD requires a key"))?;
        let mut members = vec![];
        while let Some(member) = frame.next_string()? {
            members.push(member);
        }
        Ok(Self { key, members })
    }

    pub async fn apply(self, db: &DatabaseRef, dst: &mut Connection) -> Result<()> {
        let mut db = db.write().await;

        match db.lookup_write(&self.key.clone()) {
            Some(RudisObject::Set(s)) => {
                let mut added = 0;
                for member in self.members {
                    if s.insert(member) {
                        added += 1;
                    }
                }
                dst.write_frame(&Frame::new_integer_from(added as u64))
                    .await?;
                Ok(())
            }
            Some(_) => {
                dst.write_frame(&shared::wrong_type_err).await?;
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }
            None => {
                let mut s = std::collections::HashSet::new();
                let len = self.members.len();
                for member in self.members {
                    s.insert(member);
                }
                db.insert(self.key.clone(), RudisObject::new_set_from(s), None);
                dst.write_frame(&Frame::new_integer_from(len as u64))
                    .await?;
                Ok(())
            }
        }
    }

    pub fn rewrite(&self) -> BytesMut {
        let mut out = BytesMut::new();
        // SADD key [member ...]
        shared::extend_array(&mut out, 2 + self.members.len());
        shared::extend_bulk_string(&mut out, b"SADD" as &[u8]);
        shared::extend_bulk_string(&mut out, &self.key[..]);
        for member in &self.members {
            shared::extend_bulk_string(&mut out, &member[..]);
        }
        out
    }
}

#[derive(Debug, Clone)]
pub struct SRem {
    pub key: Bytes,
    pub members: Vec<Bytes>,
}

impl SRem {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        let key = frame
            .next_string()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "SREM requires a key"))?;
        let mut members = vec![];
        while let Some(member) = frame.next_string()? {
            members.push(member);
        }
        Ok(Self { key, members })
    }

    pub async fn apply(self, db: &DatabaseRef, dst: &mut Connection) -> Result<()> {
        let mut db = db.write().await;

        match db.lookup_write(&self.key.clone()) {
            Some(RudisObject::Set(s)) => {
                let mut removed = 0;
                for member in self.members {
                    if s.remove(&member) {
                        removed += 1;
                    }
                }
                dst.write_frame(&Frame::Integer(removed as u64)).await?;
                Ok(())
            }
            Some(_) => {
                dst.write_frame(&shared::wrong_type_err).await?;
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }
            None => {
                dst.write_frame(&Frame::Integer(0)).await?;
                Ok(())
            }
        }
    }

    pub fn rewrite(&self) -> BytesMut {
        let mut out = BytesMut::new();
        // SREM key [member ...]
        shared::extend_array(&mut out, 2 + self.members.len());
        shared::extend_bulk_string(&mut out, b"SREM" as &[u8]);
        shared::extend_bulk_string(&mut out, &self.key[..]);
        for member in &self.members {
            shared::extend_bulk_string(&mut out, &member[..]);
        }
        out
    }
}
