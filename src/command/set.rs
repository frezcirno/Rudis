use super::CommandParser;
use crate::client::Client;
use crate::dbms::DictValue;
use crate::frame::Frame;
use crate::object::RudisObject;
use crate::shared;
use bytes::{Bytes, BytesMut};
use dashmap::mapref::entry::Entry;
use std::collections::HashSet;
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

    pub async fn apply(self, dst: &mut Client) -> Result<()> {
        match dst.db.clone().entry(self.key) {
            Entry::Occupied(mut oe) => match &mut oe.get_mut().value {
                RudisObject::Set(s) => {
                    let mut added = 0;
                    for member in self.members {
                        if s.insert(member) {
                            added += 1;
                        }
                    }
                    dst.write_frame(&Frame::new_integer_from(added)).await?;
                    Ok(())
                }
                _ => {
                    dst.write_frame(&shared::wrong_type_err).await?;
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
                }
            },
            Entry::Vacant(ve) => {
                let mut s = HashSet::new();
                let len = self.members.len() as i64;
                for member in self.members {
                    s.insert(member);
                }
                ve.insert(DictValue::new(RudisObject::new_set_from(s), None));
                dst.write_frame(&Frame::new_integer_from(len)).await?;
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

    pub async fn apply(self, dst: &mut Client) -> Result<()> {
        match dst.db.clone().get_mut(&self.key) {
            Some(mut entry) => match &mut entry.value {
                RudisObject::Set(s) => {
                    let mut removed = 0;
                    for member in self.members {
                        if s.remove(&member) {
                            removed += 1;
                        }
                    }
                    dst.write_frame(&Frame::Integer(removed)).await?;
                    Ok(())
                }
                _ => {
                    dst.write_frame(&shared::wrong_type_err).await?;
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
                }
            },
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
