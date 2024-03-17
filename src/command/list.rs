use super::CommandParser;
use crate::dbms::DatabaseRef;
use crate::object::RudisObject;
use crate::shared;
use crate::{connection::Connection, frame::Frame};
use bytes::{Bytes, BytesMut};
use std::collections::VecDeque;
use std::io::{Error, ErrorKind, Result};

#[derive(Debug, Clone)]
pub struct ListPush {
    pub key: Bytes,
    pub values: Vec<BytesMut>,
    pub left: bool,
}

impl ListPush {
    pub fn from(frame: &mut CommandParser, left: bool) -> Result<Self> {
        let key = frame
            .next_string()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "LPUSH requires a key"))?;
        let mut values = Vec::new();
        while let Some(value) = frame.next_string()? {
            values.push(BytesMut::from(&value[..]));
        }
        if values.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "LPUSH requires at least one value",
            ));
        }
        Ok(Self { key, values, left })
    }

    async fn extend(l: &mut VecDeque<BytesMut>, values: Vec<BytesMut>, left: bool) {
        if left {
            for value in values {
                l.push_front(value);
            }
        } else {
            l.extend(values);
        }
    }

    pub async fn apply(self, db: &DatabaseRef, dst: &mut Connection) -> Result<()> {
        let mut db = db.write().await;

        match db.lookup_write(&self.key.clone()) {
            Some(RudisObject::List(l)) => {
                Self::extend(l, self.values, self.left).await;
                dst.write_frame(&Frame::Integer(l.len() as u64)).await?;
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
                let mut l = VecDeque::new();
                Self::extend(&mut l, self.values, self.left).await;
                let len = l.len();
                db.insert(self.key, RudisObject::new_list_from(l), None);
                dst.write_frame(&Frame::Integer(len as u64)).await?;
                Ok(())
            }
        }
    }

    pub fn rewrite(&self) -> BytesMut {
        // L/RPUSH key [value ...]
        let mut out = BytesMut::new();

        shared::extend_array(&mut out, 2 + self.values.len());

        // cmd
        shared::extend_bulk_string(
            &mut out,
            if self.left { b"LPUSH" } else { b"RPUSH" } as &[u8],
        );

        // key
        shared::extend_bulk_string(&mut out, &self.key[..]);

        // value
        for value in &self.values {
            shared::extend_bulk_string(&mut out, &value[..]);
        }

        out
    }
}

#[derive(Debug, Clone)]
pub struct ListPop {
    pub key: Bytes,
    pub left: bool,
}

impl ListPop {
    pub fn from(frame: &mut CommandParser, left: bool) -> Result<Self> {
        let key = frame
            .next_string()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "LPOP requires a key"))?;
        Ok(Self { key, left })
    }

    pub async fn apply(self, db: &DatabaseRef, dst: &mut Connection) -> Result<()> {
        let mut db = db.write().await;

        match db.lookup_write(&self.key.clone()) {
            Some(RudisObject::List(l)) => {
                let response = if self.left {
                    l.pop_front()
                } else {
                    l.pop_back()
                };
                if l.is_empty() {
                    db.remove(&self.key);
                }
                if let Some(value) = response {
                    dst.write_frame(&Frame::new_bulk_from(value).sealed()?)
                        .await?;
                } else {
                    dst.write_frame(&Frame::Null).await?;
                }
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
                dst.write_frame(&Frame::Null).await?;
                Ok(())
            }
        }
    }

    pub fn rewrite(&self) -> BytesMut {
        // L/RPOP key
        let mut out = BytesMut::new();

        shared::extend_array(&mut out, 2);

        // cmd
        shared::extend_bulk_string(&mut out, if self.left { b"LPOP" } else { b"RPOP" } as &[u8]);

        // key
        shared::extend_bulk_string(&mut out, &self.key[..]);

        out
    }
}
