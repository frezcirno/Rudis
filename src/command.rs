mod db;
mod hash;
mod list;
mod ping;
mod set;
mod string;
mod unknown;
use crate::connection::Connection;
use crate::db::Database;
use crate::frame::Frame;
use bytes::Bytes;
use db::{DbSize, Del, Exists, Keys, Rename, Select, Shutdown};
use hash::{HGet, HSet};
use list::{ListPop, ListPush};
use ping::Ping;
use set::{SAdd, SRem};
use std::io::{Error, ErrorKind, Result};
use std::vec;
use string::{Append, Get, Set, Strlen};
use unknown::Unknown;

pub struct CommandParser {
    parts: vec::IntoIter<Frame>,
}

impl CommandParser {
    pub fn from(frame: Frame) -> CommandParser {
        let parts = match frame {
            Frame::Array(parts) => parts.into_iter(),
            _ => panic!("not an array frame"),
        };
        CommandParser { parts }
    }

    pub fn next(&mut self) -> Option<Frame> {
        self.parts.next()
    }

    pub fn remaining(&self) -> usize {
        self.parts.len()
    }

    pub fn has_next(&self) -> bool {
        self.remaining() > 0
    }

    pub fn next_string(&mut self) -> Result<Option<Bytes>> {
        if let Some(frame) = self.next() {
            match frame {
                Frame::Simple(s) => Ok(Some(s)),
                Frame::Bulk(b) => Ok(Some(b)),
                _ => Err(Error::new(ErrorKind::Other, "not a string")),
            }
        } else {
            Ok(None)
        }
    }

    pub fn next_integer(&mut self) -> Result<Option<u64>> {
        if let Some(frame) = self.next() {
            match frame {
                Frame::Integer(n) => Ok(Some(n)),
                _ => Err(Error::new(ErrorKind::Other, "not an integer")),
            }
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug)]
pub enum Command {
    Ping(Ping),

    Get(Get),
    Set(Set),
    Append(Append),
    Strlen(Strlen),

    Del(Del),
    Exists(Exists),
    Select(Select),
    Keys(Keys),
    DbSize(DbSize),
    Shutdown(Shutdown),
    Rename(Rename),

    LPush(ListPush),
    RPush(ListPush),
    LPop(ListPop),
    RPop(ListPop),

    HSet(HSet),
    HGet(HGet),

    SAdd(SAdd),
    SRem(SRem),

    // Publish(Publish),
    // Subscribe(Subscribe),
    // Unsubscribe(Unsubscribe),
    Unknown(Unknown),
}

impl Command {
    pub fn from(frame: Frame) -> Result<Command> {
        let mut parser = CommandParser::from(frame);

        let cmd = {
            let maybe_cmd = parser.next_string()?;
            if let Some(cmd) = maybe_cmd {
                cmd
            } else {
                return Err(Error::new(ErrorKind::InvalidInput, "No command provided"));
            }
        };

        let command = match &cmd.to_ascii_lowercase()[..] {
            b"ping" => Command::Ping(Ping::from(&mut parser)?),

            b"get" => Command::Get(Get::from(&mut parser)?),
            b"set" => Command::Set(Set::from(&mut parser)?),
            b"append" => Command::Append(Append::from(&mut parser)?),
            b"strlen" => Command::Strlen(Strlen::from(&mut parser)?),

            b"del" => Command::Del(Del::from(&mut parser)?),
            b"exists" => Command::Exists(Exists::from(&mut parser)?),
            b"select" => Command::Select(Select::from(&mut parser)?),
            b"keys" => Command::Keys(Keys::from(&mut parser)?),
            b"dbsize" => Command::DbSize(DbSize::from(&mut parser)?),
            b"shutdown" => Command::Shutdown(Shutdown::from(&mut parser)?),
            b"rename" => Command::Rename(Rename::from(&mut parser)?),

            b"lpush" => Command::LPush(ListPush::from(&mut parser, true)?),
            b"rpush" => Command::RPush(ListPush::from(&mut parser, true)?),
            b"lpop" => Command::LPop(ListPop::from(&mut parser, true)?),
            b"rpop" => Command::RPop(ListPop::from(&mut parser, false)?),

            b"hset" => Command::HSet(HSet::from(&mut parser)?),
            b"hget" => Command::HGet(HGet::from(&mut parser)?),

            b"sadd" => Command::SAdd(SAdd::from(&mut parser)?),
            b"srem" => Command::SRem(SRem::from(&mut parser)?),

            // b"publish" => Ok(Command::Publish(Publish {})),
            // b"subscribe" => Ok(Command::Subscribe(Subscribe {})),
            // b"unsubscribe" => Ok(Command::Unsubscribe(Unsubscribe {})),
            _ => Command::Unknown(Unknown::new(cmd)),
        };

        if parser.has_next() {
            return Err(Error::new(ErrorKind::Other, "Trailing bytes in the frame"));
        }

        Ok(command)
    }

    pub(crate) async fn apply(self, db: &Database, dst: &mut Connection) -> Result<()> {
        match self {
            Command::Ping(cmd) => cmd.apply(db, dst).await,

            Command::Get(cmd) => cmd.apply(db, dst).await,
            Command::Set(cmd) => cmd.apply(db, dst).await,
            Command::Append(cmd) => cmd.apply(db, dst).await,
            Command::Strlen(cmd) => cmd.apply(db, dst).await,

            Command::Del(cmd) => cmd.apply(db, dst).await,
            Command::Exists(cmd) => cmd.apply(db, dst).await,
            Command::Select(_) => unreachable!(),
            Command::Keys(cmd) => cmd.apply(db, dst).await,
            Command::DbSize(_) => unreachable!(),
            Command::Shutdown(cmd) => cmd.apply(db, dst).await,
            Command::Rename(cmd) => cmd.apply(db, dst).await,

            Command::LPush(cmd) => cmd.apply(db, dst).await,
            Command::RPush(cmd) => cmd.apply(db, dst).await,
            Command::LPop(cmd) => cmd.apply(db, dst).await,
            Command::RPop(cmd) => cmd.apply(db, dst).await,

            Command::HSet(cmd) => cmd.apply(db, dst).await,
            Command::HGet(cmd) => cmd.apply(db, dst).await,

            Command::SAdd(cmd) => cmd.apply(db, dst).await,
            Command::SRem(cmd) => cmd.apply(db, dst).await,

            // Publish(cmd) => cmd.apply(db, dst).await,
            // Subscribe(cmd) => cmd.apply(db, dst, shutdown).await,
            // `Unsubscribe` cannot be applied. It may only be received from the
            // context of a `Subscribe` command.
            // Unsubscribe(_) => Err("`Unsubscribe` is unsupported in this context".into()),
            Command::Unknown(cmd) => cmd.apply(db, dst).await,
        }
    }
}
