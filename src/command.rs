mod get;
mod ping;
mod set;
mod unknown;
use crate::connection::Connection;
use crate::db::Database;
use crate::frame::Frame;
use bytes::Bytes;
use get::Get;
use ping::Ping;
use set::Set;
use std::io::{Error, ErrorKind, Result};
use std::vec;
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
    Get(Get),
    Set(Set),
    // Publish(Publish),
    // Subscribe(Subscribe),
    // Unsubscribe(Unsubscribe),
    Ping(Ping),
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
            // Publish(cmd) => cmd.apply(db, dst).await,
            // Subscribe(cmd) => cmd.apply(db, dst, shutdown).await,
            Command::Unknown(cmd) => cmd.apply(db, dst).await,
            // `Unsubscribe` cannot be applied. It may only be received from the
            // context of a `Subscribe` command.
            // Unsubscribe(_) => Err("`Unsubscribe` is unsupported in this context".into()),
        }
    }
}
