use super::CommandParser;
use crate::db::Database;
use crate::{connection::Connection, frame::Frame};
use bytes::Bytes;
use std::io::{Error, ErrorKind, Result};

#[derive(Debug)]
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

    pub async fn apply(self, db: &Database, dst: &mut Connection) -> Result<()> {
        // Get the value from the shared database state
        let response = {
            let mut lock = db.lock();
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
