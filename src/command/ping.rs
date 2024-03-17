use super::CommandParser;
use crate::connection::Connection;
use crate::dbms::DatabaseRef;
use crate::frame::Frame;
use crate::shared;
use bytes::Bytes;
use std::io::Result;

#[derive(Debug, Default, Clone)]
pub struct Ping {
    /// optional message to be returned
    msg: Option<Bytes>,
}

impl Ping {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        let msg = frame.next_string()?.map(Bytes::from);

        Ok(Self { msg })
    }

    pub async fn apply(self, _db: &DatabaseRef, dst: &mut Connection) -> Result<()> {
        let response = match self.msg {
            None => shared::pong,
            Some(msg) => Frame::Bulk(msg),
        };

        // Write the response back to the client
        dst.write_frame(&response).await?;

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Quit {}

impl Quit {
    pub fn from(_frame: &mut CommandParser) -> Result<Self> {
        Ok(Self {})
    }
}
