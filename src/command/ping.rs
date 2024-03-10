use super::CommandParser;
use crate::connection::Connection;
use crate::db::Database;
use crate::frame::Frame;
use crate::shared;
use bytes::Bytes;
use std::io::Result;

#[derive(Debug, Default)]
pub struct Ping {
    /// optional message to be returned
    msg: Option<Bytes>,
}

impl Ping {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        let msg = frame.next_string()?.map(Bytes::from);

        Ok(Self { msg })
    }

    pub(crate) async fn apply(self, _db: &Database, dst: &mut Connection) -> Result<()> {
        let response = match self.msg {
            None => shared::pong,
            Some(msg) => Frame::Bulk(msg),
        };

        // Write the response back to the client
        dst.write_frame(&response).await?;

        Ok(())
    }
}
