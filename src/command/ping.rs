use super::CommandParser;
use crate::client::Client;
use crate::frame::Frame;
use crate::shared;
use bytes::Bytes;
use std::io::Result;

#[derive(Debug, Clone)]
pub struct Ping {}

impl Ping {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        Ok(Self {})
    }

    pub async fn apply(self, dst: &mut Client) -> Result<()> {
        // Write the response back to the client
        dst.write_frame(&shared::pong).await?;

        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct Echo {
    /// optional message to be returned
    msg: Option<Bytes>,
}

impl Echo {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        let msg = frame.next_string()?.map(Bytes::from);

        Ok(Self { msg })
    }

    pub async fn apply(self, dst: &mut Client) -> Result<()> {
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
