use crate::connection::Connection;
use crate::db::Database;
use crate::frame::Frame;
use bytes::{BufMut, Bytes, BytesMut};
use std::io::Result;

/// Represents an "unknown" command. This is not a real `Redis` command.
#[derive(Debug, Clone)]
pub struct Unknown {
    command_name: Bytes,
}

impl Unknown {
    /// Create a new `Unknown` command which responds to unknown commands
    /// issued by clients
    pub fn new(command_name: Bytes) -> Unknown {
        Unknown { command_name }
    }

    /// Responds to the client, indicating the command is not recognized.
    ///
    /// This usually means the command is not yet implemented by `mini-redis`.
    pub async fn apply(self, _db: &Database, dst: &mut Connection) -> Result<()> {
        let mut response = BytesMut::new();
        response.put_slice(b"Error: ERR unknown command ");
        response.put_slice(&self.command_name);
        let response = Frame::Error(response.freeze());
        dst.write_frame(&response).await?;
        Ok(())
    }
}
