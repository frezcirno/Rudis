use crate::frame::Frame;
use bytes::{Buf, BytesMut};
use std::io::Cursor;
use std::io::{Error, ErrorKind, Result};
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct Connection {
    pub stream: TcpStream,
    pub buffer: BytesMut,
}

const BUFFER_SIZE: usize = 16 * 1024;

impl Connection {
    pub fn from(stream: TcpStream) -> Connection {
        Connection {
            stream,
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
        }
    }

    // read a frame from the connection
    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            // log::debug!(
            //     "buffer cap = {}, len = {}, content = {:?}",
            //     self.buffer.capacity(),
            //     self.buffer.len(),
            //     &self.buffer
            // );

            if let Some(frame) = self.parse_frame().await? {
                return Ok(Some(frame));
            }

            // no enough data, need to read more
            // ensure the buffer has enough capacity
            let len = self.buffer.len();
            let max_read = BUFFER_SIZE - len;
            if max_read == 0 {
                return Err(Error::new(ErrorKind::Other, "frame is too large"));
            }

            let n_read = self.stream.read_buf(&mut self.buffer).await?;
            if n_read == 0 {
                // no left data, connection closed
                if self.buffer.is_empty() {
                    log::debug!("connection closed");
                    return Ok(None);
                }
                // connection reset by peer
                return Err(Error::new(ErrorKind::Other, "connection reset by peer"));
            }
        }
    }

    async fn parse_frame(&mut self) -> Result<Option<Frame>> {
        let mut cur = Cursor::new(&self.buffer);
        if let Some(frame) = Frame::parse(&mut cur)? {
            // if a frame is parsed successfully, advance the buffer
            self.buffer.advance(cur.position() as usize);
            return Ok(Some(frame));
        }
        Ok(None)
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> std::io::Result<usize> {
        self.stream.write(&frame.serialize()).await
    }
}
