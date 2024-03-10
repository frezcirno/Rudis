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

impl Connection {
    pub fn from(stream: TcpStream) -> Connection {
        Connection {
            stream,
            buffer: BytesMut::zeroed(16 * 1024),
        }
    }

    // read a frame from the connection
    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            if let Some(frame) = self.parse_frame().await? {
                return Ok(Some(frame));
            }

            // no enough data, need to read more
            if self.stream.read(&mut self.buffer).await? == 0 {
                if self.buffer.is_empty() {
                    return Ok(None);
                }
                // Oops, connection close by peer
                return Err(Error::new(ErrorKind::Other, "connection reset by peer"));
            }
        }
    }

    async fn parse_frame(&mut self) -> Result<Option<Frame>> {
        let mut buf = Cursor::new(&self.buffer);
        if let Some(frame) = Frame::parse(&mut buf)? {
            // if a frame is parsed successfully, advance the buffer
            let len = buf.position() as usize;
            self.buffer.advance(len);
            return Ok(Some(frame));
        }
        Ok(None)
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> std::io::Result<usize> {
        self.stream.write(&frame.serialize()).await
    }
}
