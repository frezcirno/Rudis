use crate::command::Command;
use crate::connection::Connection;
use crate::db::Database;
use crate::frame::Frame;
use crate::shared;
use bytes::Bytes;
use std::io::Result;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

const REDIS_MULTI: u32 = 1 << 3;
const REDIS_CLOSE_AFTER_REPLY: u32 = 1 << 6;
const REDIS_DIRTY_EXEC: u32 = 1 << 12;

pub struct ClientInner {
    pub name: String,
    pub last_interaction: u64,
    pub flags: AtomicU32,
}

pub struct Client {
    pub dbs: Arc<Vec<Database>>,
    pub index: usize,
    pub db: Database,
    pub connection: Connection,
    pub address: SocketAddr,
    pub inner: RwLock<ClientInner>,
    pub quit: AtomicBool,
    pub quit_ch: broadcast::Receiver<()>,
}

impl Client {
    pub async fn serve(&mut self) {
        // set the stream to non-blocking mode
        // stream.set_nonblocking(true).unwrap();

        // enable TCP_NODELAY
        // self.stream.set_nodelay(true).unwrap();

        // set the keepalive timeout to 5 seconds
        // stream.set_keepalive(Some(Duration::from_secs(5))).unwrap();

        let _ = self.handle_client().await;
    }

    fn select(&mut self, index: usize) -> Result<()> {
        if index >= self.dbs.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid db index",
            ));
        }
        self.index = index;
        self.db = self.dbs[index].clone();
        Ok(())
    }

    pub async fn handle_client(&mut self) -> Result<()> {
        while !self.quit.load(Ordering::Relaxed) {
            let maybe_frame = tokio::select! {
                frame = self.connection.read_frame() => frame?,
                _ = self.quit_ch.recv() => {
                    log::debug!("client quit");
                    return Ok(());
                },
            };
            let frame = match maybe_frame {
                Some(frame) => frame,
                // connection closed
                None => return Ok(()),
            };

            let cmd = Command::from(frame)?;

            log::debug!("client command: {:?}", cmd);

            // process special commands
            match cmd {
                Command::Select(cmd) => {
                    if let Ok(()) = self.select(cmd.index as usize) {
                        self.connection.write_frame(&shared::ok).await?;
                    } else {
                        self.connection
                            .write_frame(&Frame::Error(Bytes::from_static(b"ERR invalid db index")))
                            .await?;
                    }

                    continue;
                }
                Command::DbSize(_) => {
                    let len = self.dbs.len();
                    self.connection
                        .write_frame(&Frame::Integer(len as u64))
                        .await?;
                    continue;
                }
                // Command::Quit(_) => {
                //     self.quit.store(true, Ordering::Relaxed);
                //     self.connection.write_frame(&shared::ok).await?;
                //     return Ok(());
                // }
                _ => {}
            };

            cmd.apply(&self.db, &mut self.connection).await?;
        }

        Ok(())
    }
}
