use crate::command::Command;
use crate::connection::Connection;
use crate::db::Database;
use std::io::Result;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
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
    pub db: Database,
    pub connection: Connection,
    pub address: SocketAddr,
    pub inner: RwLock<ClientInner>,
    pub index: usize,
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

            cmd.apply(&self.db, &mut self.connection).await?;
        }

        Ok(())
    }
}
