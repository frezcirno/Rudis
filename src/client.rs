use crate::command::Command;
use crate::config::ConfigRef;
use crate::connection::Connection;
use crate::dbms::DatabaseRef;
use crate::server::Server;
use crate::shared;
use crate::{aof::AofOption, frame::Frame};
use std::io::Result;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
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
    pub config: ConfigRef,
    pub server: Arc<Server>,
    pub db: DatabaseRef,
    pub connection: Option<Connection>,
    pub address: SocketAddr,
    pub inner: RwLock<ClientInner>,
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

    pub fn select(&mut self, index: usize) -> Result<()> {
        if index >= 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid db index",
            ));
        }
        self.db = self.server.get(0);
        Ok(())
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<usize> {
        match self.connection {
            None => Ok(0), // fake client
            Some(ref mut connection) => connection.write_frame(frame).await,
        }
    }

    pub async fn handle_client(&mut self) -> Result<()> {
        loop {
            let connection = self.connection.as_mut().unwrap();

            let maybe_frame = tokio::select! {
                _ = self.quit_ch.recv() => {
                    log::debug!("server quit");
                    return Ok(());
                }
                maybe_err_frame = connection.read_frame() => {
                    // illegal frame
                    match maybe_err_frame {
                        Ok(f) => f,
                        Err(e) => {
                            self.write_frame(&shared::protocol_err).await?;
                            log::error!("read frame error: {:?}", e);
                            return Ok(());
                        }
                    }
                }
            };
            let frame = match maybe_frame {
                Some(frame) => frame,
                // connection closed
                None => return Ok(()),
            };

            let cmd = {
                let maybe_cmd = Command::from(frame);
                match maybe_cmd {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        self.write_frame(&shared::syntax_err).await?;
                        log::error!("parse command error: {:?}", e);
                        continue;
                    }
                }
            };

            // TODO: check auth

            log::debug!("client command: {:?}", cmd);

            // TODO: check memory

            // TODO: check last write disk status

            // TODO: check if the server is loading

            self.handle_command(cmd.clone()).await;

            // propagate
            self.propagate(cmd).await;
        }
    }

    async fn propagate(&mut self, cmd: Command) {
        if self.config.read().await.aof_state != AofOption::Off {
            self.server.feed_append_only_file(cmd, self.db.index).await;
        }
    }
}
