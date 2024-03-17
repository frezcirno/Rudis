use crate::aof::AofState;
use crate::command::Command;
use crate::config::ConfigRef;
use crate::connection::Connection;
use crate::dbms::{DatabaseManager, DatabaseRef};
use crate::shared;
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
    pub config: ConfigRef,
    pub dbms: DatabaseManager,
    pub db: DatabaseRef,
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

    pub fn select(&mut self, index: usize) -> Result<()> {
        if index >= 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid db index",
            ));
        }
        self.db = self.dbms.get(0);
        Ok(())
    }

    pub async fn handle_client(&mut self) -> Result<()> {
        while !self.quit.load(Ordering::Relaxed) {
            let maybe_frame = tokio::select! {
                maybe_err_frame = self.connection.read_frame() => {
                    // illegal frame
                    match maybe_err_frame {
                        Ok(f) => f,
                        Err(e) => {
                            self.connection.write_frame(&shared::protocol_err).await?;
                            log::error!("read frame error: {:?}", e);
                            return Ok(());
                        }
                    }
                },
                _ = self.quit_ch.recv() => {
                    log::debug!("server quit");
                    return Ok(());
                },
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
                        self.connection.write_frame(&shared::syntax_err).await?;
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

            self.execute_cmd(cmd.clone()).await;

            // propagate
            self.propagate(cmd).await;
        }

        Ok(())
    }

    async fn propagate(&mut self, cmd: Command) {
        if self.config.read().await.aof_state != AofState::On {
            self.dbms.feed_append_only_file(cmd, self.db.index).await;
        }
    }
}
