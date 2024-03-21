use crate::aof::{AofOption, AofState};
use crate::client::{Client, ClientInner};
use crate::config::ConfigRef;
use crate::connection::Connection;
use crate::dbms::DatabaseRef;
use crate::rdb::{Rdb, RdbState};
use crate::shared;
use log;
use std::io::{Error, ErrorKind, Result};
use std::net::{Ipv4Addr, SocketAddr};
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::{File, OpenOptions};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, RwLock};
use tokio::time::sleep;

#[derive(Default)]
pub struct RudisServerInner {
    pub runid: String,
}

pub struct Server {
    pub config: ConfigRef,
    pub dbs: DatabaseRef, // only one database for now
    pub clock_ms: AtomicU64,
    pub rdb_state: RwLock<RdbState>,
    pub aof_state: RwLock<AofState>,
    pub inner: RwLock<RudisServerInner>,
    pub listener_fd: AtomicI32,
    pub quit_ch: broadcast::Sender<()>,
}

impl Server {
    pub async fn from_config(config: ConfigRef) -> Arc<Server> {
        let server = Arc::new(Server {
            config,

            dbs: DatabaseRef::new(),

            clock_ms: AtomicU64::new(shared::now_ms()),

            rdb_state: RwLock::new(RdbState::new()),

            aof_state: RwLock::new(AofState::new()),

            inner: RwLock::new(RudisServerInner {
                runid: shared::gen_runid(),
            }),
            listener_fd: AtomicI32::new(-1),
            quit_ch: broadcast::channel(1).0,
        });

        server.init().await.unwrap();

        server.load_data_from_disk().await;

        server
    }

    async fn init(self: &Arc<Self>) -> Result<()> {
        // handle aof
        if self.config.read().await.aof_state == AofOption::On {
            let mut aof_state = self.aof_state.write().await;
            aof_state.aof_file = Some(
                OpenOptions::new()
                    .write(true)
                    .append(true)
                    .create(true)
                    .open(&self.config.read().await.aof_filename)
                    .await?,
            );
        }

        Ok(())
    }

    pub fn get(&self, _index: usize) -> DatabaseRef {
        // self.inner[index].clone()
        self.dbs.clone()
    }

    pub async fn start(self: &Arc<Self>) -> Result<()> {
        {
            // start the cron loop
            let self_clone = self.clone();
            let mut quit_ch = self.quit_ch.subscribe();
            tokio::spawn(async move {
                let mut cronloops = 0;
                let mut period_ms = 1000 / self_clone.config.read().await.hz as u64;
                loop {
                    tokio::select! {
                        _ = quit_ch.recv() => {
                            break;
                        }
                        _ = sleep(Duration::from_millis(period_ms)) => {
                            match self_clone.server_cron(cronloops).await {
                                Some(next_ms) => period_ms = next_ms,
                                None => break,
                            }
                            cronloops += 1;
                        }
                    }
                }
            });
        }

        {
            // start the before sleep loop
            let self_clone = self.clone();
            let mut quit_ch = self.quit_ch.subscribe();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = quit_ch.recv() => {
                            break;
                        }
                        _ = sleep(Duration::from_millis(100)) => {
                            self_clone.before_sleep().await;
                        }
                    }
                }
            });
        }

        // start listening for connections
        let host = self
            .config
            .read()
            .await
            .bindaddr
            .parse::<Ipv4Addr>()
            .map_err(|_| Error::new(ErrorKind::Other, "Invalid host"))?;
        let port = self.config.read().await.port;
        log::info!("Listening on {}:{}", host, port);
        let listener = TcpListener::bind(SocketAddr::new(std::net::IpAddr::V4(host), port)).await?;
        self.listener_fd
            .store(listener.as_raw_fd(), Ordering::Relaxed);

        // main loop
        let mut quit_ch = self.quit_ch.subscribe();
        loop {
            tokio::select! {
                _ = quit_ch.recv() => {
                    break;
                }
                conn = listener.accept() => match conn {
                    Ok((connection, address)) => {
                        log::info!("Accepted connection from {}", address);
                        let mut c = Client {
                            config: self.config.clone(),
                            server: self.clone(),
                            db: self.get(0),
                            connection: Some(Connection::from(connection)),
                            address,
                            inner: RwLock::new(ClientInner {
                                name: String::new(),
                                last_interaction: 0,
                                flags: Default::default(),
                            }),
                            quit_ch: self.quit_ch.subscribe(),
                        };
                        tokio::spawn(async move {
                            c.serve().await;
                        });
                    }
                    Err(err) => {
                        // A "would block error" is returned if the operation
                        // is not ready, so we'll stop trying to accept
                        // connections.
                        if Self::would_block(&err) {
                            panic!("Would block error: {:?}", err);
                        }
                        panic!("Error: {:?}", err);
                    }
                }
            }
        }

        Ok(())
    }

    async fn before_sleep(&self) {
        let mut aof_state = self.aof_state.write().await;
        let config = self.config.clone();
        aof_state
            .flush_append_only_file(config, self.clock_ms.load(Ordering::Relaxed))
            .await;
    }

    async fn track_operations_per_second(&self) {
        // let mut inner = self.inner.write().await;
        // inner.dirty = 0;
    }

    async fn server_cron(&self, cronloops: u64) -> Option<u64> {
        let period_ms = 1000 / self.config.read().await.hz as u64;

        // update clock
        self.clock_ms.store(shared::now_ms(), Ordering::Relaxed);

        // 100 ms: track operations per second
        if 100 <= period_ms || cronloops % (100 / period_ms) == 0 {
            self.track_operations_per_second().await;
        }

        // 1000 ms: print stats info
        if 1000 <= period_ms || cronloops % (1000 / period_ms) == 0 {
            // for db in self.dbs.iter() {
            let db = self.get(0);
            let index = db.index;
            let size = db.dict.capacity();
            let used = db.dict.len();
            let vkeys = db.iter().filter(|it| it.is_volatile()).count();
            if used > 0 || vkeys > 0 {
                log::debug!(
                    "DB {}: {} keys ({} volatile) in {} slots RDB child pid: {} AOF child pid: {}",
                    index,
                    used,
                    vkeys,
                    size,
                    self.rdb_state.read().await.rdb_child_pid.unwrap_or(-1),
                    self.aof_state.read().await.aof_child_pid.unwrap_or(-1)
                );
            }
            // }
        }

        self.clients_cron(cronloops).await;

        self.databases_cron(cronloops).await;

        {
            let mut rdb_state = self.rdb_state.write().await;
            let mut aof_state = self.aof_state.write().await;

            if rdb_state.rdb_child_pid.is_none() && aof_state.aof_child_pid.is_none() {
                if aof_state.aof_rewrite_scheduled {
                    // schedule an AOF rewrite
                    self.rewrite_append_only_file_background(&mut aof_state)
                        .await;
                }
            }

            if rdb_state.rdb_child_pid.is_some() || aof_state.aof_child_pid.is_some() {
                // some background process is running
                let mut status = 0;
                let pid = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
                if pid > 0 {
                    log::info!("Process {} terminated with status {}", pid, status);

                    if Some(pid) == rdb_state.rdb_child_pid {
                        self.background_save_done_handler(&mut rdb_state);
                    } else if Some(pid) == aof_state.aof_child_pid {
                        self.background_rewrite_done_handler(&mut aof_state).await;
                    } else {
                        log::warn!("Unrecognized child pid: {}", pid);
                    }
                }
            } else {
                // no background process is running,
                // check if we need to start a background save
                if self.should_save(&rdb_state).await {
                    log::info!("Starting automatic RDB save");
                    self.background_save().await;
                }

                // check if we need to start a background rewrite
                // based on the configured percentage growth
                if rdb_state.rdb_child_pid.is_none()
                    && aof_state.aof_child_pid.is_none()
                    && aof_state.aof_rewrite_percent.is_some()
                    && aof_state.aof_current_size > aof_state.aof_rewrite_min_size
                {
                    let base = aof_state.aof_rewrite_base_size;
                    let growth = (aof_state.aof_current_size * 100 / base) - 100;
                    if growth >= aof_state.aof_rewrite_percent.unwrap() {
                        log::info!(
                        "Starting automatic AOF rewrite as AOF current size: {} is {}% larger than AOF base size: {}",
                        aof_state.aof_current_size,
                        growth,
                        base
                    );
                        self.rewrite_append_only_file_background(&mut aof_state)
                            .await;
                    }
                }
            }

            // 1000 ms: flush append only file
            if 1000 <= period_ms || cronloops % (1000 / period_ms) == 0 {
                aof_state
                    .flush_append_only_file(
                        self.config.clone(),
                        self.clock_ms.load(Ordering::Relaxed),
                    )
                    .await;
            }
        }

        Some(period_ms)
    }

    async fn clients_cron(&self, _cronloops: u64) {}

    async fn databases_cron(&self, _cronloops: u64) {}

    fn would_block(err: &Error) -> bool {
        err.kind() == ErrorKind::WouldBlock
    }

    async fn load_data_from_disk(self: &Arc<Self>) -> Result<()> {
        if self.config.read().await.aof_state == AofOption::On {
            log::info!("Loading DB from AOF");
            match self.load_append_only_file().await {
                Ok(()) => log::info!("DB loaded from AOF"),
                Err(e) => log::error!("Error loading DB from AOF: {:?}", e),
            }
        } else {
            log::info!("Loading DB from disk");
            match File::open(&self.config.clone().read().await.rdb_filename).await {
                Ok(mut file) => {
                    let mut rdb = Rdb::load_file(&mut file).await?;
                    match self.rdb_load(&mut rdb).await {
                        Ok(()) => log::info!("DB loaded from disk"),
                        Err(e) => log::error!("Error loading DB from disk: {:?}", e),
                    }
                }
                Err(err) => {
                    if err.kind() != ErrorKind::NotFound {
                        log::error!("Error loading DB from disk: {:?}", err);
                        panic!("Error loading DB from disk: {:?}", err);
                    }
                }
            }
        }

        Ok(())
    }
}
