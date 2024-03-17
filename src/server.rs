use crate::client::{Client, ClientInner};
use crate::config::ConfigRef;
use crate::connection::Connection;
use crate::dbms::DatabaseManager;
use crate::rdb::Rdb;
use crate::shared;
use log;
use std::io::{Error, ErrorKind, Result};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, RwLock};
use tokio::time::sleep;

#[derive(Default)]
pub struct RudisServerInner {
    pub runid: String,
}

pub struct Server {
    pub config: ConfigRef,
    pub dbms: DatabaseManager,
    pub inner: RwLock<RudisServerInner>,
    pub quit_ch: broadcast::Sender<()>,
}

impl Server {
    pub async fn from_config(config: ConfigRef) -> Arc<Server> {
        let mut dbms = DatabaseManager::new(config.clone()).await;
        dbms.load_data_from_disk().await;

        let server = Server {
            dbms,
            config,
            inner: RwLock::new(RudisServerInner {
                runid: shared::gen_runid(),
            }),
            quit_ch: broadcast::channel(1).0,
        };

        Arc::new(server)
    }

    pub async fn start(self: &Arc<Self>) -> Result<()> {
        {
            let self_clone = self.clone();
            let mut dbms = self.dbms.clone();
            let _quit_ch = self.quit_ch.subscribe();
            tokio::spawn(async move {
                let mut cronloops = 0;
                let mut period_ms = 1000 / self_clone.config.read().await.hz as u64;
                loop {
                    sleep(Duration::from_millis(period_ms)).await;
                    match self_clone.server_cron(&mut dbms, cronloops).await {
                        Some(next_ms) => period_ms = next_ms,
                        None => break,
                    }
                    cronloops += 1;
                }
            });
        }

        {
            let self_clone = self.clone();
            let mut dbms = self.dbms.clone();
            tokio::spawn(async move {
                loop {
                    self_clone.before_sleep(&mut dbms).await;
                    sleep(Duration::from_millis(100)).await;
                }
            });
        }

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

        loop {
            match listener.accept().await {
                Ok((connection, address)) => {
                    log::info!("Accepted connection from {}", address);
                    let mut c = Client {
                        config: self.config.clone(),
                        dbms: self.dbms.clone(),
                        db: self.dbms.get(0),
                        connection: Connection::from(connection),
                        address,
                        inner: RwLock::new(ClientInner {
                            name: String::new(),
                            last_interaction: 0,
                            flags: Default::default(),
                        }),
                        quit: false.into(),
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

    async fn before_sleep(self: &Arc<Self>, dbs: &mut DatabaseManager) {
        dbs.flush_append_only_file().await;
    }

    async fn track_operations_per_second(self: &Arc<Self>) {
        // let mut inner = self.inner.write().await;
        // inner.dirty = 0;
    }

    async fn server_cron(
        self: &Arc<Self>,
        dbms: &mut DatabaseManager,
        cronloops: u64,
    ) -> Option<u64> {
        let period_ms = 1000 / self.config.read().await.hz as u64;

        // update clock
        dbms.clock_ms = shared::now_ms();

        // 100 ms: track operations per second
        if 100 <= period_ms || cronloops % (100 / period_ms) == 0 {
            self.track_operations_per_second().await;
        }

        // 500 ms: print stats info
        if 500 <= period_ms || cronloops % (500 / period_ms) == 0 {
            // for db in self.dbs.iter() {
            let db = self.dbms.get(0);
            let index = db.index;
            let db = db.read().await;
            let size = db.dict.capacity();
            let used = db.dict.len();
            let vkeys = db.expires.len();
            drop(db);
            if used > 0 || vkeys > 0 {
                log::info!(
                    "DB {}: {} keys ({} volatile) in {} slots",
                    index,
                    used,
                    vkeys,
                    size
                );
            }
            // }
        }

        self.clients_cron(cronloops).await;

        self.databases_cron(cronloops).await;

        if dbms.rdb_save_task.is_none() && dbms.aof_rewrite_task.is_none() {
            if dbms.aof_rewrite_scheduled {
                dbms.rewrite_append_only_file_background().await;
            }
        }

        if let Some(rdb_save_task) = &dbms.rdb_save_task {
            // clean up finished background save
            if rdb_save_task.is_finished() {
                self.background_save_done(dbms).await;
            }
        } else if let Some(aof_rewrite_task) = &dbms.aof_rewrite_task {
            // clean up finished background rewrite
            if aof_rewrite_task.is_finished() {
                self.background_rewrite_done_handler(dbms).await;
            }
        } else {
            // check if we need to start a background save
            if dbms.should_save().await {
                let file = File::create(&self.config.read().await.rdb_filename)
                    .await
                    .unwrap();
                let mut rdb = Rdb::from_file(file);
                let dbms_clone = dbms.clone();
                dbms.rdb_save_task = Some(tokio::spawn(async move {
                    dbms_clone.save(&mut rdb).await.unwrap();
                }));
            }

            // check if we need to rewrite the AOF
            // todo
        }

        // 1000 ms: flush append only file
        if 1000 <= period_ms || cronloops % (1000 / period_ms) == 0 {
            dbms.flush_append_only_file().await;
        }

        Some(period_ms)
    }

    async fn clients_cron(self: &Arc<Self>, _cronloops: u64) {}

    async fn databases_cron(self: &Arc<Self>, _cronloops: u64) {}

    fn would_block(err: &Error) -> bool {
        err.kind() == ErrorKind::WouldBlock
    }
}
