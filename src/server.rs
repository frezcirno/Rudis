use crate::client::{Client, ClientInner};
use crate::config::Config;
use crate::connection::Connection;
use crate::db::{Database, Databases};
use crate::rdb::Rdb;
use log;
use std::io::{Error, ErrorKind, Result};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, RwLock};
use tokio::time::sleep;

fn gen_runid() -> String {
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    let mut rng = thread_rng();
    let runid: String = (&mut rng)
        .sample_iter(&Alphanumeric)
        .take(40)
        .map(char::from)
        .collect();

    runid
}

#[derive(Default)]
pub struct RudisServerInner {
    pub runid: String,
}

pub struct Server {
    pub dbs: Databases,
    pub config: Arc<RwLock<Config>>,
    pub inner: RwLock<RudisServerInner>,
    pub quit_ch: broadcast::Sender<()>,
}

impl Server {
    pub async fn from_config(config: Config) -> Arc<Server> {
        let config = Arc::new(RwLock::new(config));

        let mut dbs = Databases::new(config.clone()).await;
        dbs.load_data_from_disk().await;

        let server = Server {
            dbs,
            config,
            inner: RwLock::new(RudisServerInner { runid: gen_runid() }),
            quit_ch: broadcast::channel(1).0,
        };

        Arc::new(server)
    }

    pub async fn start(self: &Arc<Self>) -> Result<()> {
        {
            let self_clone = self.clone();
            let mut dbs_clone = self.dbs.clone();
            let mut quit_ch = self.quit_ch.subscribe();
            tokio::spawn(async move {
                let mut cronloops = 0;
                let mut period_ms = 1000 / self_clone.config.read().await.hz as u64;
                loop {
                    sleep(Duration::from_millis(period_ms)).await;
                    match self_clone.server_cron(&mut dbs_clone, cronloops).await {
                        Some(next_ms) => period_ms = next_ms,
                        None => break,
                    }
                    cronloops += 1;
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
                        dbs: self.dbs.clone(),
                        index: 0,
                        db: self.dbs[0].clone(),
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

    async fn track_operations_per_second(self: &Arc<Self>) {
        // let mut inner = self.inner.write().await;
        // inner.dirty = 0;
    }

    async fn server_cron(self: &Arc<Self>, dbs: &mut Databases, cronloops: u64) -> Option<u64> {
        let period_ms = 1000 / self.config.read().await.hz as u64;

        // 100 ms: track operations per second
        if 100 <= period_ms || cronloops % (100 / period_ms) == 0 {
            self.track_operations_per_second().await;
        }

        // 500 ms: print stats info
        if 500 <= period_ms || cronloops % (500 / period_ms) == 0 {
            for db in self.dbs.iter() {
                let index = db.index;
                let db = db.lock().await;
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
            }
        }

        self.clients_cron(cronloops).await;

        self.databases_cron(cronloops).await;

        if dbs.rdb_save_task.is_none() && dbs.aof_rewrite_task.is_none() {
            if dbs.aof_rewrite_scheduled {
                dbs.rewrite_aof_bg().await;
            }
        }

        if let Some(rdb_save_task) = &dbs.rdb_save_task {
            // clean up finished background save
            if rdb_save_task.is_finished() {
                self.background_save_done(dbs).await;
            }
        } else if let Some(aof_rewrite_task) = &dbs.aof_rewrite_task {
            // clean up finished background rewrite
            if aof_rewrite_task.is_finished() {
                self.background_rewrite_done(dbs).await;
            }
        } else {
            // check if we need to start a background save
            if dbs.should_save() {
                let file = File::create(&self.config.read().await.rdb_filename)
                    .await
                    .unwrap();
                let mut rdb = Rdb::from_file(file);
                let dbs_clone = dbs.clone();
                dbs.rdb_save_task = Some(tokio::spawn(async move {
                    dbs_clone.save(&mut rdb).await.unwrap();
                }));
            }

            // check if we need to rewrite the AOF
            // todo
        }

        // 1000 ms: flush append only file
        if 1000 <= period_ms || cronloops % (1000 / period_ms) == 0 {
            dbs.flush_append_only_file().await;
        }

        Some(period_ms)
    }

    async fn background_save_done(self: &Arc<Self>, dbs: &mut Databases) {
        log::info!("Background save done");

        dbs.rdb_save_task = None;
    }

    async fn background_rewrite_done(self: &Arc<Self>, dbs: &mut Databases) {
        log::info!("Background rewrite done");

        dbs.aof_rewrite_task = None;
    }

    async fn clients_cron(self: &Arc<Self>, cronloops: u64) {}

    async fn databases_cron(self: &Arc<Self>, cronloops: u64) {}

    fn would_block(err: &Error) -> bool {
        err.kind() == ErrorKind::WouldBlock
    }
}
