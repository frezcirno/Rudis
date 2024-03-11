use crate::client::{Client, ClientInner};
use crate::config::Config;
use crate::connection::Connection;
use crate::db::Database;
use log;
use std::borrow::BorrowMut;
use std::io::{Error, ErrorKind, Result};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, RwLock};

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

pub struct RudisServerInner {
    pub runid: String,
    pub hz: u64,
    pub dirty: u32,
}

pub struct Server {
    pub config: Config,
    pub dbs: Arc<Vec<Database>>,
    pub inner: RwLock<RudisServerInner>,
    quit_ch: broadcast::Sender<()>,
}

impl Server {
    pub async fn from_config(config: Config) -> Arc<Server> {
        // create databases
        let mut dbs = Vec::new();
        for _ in 0..config.db_num {
            dbs.push(Database::new());
        }

        let server = Server {
            config,
            dbs: Arc::new(dbs),
            inner: RwLock::new(RudisServerInner {
                runid: gen_runid(),
                hz: 10,
                dirty: 0,
            }),
            quit_ch: broadcast::channel(1).0,
        };

        Arc::new(server)
    }

    pub async fn start(self: &Arc<Self>) -> Result<()> {
        let self_clone = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(1)).await;
                self_clone.server_cron();
            }
        });

        let host = self
            .config
            .host
            .parse::<Ipv4Addr>()
            .map_err(|_| Error::new(ErrorKind::Other, "Invalid host"))?;
        let port = self.config.port;
        log::info!("Listening on {}:{}", host, port);
        let listener = TcpListener::bind(SocketAddr::new(std::net::IpAddr::V4(host), port)).await?;

        loop {
            match listener.accept().await {
                Ok((connection, address)) => {
                    log::info!("Accepted connection from {}", address);
                    let mut c = Client {
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

    fn server_cron(self: &Arc<Self>) -> Option<Duration> {
        None
    }

    fn would_block(err: &Error) -> bool {
        err.kind() == ErrorKind::WouldBlock
    }
}
