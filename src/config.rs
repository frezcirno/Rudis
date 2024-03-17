use std::sync::Arc;
use std::{fmt::Display, ops::Deref};

use crate::{
    aof::{AofFsync, AofState},
    rdb::AutoSave,
};
use tokio::sync::RwLock;
use toml::Table;

#[derive(Clone, Debug)]
pub enum Verbosity {
    Quiet,
    Normal,
    Verbose,
    Debug,
}

impl Display for Verbosity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Verbosity::Quiet => write!(f, "quiet"),
            Verbosity::Normal => write!(f, "normal"),
            Verbosity::Verbose => write!(f, "verbose"),
            Verbosity::Debug => write!(f, "debug"),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ConfigRef {
    pub inner: Arc<RwLock<Config>>,
}

impl Deref for ConfigRef {
    type Target = Arc<RwLock<Config>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ConfigRef {
    pub fn new(config: Config) -> ConfigRef {
        ConfigRef {
            inner: Arc::new(RwLock::new(config)),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub bindaddr: String,
    pub port: u16,
    pub db_num: usize,
    pub hz: usize,
    pub verbosity: Verbosity,
    pub save_params: Vec<AutoSave>,
    pub rdb_filename: String,
    pub aof_state: AofState,
    pub aof_fsync: AofFsync,
    pub aof_filename: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            bindaddr: "0.0.0.0".to_owned(),
            port: 6379,
            rdb_filename: "dump.rdb".to_owned(),
            aof_state: AofState::Off,
            aof_fsync: AofFsync::Everysec,
            aof_filename: "appendonly.aof".to_owned(),
            db_num: 16,
            hz: 10,
            verbosity: Verbosity::Normal,
            save_params: vec![
                // 1 hour, 1 change
                AutoSave {
                    seconds: 60 * 60,
                    changes: 1,
                },
                // 10 minutes, 10 changes
                AutoSave {
                    seconds: 300,
                    changes: 10,
                },
                // 1 minute, 10000 changes
                AutoSave {
                    seconds: 60,
                    changes: 10000,
                },
            ],
        }
    }
}

impl Config {
    pub fn from_toml(file: &str) -> Config {
        let toml = std::fs::read_to_string(file).unwrap();
        let table = toml.parse::<Table>().unwrap();
        let bindaddr = table.get("bindaddr").unwrap().as_str().unwrap().to_string();
        let port = table.get("port").unwrap().as_integer().unwrap() as u16;
        let rdb_filename = table
            .get("rdb_filename")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let aof_filename = table
            .get("aof_filename")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let db_num = table.get("db_num").unwrap().as_integer().unwrap() as usize;
        let hz = table.get("hz").unwrap().as_integer().unwrap() as usize;
        let verbosity = match table.get("verbosity").unwrap().as_str().unwrap() {
            "quiet" => Verbosity::Quiet,
            "normal" => Verbosity::Normal,
            "verbose" => Verbosity::Verbose,
            "debug" => Verbosity::Debug,
            _ => panic!(),
        };
        let aof_state = match table.get("appendonly").unwrap().as_str().unwrap() {
            "yes" => AofState::On,
            "no" => AofState::Off,
            _ => panic!(),
        };
        let aof_fsync = match table.get("appendfsync").unwrap().as_str().unwrap() {
            "everysec" => AofFsync::Everysec,
            "always" => AofFsync::Always,
            "no" => AofFsync::No,
            _ => panic!(),
        };
        let save_params = table
            .get("save")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|v| {
                let mut iter = v.as_str().unwrap().split_whitespace();
                let seconds = iter.next().unwrap().parse().unwrap();
                let changes = iter.next().unwrap().parse().unwrap();
                AutoSave { seconds, changes }
            })
            .collect();
        Config {
            bindaddr,
            port,
            rdb_filename,
            aof_filename,
            db_num,
            hz,
            verbosity,
            save_params,
            aof_state,
            aof_fsync,
        }
    }
}
