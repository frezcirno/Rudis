use std::fmt::Display;

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

#[derive(Clone, Debug)]
pub struct Config {
    pub bindaddr: String,
    pub port: u16,
    pub rdb_filename: String,
    pub aof_filename: String,
    pub db_num: usize,
    pub hz: usize,
    pub verbosity: Verbosity,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            bindaddr: "0.0.0.0".to_owned(),
            port: 6379,
            rdb_filename: "dump.rdb".to_owned(),
            aof_filename: "appendonly.aof".to_owned(),
            db_num: 16,
            hz: 10,
            verbosity: Verbosity::Normal,
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
        Config {
            bindaddr,
            port,
            rdb_filename,
            aof_filename,
            db_num: 16,
            hz: 10,
            verbosity: Verbosity::Normal,
        }
    }
}
