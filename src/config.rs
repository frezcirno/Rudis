use std::env;
use toml::Table;

pub struct Config {
    pub host: String,
    pub port: u16,
    pub db_path: String,
    pub db_num: usize,
}

impl Config {
    pub fn new() -> Config {
        let host = env::var("RUDIS_HOST").unwrap_or("127.0.0.1".to_string());
        let port = env::var("RUDIS_PORT")
            .unwrap_or("6379".to_string())
            .parse()
            .unwrap();
        let db_path = env::var("RUDIS_DB_PATH").unwrap_or("rudis.db".to_string());
        Config {
            host,
            port,
            db_path,
            db_num: 16,
        }
    }

    pub fn from_toml(file: &str) -> Config {
        let toml = std::fs::read_to_string(file).unwrap();
        let table = toml.parse::<Table>().unwrap();
        let host = table.get("host").unwrap().as_str().unwrap().to_string();
        let port = table.get("port").unwrap().as_integer().unwrap() as u16;
        let db_path = table.get("db_path").unwrap().as_str().unwrap().to_string();
        Config {
            host,
            port,
            db_path,
            db_num: 16,
        }
    }

    pub fn db_path(&self) -> &str {
        &self.db_path
    }

    pub fn db_path_owned(&self) -> String {
        self.db_path.clone()
    }

    pub fn db_path_str(&self) -> &str {
        &self.db_path
    }
}
