use crate::aof::{AofFsync, AofState, AofWriter};
use crate::config::Config;
use crate::object::RudisObject;
use crate::rdb::{AutoSave, Rdb};
use crate::shared;
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::ops::Deref;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::fs::File;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;

static mut ID: AtomicU32 = AtomicU32::new(0);

#[derive(Default)]
pub struct Databases {
    pub config: Arc<RwLock<Config>>,
    pub inner: Arc<Vec<Database>>,

    pub clock_ms: u64,

    pub save_params: Vec<AutoSave>,
    pub last_save_time: u64,
    pub rdb_save_task: Option<JoinHandle<()>>,

    pub dirty: u64,
    pub dirty_before_bgsave: u64,

    pub aof_state: AofState,
    pub aof_buf: AofWriter,
    pub aof_file: Option<File>,
    pub aof_current_size: u64,       // current size of the aof file
    pub aof_last_write_status: bool, // true if last write was ok

    pub aof_selected_db: u32,

    pub aof_fsync: AofFsync,
    pub aof_last_fsync: u64, // unit: ms

    pub aof_rewrite_task: Option<JoinHandle<()>>,
    pub aof_rewrite_buf_blocks: BytesMut,
    pub aof_rewrite_scheduled: bool,
}

impl Databases {
    pub async fn new(config: Arc<RwLock<Config>>) -> Databases {
        let db_num = config.read().await.db_num;
        let mut v = Vec::with_capacity(db_num);
        for _ in 0..db_num {
            v.push(Database::new());
        }
        Databases {
            config,
            inner: Arc::new(v),
            ..Default::default()
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn get(&self, index: usize) -> Database {
        self.inner[index].clone()
    }

    pub fn clone(&self) -> Databases {
        Databases {
            inner: self.inner.clone(),
            ..Default::default()
        }
    }

    pub async fn load_data_from_disk(&mut self) {
        if self.aof_state == AofState::On {
            // TODO
        } else {
            match File::open(&self.config.clone().read().await.rdb_filename).await {
                Ok(file) => {
                    if let Ok(()) = self.load(&mut Rdb::from_file(file)).await {
                        log::info!("DB loaded from disk: bad file format?");
                    } else {
                        log::error!("Error loading DB from disk");
                    }
                }
                Err(err) => {
                    if err.kind() != ErrorKind::NotFound {
                        log::error!("Error loading DB from disk: {:?}", err);
                    }
                }
            }
        }
    }

    pub fn should_save(&self) -> bool {
        let time_to_last_save = self.clock_ms - self.last_save_time;
        for saveparam in &self.save_params {
            if self.dirty >= saveparam.changes && time_to_last_save >= saveparam.seconds {
                return true;
            }
        }
        false
    }
}

impl Deref for Databases {
    type Target = Vec<Database>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct Database {
    pub index: u32,
    inner: Arc<Mutex<DatabaseInner>>,
}

pub struct DatabaseInner {
    pub dict: HashMap<Bytes, RudisObject>,
    pub expires: HashMap<Bytes, u64>, // millisecond timestamp
}

impl DatabaseInner {
    fn check_expired(&mut self, key: &Bytes) -> bool {
        if let Some(t) = self.expires.get(key) {
            let now = shared::now_ms();
            if now > *t {
                self.dict.remove(key);
                self.expires.remove(key);
                return true;
            }
        }

        false
    }

    pub fn lookup_read(&mut self, key: &Bytes) -> Option<&RudisObject> {
        self.check_expired(key);
        if let Some(v) = self.dict.get(key) {
            Some(v)
        } else {
            None
        }
    }

    pub fn lookup_write(&mut self, key: &Bytes) -> Option<&mut RudisObject> {
        self.check_expired(key);
        self.dict.get_mut(key)
    }

    pub fn remove(&mut self, key: &Bytes) -> Option<RudisObject> {
        self.check_expired(key);
        self.expires.remove(key);
        self.dict.remove(key)
    }

    pub fn len(&self) -> usize {
        self.dict.len()
    }

    pub fn keys(&self) -> Vec<Bytes> {
        self.dict.keys().cloned().collect()
    }

    pub fn contains_key(&self, key: &Bytes) -> bool {
        self.dict.contains_key(key)
    }

    pub fn insert(&mut self, key: Bytes, value: RudisObject, expire: Option<u64>) {
        self.dict.insert(key.clone(), value);

        if let Some(expire) = expire {
            let now = shared::now_ms();
            self.expires.insert(key, now + expire);
        }
    }

    pub fn rename(&mut self, key: &Bytes, new_key: Bytes) -> bool {
        if let Some(v) = self.dict.remove(key) {
            self.dict.insert(new_key.clone(), v);
            // rename expire
            if let Some(t) = self.expires.remove(key) {
                self.expires.insert(new_key, t);
            }
            true
        } else {
            false
        }
    }

    pub fn expire_at(&mut self, key: &Bytes, expire_at_ms: u64) -> bool {
        if self.dict.contains_key(key) {
            self.expires.insert(key.clone(), expire_at_ms);
            true
        } else {
            false
        }
    }
}

impl Database {
    pub fn new() -> Database {
        Database {
            index: unsafe { ID.fetch_add(1, Ordering::Relaxed) },
            inner: Arc::new(Mutex::new(DatabaseInner {
                dict: HashMap::new(),
                expires: HashMap::new(),
            })),
        }
    }

    pub fn clone(&self) -> Database {
        Database {
            index: self.index,
            inner: self.inner.clone(),
        }
    }

    pub async fn lock(&self) -> tokio::sync::MutexGuard<DatabaseInner> {
        self.inner.lock().await
    }
}
