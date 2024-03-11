use crate::object::RudisObject;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

static mut ID: AtomicU32 = AtomicU32::new(0);

pub struct Database {
    inner: Arc<Mutex<DatabaseInner>>,
}

pub struct DatabaseInner {
    pub id: u32,
    pub dict: HashMap<Bytes, RudisObject>,
    pub expires: HashMap<Bytes, u64>, // millisecond timestamp
}

fn timestamps() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

impl DatabaseInner {
    fn check_expired(&mut self, key: &Bytes) -> bool {
        if let Some(t) = self.expires.get(key) {
            let now = timestamps();
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
            let now = timestamps();
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
}

impl Database {
    pub fn new() -> Database {
        Database {
            inner: Arc::new(Mutex::new(DatabaseInner {
                id: unsafe { ID.fetch_add(1, Ordering::Relaxed) },
                dict: HashMap::new(),
                expires: HashMap::new(),
            })),
        }
    }

    pub fn clone(&self) -> Database {
        Database {
            inner: self.inner.clone(),
        }
    }

    pub async fn lock(&self) -> tokio::sync::MutexGuard<DatabaseInner> {
        self.inner.lock().await
    }
}
