use crate::object::{RudisObject, RudisString};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

static mut ID: AtomicU32 = AtomicU32::new(0);

pub struct Database {
    inner: Arc<Mutex<DatabaseInner>>,
}

pub struct DatabaseInner {
    id: u32,
    dict: HashMap<Bytes, RudisObject>,
    expires: HashMap<Bytes, u64>, // millisecond timestamp
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

    pub fn lock(&self) -> std::sync::MutexGuard<DatabaseInner> {
        self.inner.lock().unwrap()
    }

    pub fn has_key(&self, key: &Bytes) -> bool {
        let mut inner = self.inner.lock().unwrap();
        inner.check_expired(key);
        inner.dict.contains_key(key)
    }

    pub fn set_key(&self, key: Bytes, value: Bytes, expire: Option<u64>) {
        let obj = RudisObject::String(RudisString::from(value));

        let mut inner = self.inner.lock().unwrap();
        inner.dict.insert(key.clone(), obj);

        if let Some(expire) = expire {
            let now = timestamps();
            inner.expires.insert(key, now + expire);
        }
    }
}
