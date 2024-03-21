use crate::object::RudisObject;
use crate::shared;
use bytes::Bytes;
use dashmap::mapref::entry::Entry;
use dashmap::mapref::one::{Ref, RefMut};
use dashmap::DashMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

#[derive(Default, Clone)]
pub struct DatabaseRef {
    pub index: u32,
    inner: Arc<Dict>,
}

static mut ID: AtomicU32 = AtomicU32::new(0);

impl DatabaseRef {
    pub fn new() -> DatabaseRef {
        DatabaseRef {
            index: unsafe { ID.fetch_add(1, Ordering::Relaxed) },
            inner: Arc::new(Dict::new()),
        }
    }
}

impl Deref for DatabaseRef {
    type Target = Arc<Dict>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for DatabaseRef {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[derive(Debug, Clone)]
pub struct DictValue {
    pub value: RudisObject,
    pub expire_at: Option<u64>,
}

impl DictValue {
    pub fn new(value: RudisObject, expire_at: Option<u64>) -> DictValue {
        DictValue { value, expire_at }
    }

    pub fn is_volatile(&self) -> bool {
        self.expire_at.is_some()
    }

    pub fn is_expired(&self) -> bool {
        if let Some(expire) = self.expire_at {
            let now = shared::now_ms();
            if now > expire {
                return true;
            }
        }
        false
    }
}

#[derive(Default, Clone, Debug)]
pub struct Dict {
    pub dict: DashMap<Bytes, DictValue>, // millisecond timestamp
}

impl Dict {
    pub fn new() -> Dict {
        Dict {
            dict: DashMap::new(),
        }
    }

    pub fn check_expired(&self, key: &Bytes) {
        if self.dict.get(key).map(|v| v.is_expired()).unwrap_or(false) {
            self.dict.remove(key);
        }
    }

    pub fn get(&self, key: &Bytes) -> Option<Ref<'_, Bytes, DictValue>> {
        self.check_expired(key);
        self.dict.get(key)
    }

    pub fn get_mut(&self, key: &Bytes) -> Option<RefMut<'_, Bytes, DictValue>> {
        self.check_expired(key);
        self.dict.get_mut(key)
    }

    pub fn remove(&self, key: &Bytes) -> Option<(Bytes, DictValue)> {
        self.check_expired(key);
        self.dict.remove(key)
    }

    pub fn entry(&self, key: Bytes) -> Entry<'_, Bytes, DictValue> {
        self.check_expired(&key);
        self.dict.entry(key)
    }

    pub fn contains_key(&self, key: &Bytes) -> bool {
        self.get(key).is_some()
    }

    pub fn insert(
        &self,
        key: Bytes,
        value: RudisObject,
        expire_at: Option<u64>,
    ) -> Option<DictValue> {
        self.dict.insert(key, DictValue::new(value, expire_at))
    }

    pub fn rename(&self, key: &Bytes, new_key: Bytes) -> bool {
        if let Some(v) = self.dict.remove(key) {
            self.dict.insert(new_key.clone(), v.1);
            true
        } else {
            false
        }
    }

    pub fn expire_at(&self, key: &Bytes, expire_at_ms: u64) -> bool {
        if let Some(mut v) = self.dict.get_mut(key) {
            v.expire_at = Some(expire_at_ms);
            true
        } else {
            false
        }
    }
}

impl Deref for Dict {
    type Target = DashMap<Bytes, DictValue>;

    fn deref(&self) -> &Self::Target {
        &self.dict
    }
}

impl DerefMut for Dict {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.dict
    }
}
