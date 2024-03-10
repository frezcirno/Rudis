use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::frame::Frame;

pub struct RudisString {
    value: Bytes,
}

impl RudisString {
    pub fn from(value: Bytes) -> RudisString {
        RudisString { value }
    }

    pub fn set(&mut self, value: Bytes) {
        self.value = value;
    }

    pub fn get(&self) -> &Bytes {
        &self.value
    }
}

pub struct RudisList {
    value: Vec<Bytes>,
}

impl RudisList {
    pub fn new() -> RudisList {
        RudisList { value: Vec::new() }
    }

    pub fn push(&mut self, value: Bytes) {
        self.value.push(value);
    }

    pub fn pop(&mut self) -> Option<Bytes> {
        self.value.pop()
    }

    pub fn get(&self, index: usize) -> Option<&Bytes> {
        self.value.get(index)
    }
}

pub struct RudisSet {
    value: HashSet<Bytes>,
}

impl RudisSet {
    pub fn new() -> RudisSet {
        RudisSet {
            value: HashSet::new(),
        }
    }

    pub fn insert(&mut self, value: Bytes) {
        self.value.insert(value);
    }

    pub fn remove(&mut self, value: &Bytes) {
        self.value.remove(value);
    }

    pub fn contains(&self, value: &Bytes) -> bool {
        self.value.contains(value)
    }
}

pub struct RudisHash {
    value: HashMap<Bytes, Bytes>,
}

impl RudisHash {
    pub fn new() -> RudisHash {
        RudisHash {
            value: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: Bytes, value: Bytes) {
        self.value.insert(key, value);
    }

    pub fn get(&self, key: &Bytes) -> Option<&Bytes> {
        self.value.get(key)
    }

    pub fn remove(&mut self, key: &Bytes) {
        self.value.remove(key);
    }
}

pub struct RudisZSet {
    value: BTreeMap<Bytes, f64>,
}

impl RudisZSet {
    pub fn new() -> RudisZSet {
        RudisZSet {
            value: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: Bytes, score: f64) {
        self.value.insert(key, score);
    }

    pub fn remove(&mut self, key: &Bytes) {
        self.value.remove(key);
    }

    pub fn score(&self, key: &Bytes) -> Option<&f64> {
        self.value.get(key)
    }
}

pub enum RudisObject {
    String(RudisString),
    List(RudisList),
    Set(RudisSet),
    Hash(RudisHash),
    ZSet(RudisZSet),
}

impl RudisObject {
    pub fn new_list() -> RudisObject {
        RudisObject::List(RudisList::new())
    }

    pub fn new_set() -> RudisObject {
        RudisObject::Set(RudisSet::new())
    }

    pub fn new_hash() -> RudisObject {
        RudisObject::Hash(RudisHash::new())
    }

    pub fn new_zset() -> RudisObject {
        RudisObject::ZSet(RudisZSet::new())
    }

    pub fn get_type(&self) -> &str {
        match self {
            RudisObject::String(_) => "string",
            RudisObject::List(_) => "list",
            RudisObject::Set(_) => "set",
            RudisObject::Hash(_) => "hash",
            RudisObject::ZSet(_) => "zset",
        }
    }

    pub fn set_string(&mut self, value: RudisString) {
        *self = RudisObject::String(value);
    }

    pub fn set_list(&mut self, value: RudisList) {
        *self = RudisObject::List(value);
    }

    pub fn set_set(&mut self, value: RudisSet) {
        *self = RudisObject::Set(value);
    }

    pub fn set_hash(&mut self, value: RudisHash) {
        *self = RudisObject::Hash(value);
    }

    pub fn set_zset(&mut self, value: RudisZSet) {
        *self = RudisObject::ZSet(value);
    }

    pub fn get_string(&self) -> Option<&RudisString> {
        match self {
            RudisObject::String(value) => Some(value),
            _ => None,
        }
    }

    pub fn get_list(&self) -> Option<&RudisList> {
        match self {
            RudisObject::List(value) => Some(value),
            _ => None,
        }
    }

    pub fn get_set(&self) -> Option<&RudisSet> {
        match self {
            RudisObject::Set(value) => Some(value),
            _ => None,
        }
    }

    pub fn get_hash(&self) -> Option<&RudisHash> {
        match self {
            RudisObject::Hash(value) => Some(value),
            _ => None,
        }
    }

    pub fn get_zset(&self) -> Option<&RudisZSet> {
        match self {
            RudisObject::ZSet(value) => Some(value),
            _ => None,
        }
    }

    pub fn serialize(&self) -> Frame {
        match self {
            RudisObject::String(value) => Frame::Bulk(value.get().clone()),
            _ => Frame::Error("not implemented".into()),
        }
    }
}
