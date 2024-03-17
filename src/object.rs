use crate::frame::Frame;
use bytes::{Bytes, BytesMut};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::{Deref, DerefMut};

#[derive(Debug, Clone)]
pub struct RudisString {
    pub value: BytesMut,
}

impl RudisString {
    pub fn from(value: BytesMut) -> RudisString {
        RudisString { value }
    }
}

impl Deref for RudisString {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

#[derive(Debug, Clone)]
pub struct RudisList {
    value: VecDeque<BytesMut>,
}

impl RudisList {
    pub fn new() -> RudisList {
        RudisList {
            value: VecDeque::new(),
        }
    }
}

impl Deref for RudisList {
    type Target = VecDeque<BytesMut>;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl DerefMut for RudisList {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

#[derive(Debug, Clone)]
pub struct RudisSet {
    value: HashSet<Bytes>,
}

impl RudisSet {
    pub fn new() -> RudisSet {
        RudisSet {
            value: HashSet::new(),
        }
    }
}

impl Deref for RudisSet {
    type Target = HashSet<Bytes>;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl DerefMut for RudisSet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

#[derive(Debug, Clone)]
pub struct RudisHash {
    value: HashMap<Bytes, BytesMut>,
}

impl RudisHash {
    pub fn new() -> RudisHash {
        RudisHash {
            value: HashMap::new(),
        }
    }
}

impl Deref for RudisHash {
    type Target = HashMap<Bytes, BytesMut>;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl DerefMut for RudisHash {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

#[derive(Debug, Clone)]
pub struct RudisZSet {
    value: BTreeMap<Bytes, f64>,
}

impl RudisZSet {
    pub fn new() -> RudisZSet {
        RudisZSet {
            value: BTreeMap::new(),
        }
    }
}

impl Deref for RudisZSet {
    type Target = BTreeMap<Bytes, f64>;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl DerefMut for RudisZSet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

#[derive(Debug, Clone)]
pub enum RudisObject {
    String(RudisString),
    List(RudisList),
    Set(RudisSet),
    Hash(RudisHash),
    ZSet(RudisZSet),
}

impl RudisObject {
    pub fn new_string() -> RudisObject {
        RudisObject::String(RudisString::from(BytesMut::new()))
    }

    pub fn new_string_from(value: BytesMut) -> RudisObject {
        RudisObject::String(RudisString::from(value))
    }

    pub fn get_string(&self) -> Option<&RudisString> {
        match self {
            RudisObject::String(value) => Some(value),
            _ => None,
        }
    }

    pub fn set_string(&mut self, value: RudisString) {
        *self = RudisObject::String(value);
    }

    pub fn as_string(&self) -> Option<&BytesMut> {
        match self {
            RudisObject::String(value) => Some(value),
            _ => None,
        }
    }

    pub fn new_list() -> RudisObject {
        RudisObject::List(RudisList::new())
    }

    pub fn new_list_from(value: VecDeque<BytesMut>) -> RudisObject {
        RudisObject::List(RudisList { value })
    }

    pub fn new_set() -> RudisObject {
        RudisObject::Set(RudisSet::new())
    }

    pub fn new_set_from(value: HashSet<Bytes>) -> RudisObject {
        RudisObject::Set(RudisSet { value })
    }

    pub fn new_hash() -> RudisObject {
        RudisObject::Hash(RudisHash::new())
    }

    pub fn new_hash_from(value: HashMap<Bytes, BytesMut>) -> RudisObject {
        RudisObject::Hash(RudisHash { value })
    }

    pub fn new_zset() -> RudisObject {
        RudisObject::ZSet(RudisZSet::new())
    }

    pub fn new_zset_from(value: BTreeMap<Bytes, f64>) -> RudisObject {
        RudisObject::ZSet(RudisZSet { value })
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
            RudisObject::String(value) => Frame::Bulk(<BytesMut as Clone>::clone(&value).freeze()),
            _ => Frame::Error("not implemented".into()),
        }
    }
}
