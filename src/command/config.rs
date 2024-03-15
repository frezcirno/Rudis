use super::CommandParser;
use crate::db::Database;
use crate::object::RudisObject;
use crate::{connection::Connection, frame::Frame};
use bytes::Bytes;
use std::io::{Error, ErrorKind, Result};

#[derive(Debug, Clone)]
pub struct ConfigSet {
    pub key: Bytes,
    pub value: Bytes,
}

impl ConfigSet {
    pub fn from(frame: &mut CommandParser) -> Result<ConfigSet> {
        let key = frame
            .next_string()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "CONFIG SET requires a key"))?;
        let value = frame
            .next_string()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "CONFIG SET requires a value"))?;
        Ok(ConfigSet { key, value })
    }
}

#[derive(Debug, Clone)]
pub struct ConfigGet {
    pub key: Bytes,
}

impl ConfigGet {
    pub fn from(frame: &mut CommandParser) -> Result<ConfigGet> {
        let key = frame
            .next_string()?
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "CONFIG GET requires a key"))?;
        Ok(ConfigGet { key })
    }
}

#[derive(Debug, Clone)]
pub struct ConfigResetStat {}

impl ConfigResetStat {
    pub fn from(frame: &mut CommandParser) -> Result<ConfigResetStat> {
        Ok(ConfigResetStat {})
    }
}

#[derive(Debug, Clone)]
pub struct ConfigRewrite {}

impl ConfigRewrite {
    pub fn from(frame: &mut CommandParser) -> Result<ConfigRewrite> {
        Ok(ConfigRewrite {})
    }
}
