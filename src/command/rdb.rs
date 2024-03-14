use super::CommandParser;
use crate::db::Database;
use crate::object::RudisObject;
use crate::shared;
use crate::{connection::Connection, frame::Frame};
use bytes::{Bytes, BytesMut};
use std::io::{Error, ErrorKind, Result};

#[derive(Debug)]
pub struct Save {}

impl Save {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        Ok(Self {})
    }
}

#[derive(Debug)]
pub struct BgSave {}

impl BgSave {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        Ok(Self {})
    }
}
