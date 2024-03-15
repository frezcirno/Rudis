use super::CommandParser;
use crate::db::Database;
use crate::object::RudisObject;
use crate::{connection::Connection, frame::Frame};
use bytes::Bytes;
use std::io::{Error, ErrorKind, Result};

#[derive(Debug, Clone)]
pub struct BgRewriteAof {}

impl BgRewriteAof {
    pub fn from(frame: &mut CommandParser) -> Result<Self> {
        Ok(Self {})
    }
}
