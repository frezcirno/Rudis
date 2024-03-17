use super::CommandParser;

use std::io::Result;

#[derive(Debug, Clone)]
pub struct Save {}

impl Save {
    pub fn from(_frame: &mut CommandParser) -> Result<Self> {
        Ok(Self {})
    }
}

#[derive(Debug, Clone)]
pub struct BgSave {}

impl BgSave {
    pub fn from(_frame: &mut CommandParser) -> Result<Self> {
        Ok(Self {})
    }
}
