use super::CommandParser;
use std::io::Result;

#[derive(Debug, Clone)]
pub struct BgRewriteAof {}

impl BgRewriteAof {
    pub fn from(_frame: &mut CommandParser) -> Result<Self> {
        Ok(Self {})
    }
}
