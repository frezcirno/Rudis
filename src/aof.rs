use crate::command::Command;
use crate::dbms::DatabaseManager;
use crate::object::{RudisHash, RudisList, RudisObject, RudisSet, RudisString, RudisZSet};
use crate::server::Server;
use crate::shared;
use bytes::{Bytes, BytesMut};
use std::fmt::Display;
use std::io::Result;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum AofState {
    On,
    Off,
    WaitRewrite,
}

impl AofState {
    pub fn from(b: bool) -> Self {
        if b {
            AofState::On
        } else {
            AofState::Off
        }
    }
}

impl Display for AofState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AofState::On => write!(f, "on"),
            AofState::Off => write!(f, "off"),
            AofState::WaitRewrite => write!(f, "wait-rewrite"),
        }
    }
}

impl Default for AofState {
    fn default() -> Self {
        AofState::Off
    }
}

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum AofFsync {
    Always,
    Everysec,
    No,
}

impl Display for AofFsync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AofFsync::Always => write!(f, "always"),
            AofFsync::Everysec => write!(f, "everysec"),
            AofFsync::No => write!(f, "no"),
        }
    }
}

impl Default for AofFsync {
    fn default() -> Self {
        AofFsync::Everysec
    }
}

#[derive(Default, Debug)]
pub struct AofWriter {
    pub buffer: BytesMut,
}

impl AofWriter {
    fn extend_array(&mut self, len: usize) {
        shared::extend_array(&mut self.buffer, len);
    }

    fn extend_bulk_string<'a>(&mut self, s: impl Into<&'a [u8]>) {
        shared::extend_bulk_string(&mut self.buffer, s)
    }

    fn rewrite_string(&mut self, key: &Bytes, value: &RudisString) {
        // "SET key value"
        self.extend_array(3);
        self.extend_bulk_string(b"SET" as &[u8]);
        self.extend_bulk_string(&key[..]);
        self.extend_bulk_string(&value[..]);
    }

    fn rewrite_list(&mut self, key: &Bytes, list: &RudisList) {
        for item in list.iter() {
            // "RPUSH key value"
            self.extend_array(3);
            self.extend_bulk_string(b"RPUSH" as &[u8]);
            self.extend_bulk_string(&key[..]);
            self.extend_bulk_string(&item[..]);
        }
    }

    fn rewrite_set(&mut self, key: &Bytes, set: &RudisSet) {
        for member in set.iter() {
            // "SADD key member"
            self.extend_array(3);
            self.extend_bulk_string(b"SADD" as &[u8]);
            self.extend_bulk_string(&key[..]);
            self.extend_bulk_string(&member[..]);
        }
    }

    fn rewrite_hash(&mut self, key: &Bytes, hash: &RudisHash) {
        for (field, value) in hash.iter() {
            // "HSET key field value"
            self.extend_array(4);
            self.extend_bulk_string(b"HSET" as &[u8]);
            self.extend_bulk_string(&key[..]);
            self.extend_bulk_string(&field[..]);
            self.extend_bulk_string(&value[..]);
        }
    }

    fn rewrite_zset(&mut self, key: &Bytes, zset: &RudisZSet) {
        for (member, score) in zset.iter() {
            // "ZADD key score member"
            self.extend_array(4);
            self.extend_bulk_string(b"ZADD" as &[u8]);
            self.extend_bulk_string(&key[..]);
            self.extend_bulk_string(score.to_string().as_bytes());
            self.extend_bulk_string(&member[..]);
        }
    }
}

impl Deref for AofWriter {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl DerefMut for AofWriter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

impl DatabaseManager {
    async fn rewrite_append_only_file(&mut self, filename: &str) -> Result<()> {
        let now = shared::now_ms();

        // touch tmpfile
        let tmpfile = format!("temp-rewriteaof-bg-{}.aof", shared::get_pid());
        let mut file = File::create(&tmpfile).await?;

        let mut aof = AofWriter::default();

        // for db in self.snapshot().iter() {
        let db = self.get(0);
        // "SELECT index"
        aof.extend_array(2);
        aof.extend_bulk_string(b"SELECT" as &[u8]);
        aof.extend_bulk_string(db.index.to_string().as_bytes());

        for it in db.iter() {
            if it.is_expired() {
                continue;
            }

            match &it.value {
                RudisObject::String(s) => aof.rewrite_string(&it.key(), &s),
                RudisObject::List(l) => aof.rewrite_list(&it.key(), &l),
                RudisObject::Set(s) => aof.rewrite_set(&it.key(), &s),
                RudisObject::Hash(h) => aof.rewrite_hash(&it.key(), &h),
                RudisObject::ZSet(z) => aof.rewrite_zset(&it.key(), &z),
            }

            if let Some(expire) = &it.expire_at {
                // "PEXPIREAT key timestamp"
                aof.extend_array(3);
                aof.extend_bulk_string(b"PEXPIREAT" as &[u8]);
                aof.extend_bulk_string(&it.key()[..]);
                aof.extend_bulk_string(expire.to_string().as_bytes());
            }
        }
        // }

        file.write_all(&aof).await?;
        file.flush().await?;
        file.sync_data().await?;
        drop(file);

        // rename
        if let Err(e) = tokio::fs::rename(&tmpfile, filename).await {
            log::error!(
                "Error moving temp append only file on the final destination: {:?}",
                e
            );
            tokio::fs::remove_file(&tmpfile).await?;
            return Err(e);
        }

        log::info!("SYNC append only file rewrite performed");

        Ok(())
    }

    pub async fn rewrite_append_only_file_background(&mut self) -> Result<()> {
        let mut dbms = self.clone();
        self.aof_rewrite_task = Some(tokio::spawn(async move {
            // do the job
            let aof_bg_filename = format!("temp-rewriteaof-bg-{}.aof", shared::get_pid());
            dbms.rewrite_append_only_file(&aof_bg_filename).await;
        }));

        Ok(())
    }

    pub async fn feed_append_only_file(&mut self, cmd: Command, db_index: u32) -> Result<()> {
        let mut buf = BytesMut::new();

        if self.aof_selected_db != Some(db_index) {
            // emit "SELECT index"
            shared::extend_array(&mut buf, 2);
            shared::extend_bulk_string(&mut buf, b"SELECT" as &[u8]);
            shared::extend_bulk_string(&mut buf, db_index.to_string().as_bytes());
            self.aof_selected_db = Some(db_index);
        }

        match cmd {
            Command::Ping(_cmd) => {}
            Command::Quit(_cmd) => {}
            Command::Get(_cmd) => {}
            Command::Set(cmd) => buf.extend_from_slice(&cmd.rewrite()),
            Command::Append(cmd) => buf.extend_from_slice(&cmd.rewrite()),
            Command::Strlen(_cmd) => {}
            Command::Del(cmd) => buf.extend_from_slice(&cmd.rewrite()),
            Command::Exists(_cmd) => {}
            Command::Select(_cmd) => {}
            Command::Keys(_cmd) => {}
            Command::DbSize(_cmd) => {}
            Command::Shutdown(_cmd) => {}
            Command::Rename(cmd) => buf.extend_from_slice(&cmd.rewrite()),
            Command::Expire(cmd) => buf.extend_from_slice(&cmd.rewrite()),
            Command::ExpireAt(cmd) => buf.extend_from_slice(&cmd.rewrite()),
            Command::PExpire(cmd) => buf.extend_from_slice(&cmd.rewrite()),
            Command::PExpireAt(cmd) => buf.extend_from_slice(&cmd.rewrite()),
            Command::LPush(cmd) => buf.extend_from_slice(&cmd.rewrite()),
            Command::RPush(cmd) => buf.extend_from_slice(&cmd.rewrite()),
            Command::LPop(cmd) => buf.extend_from_slice(&cmd.rewrite()),
            Command::RPop(cmd) => buf.extend_from_slice(&cmd.rewrite()),
            Command::HSet(cmd) => buf.extend_from_slice(&cmd.rewrite()),
            Command::HGet(_cmd) => {}
            Command::SAdd(cmd) => buf.extend_from_slice(&cmd.rewrite()),
            Command::SRem(cmd) => buf.extend_from_slice(&cmd.rewrite()),
            Command::Save(_cmd) => {}
            Command::BgSave(_cmd) => {}
            Command::BgRewriteAof(_cmd) => {}
            Command::ConfigGet(_cmd) => {}
            Command::ConfigSet(_cmd) => {}
            Command::ConfigResetStat(_cmd) => {}
            Command::ConfigRewrite(_cmd) => {}
            Command::Unknown(_cmd) => {}
        }

        if self.config.read().await.aof_state == AofState::On {
            self.aof_buf.extend_from_slice(&buf);
        }

        if self.aof_rewrite_task.is_some() {
            // aof full rewrite in progress
            self.aof_rewrite_buffer_append(&buf);
        }

        Ok(())
    }

    fn aof_rewrite_buffer_append(&mut self, buf: &BytesMut) {
        self.aof_rewrite_buf_blocks.extend_from_slice(buf);
    }

    fn aof_rewrite_buffer_reset(&mut self) {
        self.aof_rewrite_buf_blocks.clear();
    }

    async fn aof_rewrite_buffer_write(&mut self, file: &mut File) -> Result<()> {
        let mut buf = BytesMut::new();
        std::mem::swap(&mut self.aof_rewrite_buf_blocks, &mut buf);

        if !buf.is_empty() {
            file.write(&buf).await?;
        }

        Ok(())
    }

    /// Flush the AOF buffer to disk
    pub async fn flush_append_only_file(&mut self) -> Result<()> {
        if self.aof_buf.is_empty() {
            return Ok(());
        }
        let aof_file = self.aof_file.as_mut().unwrap();

        if self.config.read().await.aof_fsync == AofFsync::Everysec {
            if self.config.read().await.aof_state == AofState::On {
                // aof.flush().await.unwrap();
            }
        }

        // write!
        match aof_file.write(&self.aof_buf).await {
            Ok(n_written) => {
                self.aof_current_size += n_written as u64;
                self.aof_last_write_status = true;
            }
            Err(e) => {
                log::error!("flush append only file error: {:?}", e);
                // TODO: handle error
            }
        }

        match self.config.read().await.aof_fsync {
            AofFsync::Always => {
                aof_file.sync_data().await?;
                self.aof_last_fsync = self.clock_ms;
            }
            AofFsync::Everysec => {
                if self.aof_last_fsync + 1000 < self.clock_ms {
                    aof_file.sync_data().await?;
                    self.aof_last_fsync = self.clock_ms;
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// Trigger by config set
    pub async fn start_append_only(&mut self) -> Result<()> {
        self.aof_last_fsync = shared::now_ms();

        assert_eq!(self.aof_file.is_none(), true, "AOF already open");

        self.aof_file = Some(
            OpenOptions::new()
                .write(true)
                .append(true)
                .open(&self.config.read().await.aof_filename)
                .await?,
        );

        self.rewrite_append_only_file_background().await?;

        self.config.write().await.aof_state = AofState::WaitRewrite;

        Ok(())
    }

    /// Trigger by config set
    pub fn stop_append_only(&mut self) {
        self.aof_file = None;
    }
}

impl Server {
    pub async fn background_rewrite_done_handler(
        self: &Arc<Self>,
        dbs: &mut DatabaseManager,
    ) -> Result<()> {
        log::info!("Background AOF rewrite terminated with success");

        // append new data generated during the rewrite
        let aof_bg_filename = format!("temp-rewriteaof-bg-{}.aof", shared::get_pid());
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&aof_bg_filename)
            .await?;

        dbs.aof_rewrite_buffer_write(&mut file).await?;

        log::info!(
            "Parent diff successfully flushed to the rewritten AOF ({} bytes)",
            dbs.aof_rewrite_buf_blocks.len()
        );

        tokio::fs::rename(&aof_bg_filename, &dbs.config.read().await.aof_filename).await?;

        if dbs.aof_file.is_some() {
            file.sync_data().await?;
            dbs.aof_file = Some(file);
            dbs.aof_selected_db = None;
            dbs.aof_buf.clear();
        }

        log::info!("Background AOF rewrite finished successfully");

        {
            // rewrite is done
            let mut cfg = self.config.write().await;
            if cfg.aof_state == AofState::WaitRewrite {
                cfg.aof_state = AofState::On;
            }
        }

        // cleanups
        dbs.aof_rewrite_buffer_reset();

        dbs.aof_rewrite_task = None;

        // schedule a new rewrite if needed
        if dbs.config.read().await.aof_state == AofState::WaitRewrite {
            dbs.aof_rewrite_scheduled = true;
        }

        Ok(())
    }
}
