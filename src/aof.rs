use crate::client::{Client, ClientInner};
use crate::command::Command;
use crate::config::ConfigRef;
use crate::frame::Frame;
use crate::object::{RudisHash, RudisList, RudisObject, RudisSet, RudisString, RudisZSet};
use crate::server::Server;
use crate::shared;
use bytes::{Buf, Bytes, BytesMut};
use libc::pid_t;
use std::fmt::Display;
use std::io::Write;
use std::io::{Cursor, Result};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;
use tokio::sync::RwLockWriteGuard;

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum AofOption {
    On,
    Off,
    WaitRewrite,
}

impl AofOption {
    pub fn from(b: bool) -> Self {
        if b {
            AofOption::On
        } else {
            AofOption::Off
        }
    }
}

impl Display for AofOption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AofOption::On => write!(f, "on"),
            AofOption::Off => write!(f, "off"),
            AofOption::WaitRewrite => write!(f, "wait-rewrite"),
        }
    }
}

impl Default for AofOption {
    fn default() -> Self {
        AofOption::Off
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

const REDIS_AOF_REWRITE_MIN_SIZE: u64 = 64 * 1024 * 1024;
const REDIS_AOF_REWRITE_PERC: u64 = 100;

#[derive(Default, Debug)]
pub struct AofState {
    pub aof_buf: AofWriter,
    pub aof_file: Option<File>,
    pub aof_current_size: u64,       // current size of the aof file
    pub aof_last_write_status: bool, // true if last write was ok
    pub aof_child_pid: Option<pid_t>,
    pub aof_rewrite_min_size: u64,
    pub aof_rewrite_buf_blocks: BytesMut,
    pub aof_rewrite_scheduled: bool, // RDB save is running and "BgRewriteAof"
    pub aof_selected_db: Option<u32>,
    pub aof_last_fsync: u64, // unit: ms
    pub aof_rewrite_percent: Option<u64>,
    pub aof_rewrite_base_size: u64, // size of the AOF file from the latest rewrite
}

impl AofState {
    pub fn new() -> AofState {
        AofState {
            aof_buf: AofWriter::new(),
            aof_file: None,
            aof_current_size: 0,
            aof_last_write_status: true,
            aof_child_pid: None,
            aof_rewrite_min_size: REDIS_AOF_REWRITE_MIN_SIZE,
            aof_rewrite_buf_blocks: BytesMut::new(),
            aof_rewrite_scheduled: false,
            aof_selected_db: None,
            aof_last_fsync: 0,
            aof_rewrite_percent: Some(REDIS_AOF_REWRITE_PERC),
            aof_rewrite_base_size: 0,
        }
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
    pub async fn flush_append_only_file(&mut self, config: ConfigRef, clock_ms: u64) -> Result<()> {
        if self.aof_buf.is_empty() {
            return Ok(());
        }
        let aof_file = self.aof_file.as_mut().unwrap();

        let config = config.read().await;
        let aof_state = config.aof_state;
        let aof_fsync = config.aof_fsync;
        drop(config);

        if aof_fsync == AofFsync::Everysec {
            if aof_state == AofOption::On {
                // aof.flush().await.unwrap();
            }
        }

        // write!
        match aof_file.write(&self.aof_buf).await {
            Ok(n_written) => {
                self.aof_current_size += n_written as u64;
                self.aof_buf.advance(n_written);
                self.aof_last_write_status = true;

                match aof_fsync {
                    AofFsync::Always => {
                        aof_file.sync_data().await?;
                        self.aof_last_fsync = clock_ms;
                    }
                    AofFsync::Everysec => {
                        if self.aof_last_fsync + 1000 < clock_ms {
                            aof_file.sync_data().await?;
                            self.aof_last_fsync = clock_ms;
                        }
                    }
                    _ => {}
                }
            }
            Err(e) => {
                log::error!("flush append only file error: {:?}", e);
                // TODO: handle error
            }
        }

        Ok(())
    }

    async fn update_current_size(&mut self) {
        if let Some(file) = &self.aof_file {
            if let Ok(metadata) = file.metadata().await {
                self.aof_current_size = metadata.len();
            }
        }
    }
}

#[derive(Default, Debug)]
pub struct AofWriter {
    pub buffer: BytesMut,
}

impl AofWriter {
    pub fn new() -> AofWriter {
        AofWriter {
            buffer: BytesMut::new(),
        }
    }

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

impl Server {
    fn rewrite_append_only_file(&self, filename: &str) -> Result<()> {
        let now = shared::now_ms();

        // touch tmpfile
        let tmpfile = format!("temp-rewriteaof-bg-{}.aof", shared::get_pid());
        {
            let mut file = std::fs::File::create(&tmpfile).unwrap();

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

            file.write_all(&aof);
            file.flush();
            file.sync_data();
        }

        // rename
        if let Err(e) = std::fs::rename(&tmpfile, filename) {
            log::error!(
                "Error moving temp append only file on the final destination: {:?}",
                e
            );
            tokio::fs::remove_file(&tmpfile);
            return Err(e);
        }

        log::info!("SYNC append only file rewrite performed");

        Ok(())
    }

    pub async fn rewrite_append_only_file_background(
        &self,
        aof_state: &mut RwLockWriteGuard<'_, AofState>,
    ) -> Result<()> {
        if aof_state.aof_child_pid.is_some() {
            return Ok(());
        }

        // fork!
        match unsafe { libc::fork() } {
            -1 => {
                // error
                return Err(std::io::Error::last_os_error());
            }
            0 => {
                // child: rewrite free to go
                drop(aof_state); // drop the lock

                // close listener
                unsafe {
                    libc::close(self.listener_fd.load(Ordering::Relaxed));
                }

                let aof_bg_filename = format!("temp-rewriteaof-bg-{}.aof", shared::get_pid());
                match self.rewrite_append_only_file(&aof_bg_filename) {
                    Ok(_) => {
                        std::process::exit(0);
                    }
                    Err(e) => {
                        log::error!("Error rewriting append only file: {:?}", e);
                        std::process::exit(1);
                    }
                }
            }
            child => {
                // parent
                log::info!("Background AOF rewrite forked process with pid {}", child);
                aof_state.aof_child_pid = Some(child);
            }
        }

        Ok(())
    }

    pub async fn feed_append_only_file(&self, cmd: Command, db_index: u32) -> Result<()> {
        let mut buf = BytesMut::new();
        let mut aof_state = self.aof_state.write().await;

        if aof_state.aof_selected_db != Some(db_index) {
            // emit "SELECT index"
            shared::extend_array(&mut buf, 2);
            shared::extend_bulk_string(&mut buf, b"SELECT" as &[u8]);
            shared::extend_bulk_string(&mut buf, db_index.to_string().as_bytes());
            aof_state.aof_selected_db = Some(db_index);
        }

        match cmd {
            Command::Ping(_cmd) => {}
            Command::Echo(_cmd) => {}
            Command::Quit(_cmd) => {}
            Command::Get(_cmd) => {}
            Command::Set(cmd) => buf.extend_from_slice(&cmd.rewrite()),
            Command::SetNx(cmd) => buf.extend_from_slice(&cmd.rewrite()),
            Command::Append(cmd) => buf.extend_from_slice(&cmd.rewrite()),
            Command::Strlen(_cmd) => {}
            Command::Incr(cmd) => buf.extend_from_slice(&cmd.rewrite()),
            Command::IncrBy(cmd) => buf.extend_from_slice(&cmd.rewrite()),
            Command::Decr(cmd) => buf.extend_from_slice(&cmd.rewrite()),
            Command::DecrBy(cmd) => buf.extend_from_slice(&cmd.rewrite()),
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
            Command::Type(_cmd) => {}
            Command::Save(_cmd) => {}
            Command::BgSave(_cmd) => {}
            Command::BgRewriteAof(_cmd) => {}
            Command::ConfigGet(_cmd) => {}
            Command::ConfigSet(_cmd) => {}
            Command::ConfigResetStat(_cmd) => {}
            Command::ConfigRewrite(_cmd) => {}
            Command::Unknown(_cmd) => {}
        }

        if self.config.read().await.aof_state == AofOption::On {
            aof_state.aof_buf.extend_from_slice(&buf);
        }

        if aof_state.aof_child_pid.is_some() {
            // aof full rewrite in progress
            aof_state.aof_rewrite_buffer_append(&buf);
        }

        Ok(())
    }

    /// Trigger by config set
    pub async fn start_append_only(&self) -> Result<()> {
        let mut aof_state = self.aof_state.write().await;

        aof_state.aof_last_fsync = self.clock_ms.load(Ordering::Relaxed);

        assert_eq!(aof_state.aof_file.is_none(), true, "AOF already open");

        aof_state.aof_file = Some(
            OpenOptions::new()
                .write(true)
                .append(true)
                .open(&self.config.read().await.aof_filename)
                .await?,
        );

        self.rewrite_append_only_file_background(&mut aof_state)
            .await?;

        self.config.write().await.aof_state = AofOption::WaitRewrite;

        Ok(())
    }

    /// Trigger by config set
    pub async fn stop_append_only(&mut self) {
        let mut aof_state = self.aof_state.write().await;

        aof_state.aof_file = None;
    }

    pub async fn background_rewrite_done_handler(
        &self,
        aof_state: &mut RwLockWriteGuard<'_, AofState>,
    ) -> Result<()> {
        log::info!("Background AOF rewrite terminated with success");

        // append new data generated during the rewrite to result file
        let aof_bg_filename = format!(
            "temp-rewriteaof-bg-{}.aof",
            aof_state.aof_child_pid.unwrap()
        );
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&aof_bg_filename)
            .await?;

        aof_state.aof_rewrite_buffer_write(&mut file).await?;

        log::info!(
            "Parent diff successfully flushed to the rewritten AOF ({} bytes)",
            aof_state.aof_rewrite_buf_blocks.len()
        );

        tokio::fs::rename(&aof_bg_filename, &self.config.read().await.aof_filename).await?;

        if aof_state.aof_file.is_some() {
            file.sync_data().await?;
            aof_state.aof_file = Some(file);
            aof_state.aof_selected_db = None;
            aof_state.update_current_size().await;
            aof_state.aof_rewrite_base_size = aof_state.aof_current_size;
            aof_state.aof_buf.clear();
        }

        log::info!("Background AOF rewrite finished successfully");

        {
            // rewrite is done
            let mut cfg = self.config.write().await;
            if cfg.aof_state == AofOption::WaitRewrite {
                cfg.aof_state = AofOption::On;
            }
        }

        // cleanups
        aof_state.aof_rewrite_buffer_reset();

        aof_state.aof_child_pid = None;

        // schedule a new rewrite if needed
        if self.config.read().await.aof_state == AofOption::WaitRewrite {
            aof_state.aof_rewrite_scheduled = true;
        }

        Ok(())
    }

    pub async fn load_append_only_file(self: &Arc<Self>) -> Result<()> {
        let old_aof_state = {
            let mut config = self.config.write().await;
            let state = config.aof_state;
            config.aof_state = AofOption::Off;
            state
        };

        {
            let mut fake_client = Client {
                config: self.config.clone(),
                server: self.clone(),
                db: self.get(0),
                connection: None,
                address: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 0)),
                inner: RwLock::new(ClientInner {
                    name: String::new(),
                    last_interaction: 0,
                    flags: Default::default(),
                }),
                quit_ch: self.quit_ch.subscribe(),
            };

            let mut reader = tokio::io::BufReader::new(
                OpenOptions::new()
                    .read(true)
                    .open(&self.config.read().await.aof_filename)
                    .await?,
            );
            let mut buffer = BytesMut::new();

            loop {
                let mut cur = Cursor::new(&buffer);
                if let Some(frame) = Frame::parse(&mut cur)? {
                    // if a frame is parsed successfully, advance the buffer
                    buffer.advance(cur.position() as usize);

                    // handle the command
                    let cmd = Command::from(frame)?;
                    fake_client.handle_command(cmd).await?;
                } else {
                    // no enough data, need to read more
                    let n_read = reader.read_buf(&mut buffer).await?;
                    if n_read == 0 {
                        break;
                    }
                }
            }
        }

        self.config.write().await.aof_state = old_aof_state;

        let mut aof_state = self.aof_state.write().await;
        aof_state.update_current_size().await;
        aof_state.aof_rewrite_base_size = aof_state.aof_current_size;

        Ok(())
    }
}
