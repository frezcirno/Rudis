#![allow(non_upper_case_globals)]

use crate::frame::Frame;
use bytes::{Bytes, BytesMut};
use std::time::SystemTime;

pub const crlf: Bytes = Bytes::from_static(b"\r\n");
pub const ok: Frame = Frame::Simple(Bytes::from_static(b"OK"));
pub const err: Frame = Frame::Error(Bytes::from_static(b"ERR"));
pub const pong: Frame = Frame::Simple(Bytes::from_static(b"PONG"));
pub const empty_bulk: Frame = Frame::Bulk(Bytes::new());
pub const null_bulk: Frame = Frame::Null;
pub const wrong_type_err: Frame = Frame::Error(Bytes::from_static(
    b"WRONGTYPE Operation against a key holding the wrong kind of value",
));
pub const no_key_err: Frame = Frame::Error(Bytes::from_static(b"ERR no such key"));
pub const protocol_err: Frame = Frame::Error(Bytes::from_static(b"ERR Protocol error"));
pub const syntax_err: Frame = Frame::Error(Bytes::from_static(b"ERR syntax error"));

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub fn gen_runid() -> String {
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    let mut rng = thread_rng();
    let runid: String = (&mut rng)
        .sample_iter(&Alphanumeric)
        .take(40)
        .map(char::from)
        .collect();

    runid
}

pub fn extend_array(out: &mut BytesMut, len: usize) {
    out.extend_from_slice(b"*");
    out.extend_from_slice(len.to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
}

pub fn extend_bulk_string<'a>(out: &mut BytesMut, s: impl Into<&'a [u8]>) {
    let s = s.into();
    out.extend_from_slice(b"$");
    out.extend_from_slice(s.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(s);
    out.extend_from_slice(b"\r\n");
}

pub fn get_pid() -> u32 {
    unsafe { libc::getpid() as u32 }
}
