#![allow(non_upper_case_globals)]
use crate::frame::Frame;
use bytes::Bytes;

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
pub const syntax_err: Frame = Frame::Error(Bytes::from_static(b"ERR syntax error"));
