use bytes::{Buf, Bytes, BytesMut};
use std::fmt::Display;
use std::io::{Cursor, Error, ErrorKind, Result};
use std::str::FromStr;

#[derive(Clone, Debug)]
pub enum Frame {
    Simple(Bytes),
    Error(Bytes),
    Integer(u64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

impl Frame {
    pub fn new_array() -> Self {
        Frame::Array(Vec::new())
    }

    pub fn new_bulk_from(s: impl Into<Bytes>) -> Frame {
        Frame::Bulk(s.into())
    }

    pub fn new_bulk_from_slice<'a>(s: impl Into<&'a [u8]>) -> Frame {
        Frame::Bulk(Bytes::copy_from_slice(s.into()))
    }

    // pub fn new_bulk_from_bytes(s: Bytes) -> Frame {
    //     Frame::Bulk(s)
    // }

    pub fn new_integer_from(i: u64) -> Frame {
        Frame::Integer(i)
    }

    pub fn sealed(self) -> Result<Frame> {
        match self {
            Frame::Bulk(b) => {
                let mut array = Vec::new();
                array.push(Frame::Bulk(b));
                Ok(Frame::Array(array))
            }
            _ => Err(Error::new(ErrorKind::InvalidInput, "expect bulk frame")),
        }
    }

    fn next_line<'a>(src: &'a mut Cursor<&BytesMut>) -> Option<&'a [u8]> {
        let begin = src.position() as usize;

        for i in begin..src.get_ref().len() {
            if src.get_ref()[i] == b'\n' {
                src.set_position((i + 1) as u64);
                if i > 0 && src.get_ref()[i - 1] == b'\r' {
                    return Some(&src.get_ref()[begin..i - 1]);
                }
                return Some(&src.get_ref()[begin..i]);
            }
        }

        assert_eq!(src.position() as usize, begin);
        None
    }

    fn next_utf8_str<'a>(src: &'a mut Cursor<&BytesMut>) -> Result<Option<&'a str>> {
        if let Some(line) = Self::next_line(src) {
            if let Ok(s) = std::str::from_utf8(&line) {
                Ok(Some(s))
            } else {
                Err(Error::new(ErrorKind::Other, "parse error"))
            }
        } else {
            Ok(None)
        }
    }

    fn parse_into<T: FromStr>(src: &mut Cursor<&BytesMut>) -> Result<Option<T>> {
        if let Some(s) = Self::next_utf8_str(src)? {
            if let Ok(n) = T::from_str(s) {
                Ok(Some(n))
            } else {
                Err(Error::new(ErrorKind::Other, "parse error"))
            }
        } else {
            Ok(None)
        }
    }

    pub fn parse(src: &mut Cursor<&BytesMut>) -> Result<Option<Frame>> {
        if !src.has_remaining() {
            return Ok(None);
        }

        let checkpoint: u64 = src.position();
        let byte = src.get_u8();
        match byte {
            b'+' => {
                if let Some(line) = Self::next_line(src) {
                    let frame = Frame::Simple(Bytes::copy_from_slice(line));
                    return Ok(Some(frame));
                }
            }
            b'-' => {
                if let Some(line) = Self::next_line(src) {
                    let frame = Frame::Error(Bytes::copy_from_slice(line));
                    return Ok(Some(frame));
                }
            }
            b':' => {
                if let Some(n) = Self::parse_into(src)? {
                    let frame = Frame::Integer(n);
                    return Ok(Some(frame));
                }
            }
            b'$' => {
                if let Some(len) = Self::parse_into::<i64>(src)? {
                    if len == -1 {
                        return Ok(Some(Frame::Null));
                    }

                    if src.remaining() as i64 > len {
                        let bulk = Bytes::copy_from_slice(&src.chunk()[..len as usize]);
                        src.advance(len as usize);

                        return Ok(Some(Frame::Bulk(bulk)));
                    }
                }
            }
            b'*' => {
                if let Some(len) = Self::parse_into(src)? {
                    let mut array = Vec::with_capacity(len);
                    for _ in 0..len {
                        if let Some(frame) = Frame::parse(src)? {
                            array.push(frame);
                        } else {
                            // failed to parse array element
                            break;
                        }
                    }

                    // check if array is fully read, if not rollback
                    if array.len() == len as usize {
                        return Ok(Some(Frame::Array(array)));
                    }
                }
            }
            _ => {
                // maybe inline command "cmd arg1 arg2 ...\r\n"
                // unget last byte
                src.set_position(checkpoint);

                // parse each into array
                if let Some(line) = Self::next_line(src) {
                    let frames = line
                        .split(|&b| b == b' ')
                        .map(|s| Frame::Bulk(Bytes::copy_from_slice(s)))
                        .collect();
                    return Ok(Some(Frame::Array(frames)));
                }
            }
        } // match

        // data not enough, rollback
        src.set_position(checkpoint);
        Ok(None)
    }

    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&self.to_string().into_bytes());
        buf.freeze()
    }
}

impl Display for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Frame::Simple(s) => write!(f, "+{}\r\n", String::from_utf8_lossy(s)),
            Frame::Error(s) => write!(f, "-{}\r\n", String::from_utf8_lossy(s)),
            Frame::Integer(n) => write!(f, ":{}\r\n", n),
            Frame::Bulk(b) => write!(f, "${}\r\n{}\r\n", b.len(), String::from_utf8_lossy(b)),
            Frame::Null => write!(f, "$-1\r\n"),
            Frame::Array(a) => {
                write!(f, "*{}\r\n", a.len())?;
                for frame in a {
                    write!(f, "{}", frame)?;
                }
                Ok(())
            }
        }
    }
}
