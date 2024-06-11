use std::{
    fmt,
    io::{self, BufWriter, Cursor, Write},
    net::TcpStream,
    num::TryFromIntError,
    string::FromUtf8Error,
};

use bytes::{Buf, Bytes};

pub const TERMINATOR: &str = "\r\n";

#[allow(unused)]
#[derive(Debug)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub enum FrameError {
    Incomplete,
    Other(crate::Error),
}

impl Frame {
    /// Returns an empty array
    pub fn array() -> Frame {
        Frame::Array(vec![])
    }

    /// Parse the message from the client
    pub fn parse_frame(cursor: &mut Cursor<&[u8]>) -> Result<Frame, FrameError> {
        match get_u8(cursor)? {
            b'+' => {
                // strings data type
                let line = get_line(cursor)?.to_vec();
                let string = String::from_utf8(line)?;
                // println!("parse string {}", &string);
                Ok(Frame::Simple(string))
            }
            b'*' => {
                // list data type
                let len = get_decimal(cursor)?.try_into()?;
                let mut out = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    out.push(Self::parse_frame(cursor)?);
                }
                Ok(Frame::Array(out))
            }
            b'$' => {
                // bulk strings data type
                if b'-' == peak_u8(cursor)? {
                    let line = get_line(cursor)?;
                    if line != b"-1" {
                        return Err("Invalid input format.".into());
                    }
                    Ok(Frame::Null)
                } else {
                    let len = get_decimal(cursor)?.try_into()?;
                    let n = len + 2;
                    if cursor.remaining() < n {
                        return Err(FrameError::Incomplete);
                    }
                    let data = Bytes::copy_from_slice(&cursor.chunk()[..len]);

                    // skip that number of bytes + 2
                    skip(cursor, n)?;

                    Ok(Frame::Bulk(data))
                }
            }
            b':' => {
                // integer data type (u64)
                let int = get_decimal(cursor)?;
                Ok(Frame::Integer(int))
            }
            b'-' => {
                // simple errors data type
                let error = get_line(cursor)?.to_vec();
                let string = String::from_utf8(error)?;
                Ok(Frame::Error(string))
            }
            b'_' => {
                // null data type
                Ok(Frame::Null)
            }
            raw => Err(format!("Invalid frame type byte `{}`", raw).into()),
        }
    }

    #[allow(unused)]
    /// Validate if a message can be decoded from the `src`
    pub fn check_frame(src: &mut Cursor<&[u8]>) -> Result<(), FrameError> {
        match get_u8(src)? {
            b'+' => {
                // strings frame
                println!("get u8");
                Ok(())
            }
            b'*' => {
                // arrays frame
                todo!()
            }
            b'$' => {
                // bulk strings frame
                todo!()
            }
            b':' => {
                // integers frame
                todo!()
            }
            b'-' => {
                // simple errors frame
                todo!()
            }
            b'_' => {
                // null frame
                todo!()
            }
            err => Err(format!("Error reading request {}", err).into()),
        }
    }
}

pub fn frame_to_string(
    frame: &Frame,
    dst: &mut BufWriter<&mut TcpStream>,
) -> Result<(), std::io::Error> {
    match frame {
        Frame::Null => {
            dst.write_all(b"$-1\r\n")?;
        }
        Frame::Error(error) => {
            dst.write_all(b"-")?;
            dst.write_all(error.as_bytes())?;
            dst.write_all(b"\r\n")?;
        }
        Frame::Simple(string) => {
            dst.write_all(b"+")?;
            dst.write_all(string.as_bytes())?;
            dst.write_all(b"\r\n")?;
        }
        Frame::Bulk(data) => {
            dst.write_all(b"$")?;
            let len = data.len() as u64;
            write_decimal(dst, len)?;

            if String::from_utf8(data.to_vec()).unwrap().to_lowercase() == "ping" {
                dst.write_all(b"PONG")?;
            } else {
                dst.write_all(data)?;
            }

            dst.write_all(b"\r\n")?;
        }
        Frame::Array(list) => {
            dst.write_all(b"*")?;
            write_decimal(dst, list.len() as u64)?;

            for frame in list {
                frame_to_string(frame, dst)?;
            }
        }
        Frame::Integer(int) => {
            dst.write_all(b":")?;
            write_decimal(dst, *int)?;
        }
    }

    Ok(())
}

pub fn get_line<'a>(src: &'a mut Cursor<&[u8]>) -> Result<&'a [u8], FrameError> {
    // println!("parse line");
    let start = src.position() as usize;
    let end = src.get_ref().len() - 1;

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            src.set_position((i + 2) as u64);
            // println!("parsed line {:?}", &src.get_ref()[start..i]);
            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(FrameError::Incomplete)
}

pub fn peak_u8(src: &mut Cursor<&[u8]>) -> Result<u8, FrameError> {
    if !src.has_remaining() {
        return Err(FrameError::Incomplete);
    }
    let peak = src.chunk()[0];
    Ok(peak)
}

pub fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, FrameError> {
    if !src.has_remaining() {
        return Err(FrameError::Incomplete);
    }

    Ok(src.get_u8())
}

pub fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, FrameError> {
    let line = get_line(src)?.to_vec();
    let string = String::from_utf8(line)?;
    let int: u64 = string.parse().unwrap();
    Ok(int)
}

pub fn write_decimal(dst: &mut BufWriter<&mut TcpStream>, val: u64) -> io::Result<()> {
    use std::io::Write;
    let mut buf = [0u8, 20];
    let mut buf = Cursor::new(&mut buf[..]);

    write!(&mut buf, "{}", val).unwrap();

    let pos = buf.position() as usize;
    dst.write_all(&buf.get_ref()[..pos])?;
    dst.write_all(b"\r\n")?;

    Ok(())
}

pub fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), FrameError> {
    src.advance(n);
    Ok(())
}

impl From<String> for FrameError {
    fn from(value: String) -> Self {
        FrameError::Other(value.into())
    }
}

impl From<&str> for FrameError {
    fn from(value: &str) -> Self {
        value.to_string().into()
    }
}

impl From<FromUtf8Error> for FrameError {
    fn from(_value: FromUtf8Error) -> Self {
        "Invalid frame format".into()
    }
}

impl From<TryFromIntError> for FrameError {
    fn from(_value: TryFromIntError) -> Self {
        "Invalid frame format".into()
    }
}

impl fmt::Display for FrameError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FrameError::Incomplete => "stream ended early".fmt(fmt),
            FrameError::Other(err) => err.fmt(fmt),
        }
    }
}
