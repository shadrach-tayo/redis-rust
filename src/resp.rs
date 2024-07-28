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
#[derive(Debug, Clone)]
pub enum RESP {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    File(Bytes),
    Null,
    Array(Vec<RESP>),
}

#[derive(Debug)]
pub enum RESPError {
    Incomplete,
    Other(crate::Error),
}

impl RESP {
    /// Returns an empty array
    pub fn array() -> RESP {
        RESP::Array(vec![])
    }

    /// Push a `bulk` resp into an array.
    ///
    /// # Panics
    ///
    /// Panics if `self` is not an array
    pub fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            RESP::Array(vec) => vec.push(RESP::Bulk(bytes)),
            _ => panic!("Not `RESP::Array`"),
        }
    }

    /// Push an `integer` resp into an array.
    ///
    /// # Panics
    ///
    /// Panics if `self` is not an array
    pub fn push_int(&mut self, value: u64) {
        match self {
            RESP::Array(vec) => vec.push(RESP::Integer(value)),
            _ => panic!("Not `RESP::Array`"),
        }
    }

    /// Parse the message from the client
    pub fn parse_resp(cursor: &mut Cursor<&[u8]>) -> Result<RESP, RESPError> {
        match get_u8(cursor)? {
            b'+' => {
                let line = get_line(cursor)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(RESP::Simple(string))
            }
            b'*' => {
                let len = get_decimal(cursor)?.try_into()?;
                let mut out = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    out.push(Self::parse_resp(cursor)?);
                }
                Ok(RESP::Array(out))
            }
            b'$' => {
                // bulk strings data type
                if b'-' == peak_u8(cursor)? {
                    let line = get_line(cursor)?;
                    if line != b"-1" {
                        return Err("Invalid input format.".into());
                    }
                    Ok(RESP::Null)
                } else {
                    let len = get_decimal(cursor)?.try_into()?;

                    if cursor.remaining() < len {
                        return Err(RESPError::Incomplete);
                    }

                    // println!(
                    //     "Bulk, len: {len}, data: {:?}",
                    //     String::from_utf8_lossy(&cursor.chunk()[..len])
                    // );
                    let data = Bytes::copy_from_slice(&cursor.chunk()[..len]);
                    skip(cursor, len)?;

                    let pos = cursor.position() as usize;

                    let clrf = if cursor.has_remaining() {
                        println!(
                            "Check CLRF {:?}",
                            String::from_utf8_lossy(&cursor.get_ref()[pos..])
                        );
                        &cursor.get_ref()[pos..pos + 2] == b"\r\n"
                    } else {
                        println!("File {:?}", String::from_utf8_lossy(&cursor.chunk()[..]));
                        false
                    };

                    if clrf {
                        // skip that number of bytes + 2
                        skip(cursor, 2)?;
                        Ok(RESP::Bulk(data))
                    } else {
                        Ok(RESP::File(data))
                    }
                }
            }
            b':' => {
                // integer data type (u64)
                let int = get_decimal(cursor)?;
                Ok(RESP::Integer(int))
            }
            b'-' => {
                // simple errors data type
                let error = get_line(cursor)?.to_vec();
                let string = String::from_utf8(error)?;
                Ok(RESP::Error(string))
            }
            b'_' => {
                // null data type
                Ok(RESP::Null)
            }
            raw => Err(format!("Invalid RESP data type: `{}`", raw).into()),
        }
    }

    #[allow(unused)]
    /// Validate if a message can be decoded from the `src`
    pub fn check_frame(src: &mut Cursor<&[u8]>) -> Result<(), RESPError> {
        match get_u8(src)? {
            b'+' => {
                // strings resp
                println!("get u8");
                Ok(())
            }
            b'*' => {
                // arrays resp
                todo!()
            }
            b'$' => {
                // bulk strings resp
                todo!()
            }
            b':' => {
                // integers resp
                todo!()
            }
            b'-' => {
                // simple errors resp
                todo!()
            }
            b'_' => {
                // null resp
                todo!()
            }
            err => Err(format!("Error reading request {}", err).into()),
        }
    }
}

pub fn get_line<'a>(src: &'a mut Cursor<&[u8]>) -> Result<&'a [u8], RESPError> {
    // println!("parse line");
    let start = src.position() as usize;
    let end = src.get_ref().len() - 1;

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            src.set_position((i + 2) as u64);
            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(RESPError::Incomplete)
}

pub fn peak_u8(src: &mut Cursor<&[u8]>) -> Result<u8, RESPError> {
    if !src.has_remaining() {
        return Err(RESPError::Incomplete);
    }
    let peak = src.chunk()[0];
    Ok(peak)
}

pub fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, RESPError> {
    if !src.has_remaining() {
        return Err(RESPError::Incomplete);
    }

    Ok(src.get_u8())
}

pub fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, RESPError> {
    let line = get_line(src)?.to_vec();
    let string = String::from_utf8(line)?;
    let int: u64 = string.parse().unwrap();
    Ok(int)
}

pub fn write_decimal(dst: &mut BufWriter<&mut TcpStream>, val: u64) -> io::Result<()> {
    // use std::io::Write;
    let mut buf = [0u8, 20];
    let mut buf = Cursor::new(&mut buf[..]);

    write!(&mut buf, "{}", val).unwrap();

    let pos = buf.position() as usize;
    dst.write_all(&buf.get_ref()[..pos])?;
    dst.write_all(b"\r\n")?;

    Ok(())
}

pub fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), RESPError> {
    src.advance(n);
    Ok(())
}

impl From<String> for RESPError {
    fn from(value: String) -> Self {
        RESPError::Other(value.into())
    }
}

impl From<&str> for RESPError {
    fn from(value: &str) -> Self {
        value.to_string().into()
    }
}

impl From<FromUtf8Error> for RESPError {
    fn from(_value: FromUtf8Error) -> Self {
        "Invalid resp format".into()
    }
}

impl From<TryFromIntError> for RESPError {
    fn from(_value: TryFromIntError) -> Self {
        "Invalid resp format".into()
    }
}

impl fmt::Display for RESPError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RESPError::Incomplete => "stream ended early".fmt(fmt),
            RESPError::Other(err) => err.fmt(fmt),
        }
    }
}

impl std::error::Error for RESPError {}
