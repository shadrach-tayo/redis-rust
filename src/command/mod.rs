pub mod echo;
pub mod ping;

use std::vec;

use bytes::Bytes;
use echo::Echo;
pub use ping::Ping;

use crate::{connection::Connection, frame::RESP};

/// Enum of supported Protocol Commands
#[derive(Debug)]
pub enum Command {
    Ping(Ping),
    Echo(Echo),
}

impl Command {
    /// Create a Command from RESP
    ///
    /// Initialize a RespReader and use it to consume
    /// the RESP data for Command construction
    pub fn from_resp(resp: RESP) -> crate::Result<Command> {
        let mut resp_reader = RespReader::new(resp)?;

        let command_name = resp_reader.next_string()?.to_lowercase();

        let command = match command_name.as_str() {
            "echo" => Command::Echo(Echo::from_parts(&mut resp_reader)?),
            "ping" => Command::Ping(Ping::from_parts(&mut resp_reader)?),
            _ => unimplemented!(), // Err("Unsupported command".into()),
        };

        dbg!(&command);
        // Check if reader has been consumed, if not return an Error
        // to alert protocol of unexpected frame format
        resp_reader.finish()?;

        Ok(command)
    }

    /// Apply the command
    ///
    /// The response is written to the dst connection.
    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        use Command::*;

        match self {
            Echo(command) => command.apply(dst).await,
            Ping(command) => command.apply(dst).await,
            _ => unimplemented!(),
        }
    }

    pub fn get_name(&self) -> &str {
        match self {
            Command::Echo(_) => "echo",
            Command::Ping(_) => "ping",
        }
    }
}

// Implements an RESPReader that iterates over RESP data
// parsed from the protocol
pub struct RespReader {
    inner: vec::IntoIter<RESP>,
}

#[derive(Debug)]
pub enum RespReaderError {
    EndOfStream,
    Other(String),
}

impl RespReader {
    pub fn new(resp: RESP) -> Result<RespReader, RespReaderError> {
        let resp_array = match resp {
            RESP::Array(array) => array,
            resp => return Err(format!("Expected `RESP::Array` but got {:?}", resp).into()),
        };
        Ok(Self {
            inner: resp_array.into_iter(),
        })
    }

    pub fn next(&mut self) -> Result<RESP, RespReaderError> {
        self.inner.next().ok_or(RespReaderError::EndOfStream)
    }

    /// Return the next entry as a string
    ///
    /// Only `Bulk`, and `Simple` are allowed to be
    /// converted to u64 before returned
    pub fn next_string(&mut self) -> Result<String, RespReaderError> {
        match self.next()? {
            RESP::Simple(string) => Ok(string),
            RESP::Bulk(data) => {
                String::from_utf8(data.to_vec()).map_err(|_| "Invalid string".into())
            }
            other => {
                return Err(
                    format!("Expected `RESP::Simple` or `RESP::Bulk but got {:?}", other).into(),
                )
            }
        }
    }

    /// Return the next entry as a byte
    ///
    /// Only `Bulk`, and `Simple` are allowed to be
    /// converted to u64 before returned
    pub fn next_byte(&mut self) -> Result<Bytes, RespReaderError> {
        match self.next()? {
            RESP::Simple(string) => Ok(Bytes::from(string)),
            RESP::Bulk(data) => Ok(data),
            other => {
                return Err(
                    format!("Expected `RESP::Simple` or `RESP::Bulk but got {:?}", other).into(),
                )
            }
        }
    }

    /// Return the next entry as an integer
    ///
    /// Only `Integer`, `Bulk`, and `Simple` are allowed to be
    /// converted to u64 before returned
    pub fn next_int(&mut self) -> Result<u64, RespReaderError> {
        match self.next()? {
            RESP::Integer(int) => Ok(int),
            RESP::Simple(s) => convert_string_to_u64(s).map_err(|_| "Invalid integer".into()),
            RESP::Bulk(data) => convert_bytes_to_u64(data).map_err(|_| "Invalid integer".into()),
            other => {
                return Err(
                    format!("Expected `RESP::Simple` or `RESP::Bulk but got {:?}", other).into(),
                )
            }
        }
    }

    /// Check if RESP has been exhausted from the reader
    pub fn finish(&mut self) -> Result<(), RespReaderError> {
        // if self.inner.next().is_none() {
        //     Ok(())
        // } else {
        //     Err("Expected end of RESP!!!".into())
        // }
        match self.inner.next() {
            Some(_) => Err("Expected end of RESP!!!".into()),
            None => Ok(()),
        }
    }
}

pub fn convert_bytes_to_u64(bytes: bytes::Bytes) -> Result<u64, String> {
    // bytes.len();
    // convert_string_to_u64(String::from_utf8(bytes.to_vec())?)
    let mut buf = [0u8; 8];
    let len = 8.min(bytes.len());
    buf[..len].copy_from_slice(&bytes[..len]);
    Ok(u64::from_be_bytes(buf))
}

pub fn convert_string_to_u64(string: String) -> Result<u64, String> {
    convert_bytes_to_u64(bytes::Bytes::from(string))
}

// Implement standard error
impl std::error::Error for RespReaderError {}

impl From<String> for RespReaderError {
    fn from(value: String) -> Self {
        RespReaderError::Other(value)
    }
}

impl From<&str> for RespReaderError {
    fn from(value: &str) -> Self {
        value.to_string().into()
    }
}

impl std::fmt::Display for RespReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RespReaderError::EndOfStream => "RespReader reached end of stream".fmt(f),
            RespReaderError::Other(reason) => reason.fmt(f),
        }
    }
}

mod test {
    // write tests for the RespReader
    #[test]
    fn create_reader() {
        todo!()
    }
}
