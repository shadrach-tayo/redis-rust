pub mod config;
pub mod discard;
pub mod echo;
pub mod exec;
pub mod get;
pub mod incr;
pub mod info;
pub mod keys;
pub mod multi;
pub mod ping;
pub mod psync;
pub mod replconf;
pub mod set;
pub mod stream;
pub mod types;
pub mod unknown;
pub mod wait;

use std::{
    sync::{atomic::AtomicUsize, Arc},
    vec,
};

use bytes::Bytes;
use config::Config;
use discard::Discard;
use echo::Echo;
use exec::Exec;
use get::Get;
use incr::Incr;
use info::Info;
use keys::Keys;
use multi::Multi;
use ping::Ping;
pub use psync::PSync;
pub use replconf::Replconf;
use set::Set;
use stream::{XAdd, XRange, XRead};
use tokio::sync::RwLock;
use unknown::Unknown;
use wait::Wait;

use crate::{config::ServerConfig, connection::Connection, resp::RESP, Db};

/// Enum of supported Protocol Commands
#[derive(Debug)]
pub enum Command {
    Config(Config),
    Echo(Echo),
    Get(Get),
    Info(Info),
    Ping(Ping),
    Replconf(Replconf),
    PSync(PSync),
    Set(Set),
    Unknown(Unknown),
    Wait(Wait),
    Keys(Keys),
    Type(types::Type),
    XAdd(XAdd),
    XRange(XRange),
    XRead(XRead),
    Incr(Incr),
    Multi(Multi),
    Exec(Exec),
    Discard(Discard),
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
            "config" => Command::Config(Config::from_parts(&mut resp_reader)?),
            "ping" => Command::Ping(Ping::from_parts(&mut resp_reader)?),
            "set" => Command::Set(Set::from_parts(&mut resp_reader)?),
            "incr" => Command::Incr(Incr::from_parts(&mut resp_reader)?),
            "get" => Command::Get(Get::from_parts(&mut resp_reader)?),
            "info" => Command::Info(Info::from_parts(&mut resp_reader)?),
            "replconf" => Command::Replconf(Replconf::from_parts(&mut resp_reader)?),
            "psync" => Command::PSync(PSync::from_parts(&mut resp_reader)?),
            "wait" => Command::Wait(Wait::from_parts(&mut resp_reader)?),
            "keys" => Command::Keys(Keys::from_parts(&mut resp_reader)?),
            "type" => Command::Type(types::Type::from_parts(&mut resp_reader)?),
            "xadd" => Command::XAdd(XAdd::from_parts(&mut resp_reader)?),
            "xrange" => Command::XRange(XRange::from_parts(&mut resp_reader)?),
            "xread" => Command::XRead(XRead::from_parts(&mut resp_reader)?),
            "multi" => Command::Multi(Multi::from_parts(&mut resp_reader)?),
            "exec" => Command::Exec(Exec::from_parts(&mut resp_reader)?),
            "discard" => Command::Discard(Discard::from_parts(&mut resp_reader)?),
            _ => panic!("Unexpected command"),
        };

        // Check if reader has been consumed, if not return an Error
        // to alert protocol of unexpected resp format
        resp_reader.finish()?;

        Ok(command)
    }

    /// Apply the command
    ///
    /// The response is written to the dst connection.
    pub async fn apply(
        self,
        dst: &mut Connection,
        db: &Db,
        offset: Option<&AtomicUsize>,
        replicas: Arc<RwLock<Vec<Connection>>>,
        config: ServerConfig,
    ) -> crate::Result<Option<RESP>> {
        use Command::*;

        match self {
            Config(cmd) => cmd.apply(config).await,
            Echo(cmd) => cmd.apply(dst).await,
            Ping(cmd) => cmd.apply(dst).await,
            Unknown(cmd) => cmd.apply(dst).await,
            Set(cmd) => cmd.apply(&db, dst).await,
            Incr(cmd) => cmd.apply(&db, dst).await,
            Get(cmd) => cmd.apply(&db, dst).await,
            Keys(cmd) => cmd.apply(&db, dst).await,
            Type(cmd) => cmd.apply(&db, dst).await,
            Info(cmd) => cmd.apply(&db, config).await,
            Replconf(cmd) => cmd.apply(dst, offset).await,
            PSync(cmd) => cmd.apply(&db, dst).await,
            Wait(cmd) => cmd.apply(dst, offset, replicas, config).await,
            XAdd(cmd) => cmd.apply(&db).await,
            XRange(cmd) => cmd.apply(&db).await,
            XRead(cmd) => cmd.apply(&db).await,
            Multi(cmd) => cmd.apply().await,
            Exec(cmd) => cmd.apply().await,
            Discard(cmd) => cmd.apply().await,
        }
    }

    pub fn get_name(&self) -> String {
        match self {
            Command::Config(_) => "config".to_string(),
            Command::Echo(_) => "echo".to_string(),
            Command::Ping(_) => "ping".to_string(),
            Command::Set(_) => "set".to_string(),
            Command::Get(_) => "get".to_string(),
            Command::Info(_) => "info".to_string(),
            Command::Replconf(_) => "replconf".to_string(),
            Command::PSync(_) => "psync".to_string(),
            Command::Wait(_) => "wait".to_string(),
            Command::Keys(_) => "keys".to_string(),
            Command::Type(_) => "type".to_string(),
            Command::XAdd(_) => "xadd".to_string(),
            Command::XRange(_) => "xrange".to_string(),
            Command::XRead(_) => "xread".to_string(),
            Command::Incr(_) => "incr".to_string(),
            Command::Multi(_) => "multi".to_string(),
            Command::Exec(_) => "exec".to_string(),
            Command::Discard(_) => "discard".to_string(),
            Command::Unknown(_) => "unknown".into(),
        }
    }

    pub fn is_replicable_command(&self) -> bool {
        match self {
            Command::Set(_) => true,
            _ => false,
        }
    }

    pub fn affects_offset(&self) -> bool {
        match self {
            Command::Set(_) => true,
            // Command::Replconf(_) => true,
            // Command::Ping(_) => true,
            _ => false,
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
        match self.inner.next() {
            Some(_) => Err("Expected end of RESP!!!".into()),
            None => Ok(()),
        }
    }
}

pub fn convert_bytes_to_u64(bytes: bytes::Bytes) -> Result<u64, String> {
    let int = String::from_utf8(bytes.to_vec())
        .map_err(|_| "Cannot parse u64 from bytes".to_string())?
        .parse::<u64>()
        .map_err(|_| "Cannot parse u64 from bytes".to_string())?;

    // println!("U64 value {:?}", int);
    Ok(int)
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
