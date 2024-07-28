use bytes::Bytes;

use crate::{config::ServerConfig, resp::RESP, RespReader, RespReaderError};

#[derive(Debug, Default)]
pub struct Config {
    command: String,
    key: String,
}

impl Config {
    /// contruct new Config command
    pub fn new(command: String) -> Self {
        Config {
            command,
            ..Default::default()
        }
    }

    /// Construct new Config command by consuming the RespReader
    ///
    /// # default
    ///
    /// Return `Config::default` if RespReader has no stream left
    /// otherwise return the error
    pub fn from_parts(reader: &mut RespReader) -> Result<Self, RespReaderError> {
        let command = reader.next_string()?;
        let key = reader.next_string()?;
        Ok(Config { command, key })
    }

    /// Apply the echo command and write to the Tcp connection stream
    pub async fn apply(self, config: ServerConfig) -> crate::Result<Option<RESP>> {
        let mut resp = RESP::Null;

        match (self.command, self.key) {
            (cmd, key) if cmd.to_lowercase() == "get" && key.to_lowercase() == "dir" => {
                resp = RESP::Array(vec![
                    RESP::Bulk(Bytes::from("dir")),
                    RESP::Bulk(Bytes::from(config.dir.unwrap().clone())),
                ]);
            }
            (cmd, key) if cmd.to_lowercase() == "get" && key.to_lowercase() == "dbfilename" => {
                resp = RESP::Array(vec![
                    RESP::Bulk(Bytes::from("dbfilename")),
                    RESP::Bulk(Bytes::from(config.dbfilename.unwrap().clone())),
                ]);
            }
            (cmd, key) => {
                println!("Unsupported Config request: CONFIG {cmd} {key}");
            }
        }

        Ok(Some(resp))
    }
}

/// Convert Config command back into an equivalent `RESP`
impl From<Config> for RESP {
    fn from(value: Config) -> Self {
        let mut resp = RESP::array();
        resp.push_bulk(Bytes::from("CONFIG"));
        resp.push_bulk(Bytes::from(value.command.into_bytes()));
        resp.push_bulk(Bytes::from(value.key.into_bytes()));

        resp
    }
}
