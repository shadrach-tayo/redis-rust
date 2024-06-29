use std::time::Duration;

use bytes::Bytes;
use tokio::time;

use crate::{connection::Connection, frame::RESP, Db, RespReader, RespReaderError};

#[derive(Debug, Default)]
pub struct PSync {
    key: String,
    value: String,
}

pub const EMPTY_DB_FILE: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

pub(crate) fn empty_rdb_file() -> Vec<u8> {
    let rdb_bytes: Vec<u8> = (0..EMPTY_DB_FILE.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&EMPTY_DB_FILE[i..i + 2], 16).unwrap())
        .collect();
    // let mut header: Vec<u8> = format!("${}\r\n", rdb_bytes.len()).as_bytes().to_owned();
    // header.extend(rdb_bytes);
    // header
    rdb_bytes
}

impl PSync {
    /// Returns command name
    pub fn get_name(&self) -> &str {
        "PYSNC"
    }

    /// Construct new REPLCONF command by consuming the RespReader
    ///
    /// Parse next_string()? to get the config key
    /// Parse next_string()? to get the config value
    ///
    pub fn from_parts(reader: &mut RespReader) -> Result<Self, RespReaderError> {
        let key = reader.next_string()?;
        let value = reader.next_string()?;

        Ok(PSync { key, value })
    }

    /// Apply the echo command and write to the Tcp connection stream
    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        #[allow(unused_assignments)]
        let (replid, _) = db.get_repl_info();
        let replid = replid.unwrap();
        let resp = RESP::Simple(format!("+FULLRESYNC {} 0", replid));

        dbg!(&resp);

        dst.write_frame(&resp).await?;
        time::sleep(Duration::from_millis(50)).await;
        dst.write_raw_bytes(empty_rdb_file()).await?;

        Ok(())
    }
}

impl From<PSync> for RESP {
    fn from(value: PSync) -> Self {
        let mut resp = RESP::array();
        resp.push_bulk(Bytes::from("PSYNC"));
        resp.push_bulk(Bytes::from(value.key));
        resp.push_bulk(Bytes::from(value.value));
        resp
    }
}
