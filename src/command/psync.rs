use crate::{connection::Connection, resp::RESP, Db, RespReader, RespReaderError};
use bytes::Bytes;

#[allow(unused_imports)]
use std::time::Duration;
#[allow(unused_imports)]
use tokio::time;

#[derive(Debug, Default)]
pub struct PSync {
    key: String,
    value: String,
}

pub const EMPTY_DB_FILE: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

static EMPTY_RDB_FILE_: [u8; 88] = [
    0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73,
    0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69,
    0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2,
    0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0,
    0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff,
    0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2,
];

#[allow(unused)]
pub(crate) fn empty_rdb_file() -> Vec<u8> {
    let rdb_bytes: Vec<u8> = (0..EMPTY_DB_FILE.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&EMPTY_DB_FILE[i..i + 2], 16).unwrap())
        .collect();

    rdb_bytes
}

impl PSync {
    pub fn new(key: String, value: String) -> Self {
        PSync { key, value }
    }

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
    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<Option<RESP>> {
        let (replid, _) = db.get_repl_info();
        let replid = replid.unwrap();
        let resp = RESP::Simple(format!("+FULLRESYNC {} 0", replid));
        println!("Write full sync 1");
        dst.write_frame(&resp).await?;

        // write empty rdb file
        dst.write_frame(&RESP::File(EMPTY_RDB_FILE_.as_slice().into()))
            .await?;

        println!("RDB file sent!!!");

        Ok(None)
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
