use bytes::Bytes;

use crate::{resp::RESP, RespReader, RespReaderError};

#[derive(Debug, Default)]
pub struct Exec;

impl Exec {
    /// contruct new Exec command
    pub fn new() -> Self {
        Exec {}
    }

    /// Construct new Exec command by consuming the RespReader
    ///
    /// # default
    ///
    /// Return `Exec::default` if RespReader has no stream left
    /// otherwise return the error
    pub fn from_parts(_reader: &mut RespReader) -> Result<Self, RespReaderError> {
        Ok(Exec {})
    }

    /// Apply the echo command and write to the Tcp connection stream
    pub async fn apply(self) -> crate::Result<Option<RESP>> {
        Ok(None)
    }
}

/// Convert Exec command back into an equivalent `RESP`
impl From<Exec> for RESP {
    fn from(_value: Exec) -> Self {
        let mut resp = RESP::array();
        resp.push_bulk(Bytes::from("multi"));
        resp
    }
}
