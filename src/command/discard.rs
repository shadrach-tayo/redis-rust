use bytes::Bytes;

use crate::{resp::RESP, RespReader, RespReaderError};

#[derive(Debug, Default)]
pub struct Discard;

impl Discard {
    /// contruct new Discard command
    pub fn new() -> Self {
        Discard {}
    }

    /// Construct new Discard command by consuming the RespReader
    ///
    /// # default
    ///
    /// Return `Discard::default` if RespReader has no stream left
    /// otherwise return the error
    pub fn from_parts(_reader: &mut RespReader) -> Result<Self, RespReaderError> {
        Ok(Discard {})
    }

    /// Apply the echo command and write to the Tcp connection stream
    pub async fn apply(self) -> crate::Result<Option<RESP>> {
        Ok(None)
    }
}

/// Convert Discard command back into an equivalent `RESP`
impl From<Discard> for RESP {
    fn from(_value: Discard) -> Self {
        let mut resp = RESP::array();
        resp.push_bulk(Bytes::from("discard"));
        resp
    }
}
