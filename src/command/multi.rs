use bytes::Bytes;

use crate::{connection::Connection, resp::RESP, RespReader, RespReaderError};

#[derive(Debug, Default)]
pub struct Multi;

impl Multi {
    /// contruct new Multi command
    pub fn new() -> Self {
        Multi {}
    }

    /// Construct new Multi command by consuming the RespReader
    ///
    /// # default
    ///
    /// Return `Multi::default` if RespReader has no stream left
    /// otherwise return the error
    pub fn from_parts(_reader: &mut RespReader) -> Result<Self, RespReaderError> {
        Ok(Multi {})
    }

    /// Apply the echo command and write to the Tcp connection stream
    pub async fn apply(self) -> crate::Result<Option<RESP>> {
        Ok(Some(RESP::Simple("OK".to_string())))
    }
}

/// Convert Multi command back into an equivalent `RESP`
impl From<Multi> for RESP {
    fn from(_value: Multi) -> Self {
        let mut resp = RESP::array();
        resp.push_bulk(Bytes::from("multi"));
        resp
    }
}
