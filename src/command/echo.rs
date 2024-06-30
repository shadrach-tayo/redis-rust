use bytes::Bytes;

use crate::{connection::Connection, frame::RESP, RespReader, RespReaderError};

#[derive(Debug, Default)]
pub struct Echo {
    msg: Option<Bytes>,
}

impl Echo {
    /// contruct new Echo command
    pub fn new(msg: Option<Bytes>) -> Self {
        Echo { msg }
    }

    /// Construct new Echo command by consuming the RespReader
    ///
    /// # default
    ///
    /// Return `Echo::default` if RespReader has no stream left
    /// otherwise return the error
    pub fn from_parts(reader: &mut RespReader) -> Result<Self, RespReaderError> {
        match reader.next_byte() {
            Ok(msg) => Ok(Echo { msg: Some(msg) }),
            Err(RespReaderError::EndOfStream) => Ok(Echo::default()),
            Err(err) => Err(err.into()),
        }
    }

    /// Apply the echo command and write to the Tcp connection stream
    pub async fn apply(self, _dst: &mut Connection) -> crate::Result<Option<RESP>> {
        let resp = match self.msg {
            Some(msg) => RESP::Bulk(msg),
            None => RESP::Simple("".to_string()),
        };

        Ok(Some(resp))
    }
}

/// Convert Echo command back into an equivalent `RESP`
impl From<Echo> for RESP {
    fn from(value: Echo) -> Self {
        let mut resp = RESP::array();
        resp.push_bulk(Bytes::from("echo"));
        if let Some(msg) = value.msg {
            resp.push_bulk(Bytes::from(msg));
        }
        resp
    }
}
