use bytes::Bytes;

use crate::{connection::Connection, frame::RESP, Db, RespReader, RespReaderError};

#[derive(Debug, Default)]
pub struct Get {
    /// cache lookup key
    key: String,
}

impl Get {
    /// contruct new Get command
    pub fn new(key: String) -> Self {
        Get { key }
    }

    /// Construct new Get command by consuming the RespReader
    ///
    /// # default
    ///
    /// Return `Get::default` if RespReader has no stream left
    /// otherwise return the error
    pub fn from_parts(reader: &mut RespReader) -> Result<Self, RespReaderError> {
        let key = reader.next_string()?;

        Ok(Get { key })
    }

    /// Apply the echo command and write to the Tcp connection stream
    pub async fn apply(self, db: &Db, _dst: &mut Connection) -> crate::Result<Option<RESP>> {
        // set the value in the shared cache.
        let value = db.get(&self.key);

        let response = if let Some(bytes) = value {
            RESP::Bulk(bytes)
        } else {
            RESP::Null
        };

        // write the OK response to the client connection buffer
        // dst.write_frame(&response).await?;

        Ok(Some(response))
    }
}

/// Convert Get command back into an equivalent `RESP`
impl From<Get> for RESP {
    fn from(value: Get) -> Self {
        let mut resp = RESP::array();
        resp.push_bulk(Bytes::from("get"));
        resp.push_bulk(Bytes::from(value.key.into_bytes()));

        resp
    }
}
