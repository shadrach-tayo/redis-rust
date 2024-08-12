use bytes::Bytes;

use crate::{connection::Connection, resp::RESP, Db, RespReader, RespReaderError};

#[derive(Debug, Default)]
pub struct Keys {
    /// cache lookup key
    key: String,
}

impl Keys {
    /// contruct new Keys command
    pub fn new(key: String) -> Self {
        Keys { key }
    }

    /// Construct new Keys command by consuming the RespReader
    ///
    /// # default
    ///
    /// Return `Keys::default` if RespReader has no stream left
    /// otherwise return the error
    pub fn from_parts(reader: &mut RespReader) -> Result<Self, RespReaderError> {
        let key = reader.next_string()?;

        Ok(Keys { key })
    }

    /// Apply the echo command and write to the Tcp connection stream
    pub async fn apply(self, db: &Db, _dst: &mut Connection) -> crate::Result<Option<RESP>> {
        let response = match self.key.as_str() {
            "*" => db.keys(),
            _ => {
                todo!()
            }
        };

        let mut resp = RESP::array();
        response
            .into_iter()
            .for_each(|key| resp.push_bulk(Bytes::from(key)));

        Ok(Some(resp))
    }
}

/// Convert Keys command back into an equivalent `RESP`
impl From<Keys> for RESP {
    fn from(value: Keys) -> Self {
        let mut resp = RESP::array();
        resp.push_bulk(Bytes::from("KEYS"));
        resp.push_bulk(Bytes::from(value.key.into_bytes()));

        resp
    }
}
