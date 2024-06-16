use bytes::Bytes;

use crate::{connection::Connection, frame::RESP, Db, RespReader, RespReaderError};

#[derive(Debug, Default)]
pub struct Set {
    /// cache lookup key
    key: String,

    // value to store in db
    value: Bytes,
    // expiration time of key
    // ex: Option<Duration>
}

impl Set {
    /// contruct new Set command
    pub fn new(key: String, value: Bytes) -> Self {
        Set { key, value }
    }

    /// Construct new Set command by consuming the RespReader
    ///
    /// # default
    ///
    /// Return `Set::default` if RespReader has no stream left
    /// otherwise return the error
    pub fn from_parts(reader: &mut RespReader) -> Result<Self, RespReaderError> {
        let key = reader.next_string()?;

        let value = reader.next_byte()?;

        Ok(Set { key, value })
    }

    /// Apply the echo command and write to the Tcp connection stream
    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // set the value in the shared cache.
        db.set(self.key, self.value, None);

        // write the OK response to the client connection buffer
        dst.write_frame(&RESP::Simple("OK".into())).await?;

        Ok(())
    }
}

/// Convert Set command back into an equivalent `RESP`
impl From<Set> for RESP {
    fn from(value: Set) -> Self {
        let mut resp = RESP::array();
        resp.push_bulk(Bytes::from("set"));
        resp.push_bulk(Bytes::from(value.key.into_bytes()));
        resp.push_bulk(value.value);

        // write expiration time to RESP

        resp
    }
}
