use bytes::Bytes;

use crate::{connection::Connection, resp::RESP, Db, RespReader, RespReaderError, ValueType};

#[derive(Debug, Default)]
pub struct Type {
    /// cache lookup key
    key: String,
}

impl Type {
    /// contruct new Type command
    pub fn new(key: String) -> Self {
        Type { key }
    }

    /// Construct new Type command by consuming the RespReader
    ///
    /// # default
    ///
    /// Return `Type::default` if RespReader has no stream left
    /// otherwise return the error
    pub fn from_parts(reader: &mut RespReader) -> Result<Self, RespReaderError> {
        let key = reader.next_string()?;

        Ok(Type { key })
    }

    /// Apply the echo command and write to the Tcp connection stream
    pub async fn apply(self, db: &Db, _dst: &mut Connection) -> crate::Result<Option<RESP>> {
        // set the value in the shared cache.
        let value = db.get(&self.key);

        if let Some(value_type) = value {
            println!("Value type: {:?}", value_type);
            match value_type {
                ValueType::String(_) => Ok(Some(RESP::Simple("string".to_string()))),
                ValueType::Stream(_) => Ok(Some(RESP::Simple("stream".to_string()))),
            }
        } else {
            Ok(Some(RESP::Simple("none".to_string())))
        }
    }
}

/// Convert Type command back into an equivalent `RESP`
impl From<Type> for RESP {
    fn from(value: Type) -> Self {
        let mut resp = RESP::array();
        resp.push_bulk(Bytes::from("type"));
        resp.push_bulk(Bytes::from(value.key.into_bytes()));

        resp
    }
}
