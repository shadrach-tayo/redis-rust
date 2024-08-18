use bytes::Bytes;

use crate::{connection::Connection, resp::RESP, Db, RespReader, RespReaderError, ValueType};

#[derive(Debug, Default)]
pub struct Incr {
    /// cache lookup key to increment
    key: String,
}

impl Incr {
    /// contruct new Incr command
    pub fn new(key: String) -> Self {
        Incr { key }
    }

    /// Construct new Incr command by consuming the RespReader
    ///
    /// # default
    ///
    /// Return `Incr::default` if RespReader has no stream left
    /// otherwise return the error
    pub fn from_parts(reader: &mut RespReader) -> Result<Self, RespReaderError> {
        let key = reader.next_string()?;
        Ok(Incr { key })
    }

    /// Apply the echo command and write to the Tcp connection stream
    pub async fn apply(self, db: &Db, _dst: &mut Connection) -> crate::Result<Option<RESP>> {
        // set the value in the shared cache.
        let value = db.get(&self.key);
        let mut resp = RESP::Simple("OK".into());

        match value {
            Some(value_type) => match value_type {
                ValueType::String(value) => {
                    let int = String::from_utf8(value.to_vec()).unwrap().parse::<u64>();
                    if let Ok(int) = int {
                        db.set(
                            self.key,
                            ValueType::String(Bytes::from(format!("{}", int + 1))),
                            None,
                        );
                        resp = RESP::Integer(int + 1);
                    } else {
                        // unimplemented!("Value exists but it is not a numerical value");
                        resp = RESP::Error("ERR value is not an integer or out of range".into());
                    }
                }
                ValueType::Stream(_) => unimplemented!("The value is a stream"),
            },
            None => {
                db.set(self.key, ValueType::String(Bytes::from("1")), None);
                resp = RESP::Integer(1);
            }
        }

        Ok(Some(resp))
    }
}

/// Convert Incr command back into an equivalent `RESP`
impl From<Incr> for RESP {
    fn from(value: Incr) -> Self {
        let mut resp = RESP::array();
        resp.push_bulk(Bytes::from("Incr"));
        resp.push_bulk(Bytes::from(value.key.into_bytes()));
        resp
    }
}
