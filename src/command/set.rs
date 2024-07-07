use tokio::time::Duration;

use bytes::Bytes;

use crate::{connection::Connection, resp::RESP, Db, RespReader, RespReaderError};

#[derive(Debug, Default)]
pub struct Set {
    /// cache lookup key
    key: String,

    // value to store in db
    value: Bytes,
    // expiration time of key
    expire: Option<Duration>,
}

impl Set {
    /// contruct new Set command
    pub fn new(key: String, value: Bytes, expire: Option<Duration>) -> Self {
        Set { key, value, expire }
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

        let mut expire = None;

        match reader.next_string() {
            // parse PX argument to SET command
            Ok(s) if s.to_lowercase() == "px" => {
                let duration = reader.next_int().map(|dur| Duration::from_millis(dur))?;
                expire = Some(duration);
            }

            // parse EX argument to SET command
            Ok(s) if s.to_lowercase() == "ex" => {
                let duration = reader.next_int().map(|dur| Duration::from_secs(dur))?;
                expire = Some(duration);
            }
            Ok(arg) => {
                return Err(RespReaderError::Other(format!(
                    "Unsupported argument to SET: {}",
                    arg
                )))
            }
            Err(_) => {}
        }

        Ok(Set { key, value, expire })
    }

    /// Apply the echo command and write to the Tcp connection stream
    pub async fn apply(self, db: &Db, _dst: &mut Connection) -> crate::Result<Option<RESP>> {
        // set the value in the shared cache.
        db.set(self.key, self.value, self.expire);

        Ok(Some(RESP::Simple("OK".into())))
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
