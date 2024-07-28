use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;

use crate::{connection::Connection, resp::RESP, RespReader, RespReaderError};

#[derive(Debug, Default)]
pub struct Replconf {
    // key: String,
    pub values: Vec<String>,
}

impl Replconf {
    pub fn new(values: Vec<String>) -> Self {
        Replconf { values }
    }
    /// Returns command name
    pub fn get_name(&self) -> &str {
        "REPLCONF"
    }

    /// Construct new REPLCONF command by consuming the RespReader
    ///
    /// Parse next_string()? to get the config key
    /// Parse next_string()? to get the config value
    ///
    pub fn from_parts(reader: &mut RespReader) -> Result<Self, RespReaderError> {
        // let key = reader.next_string()?;
        let mut values = vec![];
        let mut value = reader.next_string();

        while value.is_ok() {
            values.push(value.unwrap());
            value = reader.next_string();
        }

        Ok(Replconf { values })
    }

    /// Apply the echo command and write to the Tcp connection stream
    pub async fn apply(
        self,
        dst: &mut Connection,
        offset: Option<&AtomicUsize>,
    ) -> crate::Result<Option<RESP>> {
        #[allow(unused_assignments)]
        let mut resp = RESP::Simple("OK".to_owned());

        let mut value_iter = self.values.iter();

        let key = value_iter.next();
        let cmd = value_iter.next();

        match (key, cmd) {
            (Some(key), Some(cmd))
                if key.to_lowercase() == "getack" && cmd.to_lowercase() == "*" =>
            {
                let offset_bytes = offset.unwrap().load(Ordering::SeqCst).to_string();
                resp = RESP::Array(vec![
                    RESP::Bulk(Bytes::from("REPLCONF".as_bytes())),
                    RESP::Bulk(Bytes::from("ACK".as_bytes())),
                    RESP::Bulk(Bytes::from(offset_bytes)),
                ]);
            }
            _ => (),
        }

        // println!("Write RESPONSE {:?}", &resp);
        // eagerly send reply to replica connection
        dst.write_frame(&resp).await?;

        Ok(None)
    }
}

impl From<Replconf> for RESP {
    fn from(this: Replconf) -> Self {
        let mut resp = RESP::array();
        resp.push_bulk(Bytes::from("REPLCONF"));
        for value in this.values.iter() {
            resp.push_bulk(Bytes::from(value.clone()));
        }
        resp
    }
}
