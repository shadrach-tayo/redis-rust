use bytes::Bytes;

use crate::{connection::Connection, frame::RESP, RespReader, RespReaderError};

#[derive(Debug, Default)]
pub struct Replconf {
    key: String,
    value: String,
}

impl Replconf {
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
        let key = reader.next_string()?;
        let value = reader.next_string()?;

        Ok(Replconf { key, value })
    }

    /// Apply the echo command and write to the Tcp connection stream
    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        #[allow(unused_assignments)]
        let resp = RESP::Simple("OK".to_owned());

        dbg!(&resp);

        dst.write_frame(&resp).await?;

        Ok(())
    }
}

impl From<Replconf> for RESP {
    fn from(value: Replconf) -> Self {
        let mut resp = RESP::array();
        resp.push_bulk(Bytes::from("REPLCONF"));
        resp.push_bulk(Bytes::from(value.key));
        resp.push_bulk(Bytes::from(value.value));
        resp
    }
}
