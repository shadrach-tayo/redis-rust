use bytes::Bytes;

use crate::{connection::Connection, frame::RESP, RespReader, RespReaderError};

#[derive(Debug, Default)]
pub struct Ping {
    msg: Option<Bytes>,
}

impl Ping {
    /// contruct new Ping command
    pub fn new(msg: Option<Bytes>) -> Self {
        Ping { msg }
    }

    /// Construct new Ping command by consuming the RespReader
    ///
    /// # default
    ///
    /// Return `Ping::default` if RespReader has no stream left
    /// otherwise return the error
    pub fn from_parts(reader: &mut RespReader) -> Result<Self, RespReaderError> {
        match reader.next_byte() {
            Ok(msg) => Ok(Ping { msg: Some(msg) }),
            Err(RespReaderError::EndOfStream) => Ok(Ping::default()),
            Err(err) => Err(err.into()),
        }
    }

    /// Apply the echo command and write to the Tcp connection stream
    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let resp = match self.msg {
            Some(_msg) => RESP::Simple("PONG".to_string()), //RESP::Bulk(msg),
            None => RESP::Simple("PONG".to_string()),
        };

        // dbg!(&resp);

        dst.write_frame(&resp).await?;

        Ok(())
    }
}

/// Convert Ping command back into an equivalent `RESP`
impl From<Ping> for RESP {
    fn from(value: Ping) -> Self {
        let mut resp = RESP::array();
        resp.push_bulk(Bytes::from("ping"));
        if let Some(msg) = value.msg {
            resp.push_bulk(Bytes::from(msg));
        }
        resp
    }
}
