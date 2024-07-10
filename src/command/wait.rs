use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;

use crate::{connection::Connection, resp::RESP, RespReader, RespReaderError};

#[derive(Debug, Default)]
pub struct Wait {
    pub no_of_replicas: u64,
    pub timeout: u64,
}

impl Wait {
    pub fn new(replicas: u64, timeout: u64) -> Self {
        Wait {
            no_of_replicas: replicas,
            timeout,
        }
    }
    /// Returns command name
    pub fn get_name(&self) -> &str {
        "Wait"
    }

    /// Construct new Wait command by consuming the RespReader
    ///
    /// Parse next_string()? to get the config key
    /// Parse next_string()? to get the config value
    ///
    pub fn from_parts(reader: &mut RespReader) -> Result<Self, RespReaderError> {
        let no_of_replicas = reader.next_int()?;
        let timeout = reader.next_int()?;

        Ok(Wait {
            no_of_replicas,
            timeout,
        })
    }

    /// Apply the echo command and write to the Tcp connection stream
    pub async fn apply(
        self,
        dst: &mut Connection,
        replicas: &AtomicUsize,
    ) -> crate::Result<Option<RESP>> {
        let int = replicas
            .load(Ordering::SeqCst)
            .to_string()
            .parse()
            .unwrap_or(0);
        let resp = RESP::Integer(int);

        dst.write_frame(&resp).await?;

        Ok(None)
    }
}

impl From<Wait> for RESP {
    fn from(value: Wait) -> Self {
        let resp = RESP::Array(vec![
            RESP::Bulk("WAIT".into()),
            RESP::Bulk(Bytes::from(value.no_of_replicas.to_string())),
            RESP::Bulk(Bytes::from(value.timeout.to_string())),
        ]);

        resp
    }
}
