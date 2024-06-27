use bytes::Bytes;

use crate::{connection::Connection, frame::RESP, Db, RespReader, RespReaderError};

#[derive(Debug, Default)]
pub struct Info {
    section: String,
}

impl Info {
    /// contruct new Info command
    pub fn new(section: String) -> Self {
        Info { section }
    }

    /// Apply the echo command and write to the Tcp connection stream
    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // let resp = RESP::Bulk(Bytes::from("role:master"));

        // dbg!(&resp);
        let role = db.get_role();
        let resp = format!("role:{}", role);
        dst.write_frame(&RESP::Bulk(Bytes::from(resp))).await?;

        Ok(())
    }

    pub fn from_parts(reader: &mut RespReader) -> Result<Self, RespReaderError> {
        let section = match reader.next_string() {
            Ok(s) if s.to_lowercase() == "replication" => s.to_string(),
            Ok(invalid_section) => {
                return Err(RespReaderError::Other(format!(
                    "Info command Invalid section: {}",
                    invalid_section
                )))
            }
            Err(err) => return Err(err),
        };

        Ok(Self { section })
    }
}

impl From<Info> for RESP {
    fn from(value: Info) -> Self {
        let mut resp = RESP::array();
        resp.push_bulk(Bytes::from("INFO"));
        resp.push_bulk(Bytes::from(value.section));
        resp
    }
}
