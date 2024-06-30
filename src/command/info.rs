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
    pub async fn apply(self, db: &Db, _dst: &mut Connection) -> crate::Result<Option<RESP>> {
        // dbg!(&resp);
        let role = db.get_role();
        let mut data: String = "role:".to_owned();
        data.push_str(role.as_str());
        data.push_str("\r\n");

        let repl_info = db.get_repl_info();
        if repl_info.0.is_some() {
            data.push_str("master_replid:");
            data.push_str(repl_info.0.unwrap_or("".to_owned()).as_str());
            data.push_str("\r\n");

            data.push_str("master_repl_offset:");
            data.push_str(repl_info.1.to_string().as_str());
            data.push_str("\r\n");
        }

        let resp = RESP::Bulk(Bytes::from(data));
        // dst.write_frame(&resp).await?;

        Ok(Some(resp))
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
