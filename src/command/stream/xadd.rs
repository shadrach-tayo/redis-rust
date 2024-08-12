use std::collections::HashMap;

use bytes::Bytes;

use crate::{resp::RESP, Db, RespReader, RespReaderError, StreamData, ValueType};

#[derive(Debug, Default)]
pub struct XAdd {
    pub key: String,
    pub id: Option<(u64, u64)>,
    pub stream_id: Option<String>,
    pub fields: HashMap<String, String>,
}

impl XAdd {
    pub fn new(key: String) -> Self {
        XAdd {
            key,
            ..XAdd::default()
        }
    }

    /// Construct new Stream command by consuming the RespReader
    ///
    /// Parse next_string()? to get the pair key
    /// Parse next_string()? to get the pair value
    ///
    pub fn from_parts(reader: &mut RespReader) -> Result<Self, RespReaderError> {
        let key = reader.next_string()?;
        let stream_id = reader.next_string()?;

        let stream_id_split = stream_id
            .split('-')
            .map(|char| char.parse().unwrap())
            .collect::<Vec<u64>>();

        let mut pairs = HashMap::new();

        loop {
            let field_id = reader.next_string();
            if field_id.is_ok() {
                pairs.insert(field_id.unwrap(), reader.next_string()?);
            } else {
                break;
            }
        }

        Ok(XAdd {
            key,
            fields: pairs,
            id: Some((stream_id_split[0], stream_id_split[1])),
            stream_id: Some(stream_id),
        })
    }

    /// Apply the echo command and write to the Tcp connection stream
    pub async fn apply(self, db: &Db) -> crate::Result<Option<RESP>> {
        let stream = StreamData {
            id: self.id.unwrap(),
            pairs: self.fields,
        };

        let value = ValueType::Stream(vec![stream]);

        db.set(self.key, value, None);

        Ok(Some(RESP::Simple(self.stream_id.unwrap())))
    }
}

impl From<XAdd> for RESP {
    fn from(this: XAdd) -> Self {
        let mut resp = RESP::array();
        resp.push_bulk(Bytes::from("XADD"));
        resp.push_bulk(Bytes::from(this.key));
        resp.push_bulk(Bytes::from(this.stream_id.unwrap()));
        for (key, value) in this.fields.into_iter() {
            resp.push_bulk(Bytes::from(key));
            resp.push_bulk(Bytes::from(value));
        }
        resp
    }
}
