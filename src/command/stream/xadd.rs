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
            id: None, // Some((stream_id_split[0], stream_id_split[1])),
            stream_id: Some(stream_id),
        })
    }

    fn is_stream_id_valid(&self) -> bool {
        let ids = self
            .stream_id
            .clone()
            .unwrap_or("".to_string())
            .split('-')
            .map(|char| char.parse().unwrap())
            .collect::<Vec<u64>>();

        if ids.len() > 2 {
            return false;
        }

        if ids[1] < 1 {
            return false;
        }

        return true;
    }

    /// Apply the stream command and write to the Tcp connection stream
    pub async fn apply(self, db: &Db) -> crate::Result<Option<RESP>> {
        if !self.is_stream_id_valid() {
            return Ok(Some(RESP::Error(
                "ERR The ID specified in XADD must be greater than 0-0".to_string(),
            )));
        }

        let id = self
            .stream_id
            .clone()
            .unwrap()
            .split('-')
            .map(|char| char.parse().unwrap())
            .collect::<Vec<u64>>();

        let new_stream = StreamData {
            id: (id[0], id[1]),
            pairs: self.fields,
        };

        let prev_stream = db.get(&self.key);

        let mut streams = if let Some(prev_stream) = prev_stream {
            match prev_stream {
                ValueType::Stream(stream) => stream,
                _ => vec![],
            }
        } else {
            vec![]
        };

        // The ID should be greater than the ID of the last entry in the stream.
        // The millisecondsTime part of the ID should be greater than or equal to the millisecondsTime of the last entry.
        // If the millisecondsTime part of the ID is equal to the millisecondsTime of the last entry, the sequenceNumber part of the ID should be greater than the sequenceNumber of the last entry.
        // If the stream is empty, the ID should be greater than 0-0
        for stream in streams.iter() {
            if stream.id >= new_stream.id {
                return Ok(Some(RESP::Error(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string(),
                    )));
            }
        }

        streams.push(new_stream);

        println!("Stream: {}", self.key);
        println!("Data: {:?}", &streams);

        let value = ValueType::Stream(streams);

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
