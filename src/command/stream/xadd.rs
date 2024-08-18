use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use tokio::time::Instant;

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
            id: None,
            stream_id: Some(stream_id),
        })
    }

    fn is_stream_id_valid(&self) -> bool {
        let ids = self
            .stream_id
            .clone()
            .unwrap_or("".to_string())
            .split('-')
            .map(|char| char.to_string())
            .collect::<Vec<String>>();

        if ids.len() > 2 {
            return false;
        }

        if ids.len() == 2 {
            if ids[1] == "0" {
                return ids[0] != "0";
            }
        }

        if ids.len() == 1 && ids[0] != "*" {
            return false;
        }

        return true;
    }

    fn get_stream_id(&self, next_sequence_id: u64) -> (u64, u64) {
        let ids = self
            .stream_id
            .clone()
            .unwrap()
            .split('-')
            .map(|char| char.to_string())
            .collect::<Vec<String>>();

        let now = {
            let start = SystemTime::now();
            let since_the_epoch = start
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards");
            since_the_epoch.as_secs() * 1000 + since_the_epoch.subsec_nanos() as u64 / 1_000_000
        };

        let millisec = ids
            .get(0)
            .map(|t| if t == "*" { now } else { t.parse().unwrap() })
            .or(Some(now))
            .unwrap();

        let sequence_id = ids
            .get(1)
            .map(|t| {
                if t == "*" {
                    next_sequence_id
                } else {
                    t.parse().unwrap()
                }
            })
            .or(Some(next_sequence_id))
            .unwrap();

        (millisec, sequence_id)
    }

    /// Apply the stream command and write to the Tcp connection stream
    pub async fn apply(self, db: &Db) -> crate::Result<Option<RESP>> {
        if !self.is_stream_id_valid() {
            return Ok(Some(RESP::Error(
                "ERR The ID specified in XADD must be greater than 0-0".to_string(),
            )));
        }

        let prev_stream = db.get(&self.key);

        let mut streams = if let Some(prev_stream) = prev_stream {
            match prev_stream {
                ValueType::Stream(stream) => stream,
                _ => vec![],
            }
        } else {
            vec![]
        };

        let next_sequence_id = {
            let ids = self
                .stream_id
                .clone()
                .unwrap_or("".to_string())
                .split('-')
                .map(|char| char.to_string())
                .collect::<Vec<String>>();

            if ids[0] == "*" {
                if ids.len() == 1 {
                    0
                } else {
                    1
                }
            } else {
                let time_part: u64 = ids[0].parse().unwrap();
                let time_match = streams
                    .iter()
                    .rev()
                    .find(|stream| stream.id.0 == time_part)
                    .map(|stream| stream.id.1);

                if time_match.is_some() {
                    println!("Match found: {:?}", time_match);
                    time_match.unwrap() + 1
                } else {
                    if ids[0] == "0" {
                        1
                    } else {
                        0
                    }
                }
            }
        };

        println!("Next Sequence: {next_sequence_id}");

        let stream_id = self.get_stream_id(next_sequence_id);

        let next_stream_id = format!("{}-{}", stream_id.0, stream_id.1);

        let new_stream = StreamData {
            id: stream_id,
            pairs: self.fields,
            _created_at: Instant::now(),
        };

        // The ID should be greater than the ID of the last entry in the stream.
        // The millisecondsTime part of the ID should be greater than or equal to the millisecondsTime of the last entry.
        // If the millisecondsTime part of the ID is equal to the millisecondsTime of the last entry, the sequenceNumber part of the ID should be greater than the sequenceNumber of the last entry.
        // If the stream is empty, the ID should be greater than 0-0
        for stream in streams.iter() {
            if stream.id >= new_stream.id {
                println!("Prev Stream: {:?}, Stream: {:?}", stream.id, new_stream.id);
                return Ok(Some(RESP::Error(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string(),
                    )));
            }
        }

        streams.push(new_stream);

        let value = ValueType::Stream(streams);

        db.set(self.key, value, None);

        Ok(Some(RESP::Bulk(Bytes::from(next_stream_id))))
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
