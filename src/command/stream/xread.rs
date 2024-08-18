use crate::{resp::RESP, Db, RespReader, RespReaderError, ValueType};
use bytes::Bytes;

#[derive(Debug, Default)]
pub struct XRead {
    pub streams: Vec<StreamFilter>,
    // pub stream_ids: Vec<(u64, u64)>,
}

#[derive(Debug, Default, Clone)]
pub struct StreamFilter {
    key: String,
    id: (u64, u64),
}

fn get_range_value(string: String) -> (u64, u64) {
    let ids = string
        .split('-')
        .map(|char| char.to_string())
        .collect::<Vec<String>>();

    let millisec = ids.get(0).map(|t| t.parse().unwrap()).unwrap();

    let sequence_id = ids.get(1).map(|t| t.parse().unwrap()).or(Some(0)).unwrap();

    (millisec, sequence_id)
}

impl XRead {
    pub fn new(streams: Vec<StreamFilter>) -> Self {
        XRead {
            streams,
            ..XRead::default()
        }
    }

    /// Construct new XRead command by consuming the RespReader
    ///
    /// Parse next_string()? to get the pair key
    /// Parse next_string()? to get the pair value
    ///
    pub fn from_parts(reader: &mut RespReader) -> Result<Self, RespReaderError> {
        let option = reader.next_string()?;

        assert!(option == "streams");

        let mut streams = vec![];
        let mut keys = vec![];
        let mut ids = vec![];

        while let Ok(next) = reader.next_string() {
            let parts = next
                .split('-')
                .map(|char| char.to_string())
                .collect::<Vec<String>>();
            if parts.len() == 2 {
                ids.push(next);
            } else {
                keys.push(next);
            }
        }

        for (idx, key) in keys.iter().enumerate() {
            let id = ids
                .get(idx)
                .map(|p| p.to_owned())
                .unwrap_or("0-0".to_string());
            streams.push(StreamFilter {
                key: key.to_owned(),
                id: get_range_value(id),
            })
        }

        // println!("XRead: {:?}", streams);
        Ok(XRead { streams })
    }

    /// Apply the stream command and write to the Tcp connection stream
    pub async fn apply(self, db: &Db) -> crate::Result<Option<RESP>> {
        let mut resp = RESP::array();

        let xreads: Vec<RESP> = self
            .streams
            .iter()
            .filter_map(|stream| {
                let streams = db.get(&stream.key);
                let streams = if let Some(prev_stream) = streams {
                    match prev_stream {
                        ValueType::Stream(stream) => Some(stream),
                        _ => None,
                    }
                } else {
                    return None;
                };

                if streams.is_none() {
                    None
                } else {
                    let streams = streams.unwrap();

                    let mut stream_resp = RESP::array();

                    let results: Vec<RESP> = streams
                        .iter()
                        .filter_map(|entry| {
                            if entry.id > stream.id {
                                let mut entry_resp = RESP::array();
                                entry_resp.push_bulk(Bytes::from(format!(
                                    "{}-{}",
                                    entry.id.0, entry.id.1
                                )));
                                let mut inner_resp = RESP::array();
                                for (key, value) in entry.pairs.iter() {
                                    inner_resp.push_bulk(Bytes::from(key.to_owned()));
                                    inner_resp.push_bulk(Bytes::from(value.to_owned()));
                                }
                                entry_resp.push(inner_resp);
                                Some(entry_resp)
                            } else {
                                None
                            }
                        })
                        .collect();

                    let mut field_resp = RESP::array();

                    for result in results {
                        field_resp.push(result);
                    }

                    stream_resp.push_bulk(Bytes::from(stream.key.to_owned()));
                    stream_resp.push(field_resp);

                    Some(stream_resp)
                }
            })
            .collect();

        for data in xreads.iter() {
            resp.push(data.to_owned());
        }
        println!("XREAD: {:?}", &resp);

        Ok(Some(resp))
    }
}

impl From<XRead> for RESP {
    fn from(this: XRead) -> Self {
        let mut resp = RESP::array();
        resp.push_bulk(Bytes::from("XREAD"));
        resp.push_bulk(Bytes::from("streams"));
        for stream in this.streams.iter() {
            resp.push_bulk(Bytes::from(stream.key.to_owned()));
        }
        for stream in this.streams.iter() {
            resp.push_bulk(Bytes::from(format!("{}-{}", stream.id.0, stream.id.1)));
        }

        resp
    }
}
