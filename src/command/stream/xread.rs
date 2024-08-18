use std::time::Duration;

use crate::{resp::RESP, Db, RespReader, RespReaderError, ValueType};
use bytes::Bytes;
use tokio::time::Instant;

#[derive(Debug, Default)]
pub struct XRead {
    pub streams: Vec<StreamFilter>,
    pub block: Option<u64>, // pub stream_ids: Vec<(u64, u64)>,
}

#[derive(Debug, Default, Clone)]
pub struct StreamFilter {
    key: String,
    id: (u64, u64),
    created_at_filter: Option<Instant>,
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
    pub fn from_parts(reader: &mut RespReader) -> Result<Self, RespReaderError> {
        let mut streams = vec![];
        let mut keys = vec![];
        let mut ids = vec![];

        let mut block = None;

        while let Ok(next) = reader.next_string() {
            match next.to_lowercase().as_str() {
                "block" => {
                    block = Some(reader.next_int()?);
                }
                "streams" => continue,
                "count" => unimplemented!("COUNT option not implement for XREAD ❌"),
                "$" => ids.push("$".to_string()),
                next => {
                    let parts = next
                        .split('-')
                        .map(|char| char.to_string())
                        .collect::<Vec<String>>();
                    if parts.len() == 2 {
                        ids.push(next.to_owned());
                    } else {
                        keys.push(next.to_owned());
                    }
                }
            }
        }

        for (idx, key) in keys.iter().enumerate() {
            let id = ids
                .get(idx)
                .map(|p| p.to_owned())
                .unwrap_or("0-0".to_string());

            if id == "$" {
                streams.push(StreamFilter {
                    key: key.to_owned(),
                    id: (0, 0),
                    created_at_filter: Some(Instant::now()),
                })
            } else {
                streams.push(StreamFilter {
                    key: key.to_owned(),
                    id: get_range_value(id),
                    created_at_filter: None,
                })
            }
        }

        Ok(XRead { streams, block })
    }

    async fn run_command(&self, db: &Db) -> Vec<RESP> {
        self.streams
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
                            if stream.created_at_filter.is_some() {
                                if stream.created_at_filter.unwrap() < entry._created_at {
                                    Some(entry.into())
                                } else {
                                    None
                                }
                            } else if entry.id > stream.id {
                                Some(entry.into())
                            } else {
                                None
                            }
                        })
                        .collect();

                    if results.len() == 0 {
                        return None;
                    }

                    let mut field_resp = RESP::array();

                    for result in results {
                        field_resp.push(result);
                    }

                    stream_resp.push_bulk(Bytes::from(stream.key.to_owned()));
                    stream_resp.push(field_resp);

                    Some(stream_resp)
                }
            })
            .collect()
    }

    /// Apply the stream command and write to the Tcp connection stream
    pub async fn apply(self, db: &Db) -> crate::Result<Option<RESP>> {
        let mut resp = RESP::Null;

        let xreads = match self.block {
            Some(0) => {
                let mut streams = self.run_command(db).await;
                while streams.len() == 0 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    streams = self.run_command(db).await;
                }
                streams
            }
            Some(timeout) => {
                tokio::time::sleep(Duration::from_millis(timeout)).await;
                self.run_command(db).await
            }
            None => self.run_command(db).await,
        };

        if xreads.len() > 0 {
            resp = RESP::array();
        }

        for data in xreads.iter() {
            resp.push(data.to_owned());
        }

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
