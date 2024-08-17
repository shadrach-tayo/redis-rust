use crate::{resp::RESP, Db, RespReader, RespReaderError, ValueType};
use bytes::Bytes;

// const MAX_TIMESTAMP: u64 = 32536799999000; // '2038-01-19 03:14:07' UTC.

#[derive(Debug, Default)]
pub struct XRange {
    pub key: String,
    pub start: (u64, u64),
    pub end: (u64, u64),
    pub mode: Option<String>,
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

impl XRange {
    pub fn new(key: String) -> Self {
        XRange {
            key,
            ..XRange::default()
        }
    }

    /// Construct new Stream command by consuming the RespReader
    ///
    /// Parse next_string()? to get the pair key
    /// Parse next_string()? to get the pair value
    ///
    pub fn from_parts(reader: &mut RespReader) -> Result<Self, RespReaderError> {
        let key = reader.next_string()?;

        let (start, end, mode) = match reader.next_string() {
            Ok(range_or_mode) if range_or_mode == "-" => {
                let end = get_range_value(reader.next_string()?);
                ((0, 0), end, Some(range_or_mode))
            }
            Ok(start) => {
                let start = get_range_value(start);
                let value = reader.next_string()?;
                let mut end: (u64, u64) = (0, 0);
                let mut mode = None;
                if value == "+" {
                    mode = Some(value);
                } else {
                    end = get_range_value(value);
                }
                (start, end, mode)
            }
            Err(err) => return Err(err.into()),
        };

        println!("xrange: {key}: {:?}-{:?}", start, end);
        Ok(XRange {
            key,
            start,
            end,
            mode,
        })
    }

    /// Apply the stream command and write to the Tcp connection stream
    pub async fn apply(self, db: &Db) -> crate::Result<Option<RESP>> {
        let streams = db.get(&self.key);

        let streams = if let Some(prev_stream) = streams {
            match prev_stream {
                ValueType::Stream(stream) => stream,
                _ => vec![],
            }
        } else {
            vec![]
        };

        let mut resp = RESP::array();

        let xrange: Vec<RESP> = match self.mode {
            None => streams
                .iter()
                .filter_map(|entry| {
                    if entry.id >= self.start && entry.id <= self.end {
                        let mut stream_resp = RESP::array();
                        stream_resp
                            .push_bulk(Bytes::from(format!("{}-{}", entry.id.0, entry.id.1)));
                        let mut inner_resp = RESP::array();
                        for (key, value) in entry.pairs.iter() {
                            inner_resp.push_bulk(Bytes::from(key.to_owned()));
                            inner_resp.push_bulk(Bytes::from(value.to_owned()));
                        }
                        stream_resp.push(inner_resp);
                        Some(stream_resp)
                    } else {
                        None
                    }
                })
                .collect(),
            Some(mode) if mode == "-" => streams
                .iter()
                .filter_map(|entry| {
                    if entry.id <= self.end {
                        let mut stream_resp = RESP::array();
                        stream_resp
                            .push_bulk(Bytes::from(format!("{}-{}", entry.id.0, entry.id.1)));
                        let mut inner_resp = RESP::array();
                        for (key, value) in entry.pairs.iter() {
                            inner_resp.push_bulk(Bytes::from(key.to_owned()));
                            inner_resp.push_bulk(Bytes::from(value.to_owned()));
                        }
                        stream_resp.push(inner_resp);
                        Some(stream_resp)
                    } else {
                        None
                    }
                })
                .collect(),
            Some(mode) if mode == "+" => streams
                .iter()
                .filter_map(|entry| {
                    if entry.id >= self.start {
                        let mut stream_resp = RESP::array();
                        stream_resp
                            .push_bulk(Bytes::from(format!("{}-{}", entry.id.0, entry.id.1)));
                        let mut inner_resp = RESP::array();
                        for (key, value) in entry.pairs.iter() {
                            inner_resp.push_bulk(Bytes::from(key.to_owned()));
                            inner_resp.push_bulk(Bytes::from(value.to_owned()));
                        }
                        stream_resp.push(inner_resp);
                        Some(stream_resp)
                    } else {
                        None
                    }
                })
                .collect(),
            Some(unsupported) => panic!("unsupported XRANGE query {unsupported}"),
        };

        for data in xrange.iter() {
            resp.push(data.to_owned());
        }
        println!("XRANGE: {:?}", &resp);

        Ok(Some(resp))
    }
}

impl From<XRange> for RESP {
    fn from(this: XRange) -> Self {
        let mut resp = RESP::array();
        resp.push_bulk(Bytes::from("XRANGE"));
        resp.push_bulk(Bytes::from(this.key));
        resp.push_bulk(Bytes::from(format!("{}-{}", this.start.0, this.start.1)));
        resp.push_bulk(Bytes::from(format!("{}-{}", this.end.0, this.end.1)));
        resp
    }
}
