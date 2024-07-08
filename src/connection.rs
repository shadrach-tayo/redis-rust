use std::{
    io::{self, Cursor},
    time::{Duration, Instant},
};

#[allow(unused_imports)]
use bytes::{Buf, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    // time::timeout,
};

use crate::resp::RESP;

/// Read and write RESP data from the socket
/// to read
#[derive(Debug)]
pub struct Connection {
    /// A self reference to the tcp connection
    stream: TcpStream,

    /// Wrap incoming `TcpStream` with `BufWriter` to provide
    /// buffered writing to the socket
    // stream: BufWriter<TcpStream>,

    /// an in-memory buffer for holding RESP raw bytes for passing
    buffer: BytesMut,

    /// Idle window allowed before closing the connection
    pub idle_close: Duration,

    /// last time the connection was active
    /// i.e received a resp from the client
    pub last_active_time: Option<Instant>,

    ///
    pub closed: bool,

    pub is_master: bool,
}

/// Read bytes from tcpStream and convert to RESP for processing
/// Write RESP to tcp stream
impl Connection {
    pub fn new(stream: TcpStream, is_master: bool) -> Connection {
        Connection {
            stream,
            buffer: BytesMut::with_capacity(4 * 1024),
            idle_close: Duration::from_secs(60 * 60 * 24), // connection ttl = 24 hours
            closed: false,
            last_active_time: None,
            is_master,
        }
    }

    pub async fn flush_stream(&mut self) -> io::Result<()> {
        self.stream.flush().await
    }

    /// Read a single RESP from the connection stream
    pub async fn read_resp(&mut self) -> crate::Result<Option<(RESP, usize)>> {
        loop {
            if let Some(resp) = self.parse_resp()? {
                // println!("Incoming {:?}", resp);
                return Ok(Some(resp));
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("Connection reset by peer".into());
                }
            }
        }
    }

    /// Attempts to parse bytes from the buffered connection
    /// stream to a `RESP` data structure for processing
    pub fn parse_resp(&mut self) -> crate::Result<Option<(RESP, usize)>> {
        let mut cursor = Cursor::new(&self.buffer[..]);
        let _size = self.buffer.len();

        match RESP::parse_resp(&mut cursor) {
            Ok(resp) => {
                // get the current position of the cursor after the resp is
                // successfully parsed
                let pos = cursor.position() as usize;

                // advance the connection buffer by the pos
                // This discards the read buffer and next calls to read from the
                // buffer starts from pos.
                // self.buffer.advance(pos);
                let _ = self.buffer.split_to(pos);

                return Ok(Some((resp, pos)));
            }
            // Not enough data present to parse a RESP
            Err(crate::RESPError::Incomplete) => Ok(None),
            Err(err) => return Err(err.into()),
        }
    }

    /// Write a single `RESP` value to the underlying connection stream
    pub async fn write_frame(&mut self, resp: &RESP) -> io::Result<()> {
        // println!("Write resp {:?}", &resp);
        match resp {
            RESP::Array(list) => {
                // Encode the RESP data type prefix for an array `*`
                self.stream.write_all(b"*").await?;
                self.write_decimal(list.len() as u64).await?;

                for resp in list {
                    self.write_value(resp).await?;
                }
            }
            // resp is a literal type not a list/aggregate
            _ => self.write_value(resp).await?,
        }

        // println!("Outgoing Buffer: {:?}", resp);
        self.stream.flush().await
    }

    /// Write a single `RESP` value to the underlying connection stream
    async fn write_value(&mut self, resp: &RESP) -> io::Result<()> {
        match resp {
            RESP::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            RESP::Error(error) => {
                self.stream.write_all(b"-").await?;
                self.stream.write_all(error.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            RESP::Simple(string) => {
                self.stream.write_all(b"+").await?;
                self.stream.write_all(string.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            RESP::Integer(int) => {
                self.stream.write_all(b":").await?;
                self.write_decimal(*int).await?;
            }
            RESP::Bulk(data) => {
                self.stream.write_all(b"$").await?;
                let len = data.len() as u64;
                self.write_decimal(len).await?;
                self.stream.write_all(data).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            RESP::File(data) => {
                self.stream.write_all(b"$").await?;
                let len = data.len() as u64;
                self.write_decimal(len).await?;
                println!("Write File: {}", len);
                self.stream.write_all(data).await?;
            }
            RESP::Array(_) => unimplemented!(),
        }

        Ok(())
    }

    /// Write a decimal to the stream
    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        let mut buf = [0u8, 20];
        let mut buf = Cursor::new(&mut buf[..]);

        write!(&mut buf, "{}", val).unwrap();

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}
