use std::{
    io::{self, Cursor},
    time::{Duration, Instant},
};

use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::frame::RESP;

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
    /// i.e received a frame from the client
    pub last_active_time: Option<Instant>,

    ///
    pub closed: bool,
}

/// Read bytes from tcpStream and convert to RESP for processing
/// Write RESP to tcp stream
impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            stream,
            buffer: BytesMut::with_capacity(4096),
            idle_close: Duration::from_secs(10),
            closed: false,
            last_active_time: None,
        }
    }

    /// Read a single RESP from the connection stream
    pub async fn read_resp(&mut self) -> crate::Result<Option<RESP>> {
        let size = self.stream.read_buf(&mut self.buffer).await?;

        if size == 0 {
            return Ok(None);
        }

        let resp = self.parse_frame()?;
        Ok(resp)
    }

    /// Attempts to parse bytes from the buffered connection
    /// stream to a `RESP` data structure for processing
    pub fn parse_frame(&mut self) -> crate::Result<Option<RESP>> {
        println!(
            "Incoming Buffer {:?}",
            String::from_utf8(self.buffer.clone().to_vec())
        );
        let mut cursor = Cursor::new(&self.buffer[..]);

        match RESP::parse_frame(&mut cursor) {
            Ok(resp) => {
                let _ = self.buffer.split();
                return Ok(Some(resp));
            }
            // Not enough data present to parse a RESP
            Err(crate::RESPError::Incomplete) => Ok(None),
            Err(err) => return Err(err.into()),
        }
    }

    /// Write a single `RESP` value to the underlying connection stream
    pub async fn write_frame(&mut self, frame: &RESP) -> io::Result<()> {
        println!("Write frame {:?}", &frame);
        match frame {
            RESP::Array(list) => {
                // Encode the RESP data type prefix for an array `*`
                self.stream.write_all(b"*").await?;
                self.write_decimal(list.len() as u64).await?;

                for frame in list {
                    self.write_value(frame).await?;
                }
            }
            // frame is a literal type not a list/aggregate
            _ => self.write_value(frame).await?,
        }

        // println!("Outgoing Buffer: {:?}", frame);
        self.stream.flush().await
    }

    /// Write a single `RESP` value to the underlying connection stream
    async fn write_value(&mut self, frame: &RESP) -> io::Result<()> {
        match frame {
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

                // if String::from_utf8(data.to_vec()).unwrap().to_lowercase() == "ping" {
                //     self.stream.write_all(b"PONG").await?;
                // } else {
                self.stream.write_all(data).await?;
                // }

                self.stream.write_all(b"\r\n").await?;
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
