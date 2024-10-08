use std::{
    io::{self, Cursor},
    sync::atomic::AtomicU64,
    time::{Duration, Instant},
};

#[allow(unused_imports)]
use bytes::{Buf, BytesMut};
use futures::{future::BoxFuture, FutureExt};
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

    // keep track of total bytes of replica commands
    // sent to this connection
    pub repl_offset: AtomicU64,
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
            repl_offset: AtomicU64::new(0),
        }
    }

    pub fn get_addr(&mut self) -> String {
        self.stream
            .local_addr()
            .map_or("UknownSocketAddr".to_string(), |socket| socket.to_string())
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

        // We first check if the incoming buffer is a valid RESP
        // by parsing the Cursor through the check method of the RESP
        // If the check returns a OK, we have a valid RESP and we can go
        // ahead to parse the resp and return the corresponding (resp, size) tuple
        //
        // If the incoming buffer is not complete we return a RESPError::Incompelete arm
        // and return Ok(None) so the we keep trying until the connection has enough buffer
        // to extract a valid RESP data structure
        //
        // If the buffer is invalid we return the Err Arm
        match RESP::check(&mut cursor) {
            Ok(_) => {
                // we store the length of the valid RESP to be parsed
                let len = cursor.position() as usize;

                // the check method advances the cursor position while parsing
                // the buffer, we have to reset it to zero before calling the
                // parse method
                cursor.set_position(0);

                // parse the valid RESP
                let resp = RESP::parse_resp(&mut cursor)?;

                // We have to advance the connection buffer by the length
                // of the parsed RESP buffer so we don't reuse the same buffer
                // more than once
                self.buffer.advance(len as usize);

                return Ok(Some((resp, len)));
            }
            // Not enough data present to parse a RESP
            Err(crate::RESPError::Incomplete) => Ok(None),
            Err(err) => return Err(err.into()),
        }
    }

    /// Write a single `RESP` value to the underlying connection stream
    pub fn write_frame<'a>(&'a mut self, resp: &'a RESP) -> BoxFuture<'a, io::Result<()>> {
        async move {
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
        .boxed()
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
            RESP::Array(frames) => {
                // Encode the RESP data type prefix for an array `*`
                self.stream.write_all(b"*").await?;
                self.write_decimal(frames.len() as u64).await?;

                for frame in frames {
                    self.write_frame(&frame).await?;
                }
            }
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
