// Uncomment this block to pass the first stage
use std::{
    fmt,
    io::{BufReader, Cursor, Read, Write},
    net::{TcpListener, TcpStream},
    num::TryFromIntError,
    path::Display,
    str::Utf8Error,
    string::FromUtf8Error,
};

use bytes::{Buf, Bytes, BytesMut};

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                handle_connections(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connections(mut stream: TcpStream) {
    // let mut reader = BufReader::new(&mut stream);
    let mut buf = Vec::<u8>::new();
    let response = "*2\r\n+PONG\r\n+PONG\r\n";

    loop {
        match stream.read(&mut buf) {
            Ok(size) => {
                println!("read buffer {}, size {size}", buf.len());
                if size == 0 {
                    break;
                }
                let frame_result = parse_frame(&mut buf);
                match frame_result {
                    Ok(frame) => println!("Parsed frame: {:?}", frame),
                    Err(err) => {
                        println!("Error parsing frames {}", err);
                    }
                }
                println!("Still reading buffer");
            }
            Err(_) => todo!(),
        }
    }
    stream.write_all(response.as_bytes()).unwrap();
}

fn parse_frame(buf: &mut Vec<u8>) -> Result<Frame, FrameError> {
    let mut cursor = Cursor::new(&buf[..]);
    println!(
        "parse_frame :{}, remaining: {}",
        buf.len(),
        cursor.has_remaining()
    );
    match get_u8(&mut cursor)? {
        b'+' => {
            // strings frame
            let line = get_line(&mut cursor)?.to_vec();
            let string = String::from_utf8(line)?;
            println!("parse string {}", &string);
            Ok(Frame::Simple(string))
        }
        b'*' => {
            // arrays frame
            let len = get_decimal(&mut cursor)?.try_into()?;
            println!("parse decimal {}", len);
            let mut out = Vec::with_capacity(len as usize);
            for _ in 0..len {
                out.push(parse_frame(buf)?);
            }
            Ok(Frame::Array(out))
        }
        b'$' => {
            // bulk strings frame
            todo!()
        }
        b':' => {
            // integers frame
            let int = get_decimal(&mut cursor)?;
            Ok(Frame::Integer(int))
        }
        b'-' => {
            // simple errors frame
            todo!()
        }
        b'_' => {
            // null frame
            todo!()
        }
        _ => unimplemented!(),
    }

    // Ok(res)
}

fn check_frame(src: &mut Cursor<&[u8]>) -> Result<(), FrameError> {
    match get_u8(src)? {
        b'+' => {
            // strings frame
            println!("get u8");
            Ok(())
        }
        b'*' => {
            // arrays frame
            todo!()
        }
        b'$' => {
            // bulk strings frame
            todo!()
        }
        b':' => {
            // integers frame
            todo!()
        }
        b'-' => {
            // simple errors frame
            todo!()
        }
        b'_' => {
            // null frame
            todo!()
        }
        err => Err(format!("Error reading request {}", err).into()),
    }
}

fn get_line<'a>(src: &'a mut Cursor<&[u8]>) -> Result<&'a [u8], FrameError> {
    println!("parse line");
    let start = src.position() as usize;
    let end = src.get_ref().len() - 1;

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            src.set_position((i + 2) as u64);
            println!("parsed line {:?}", &src.get_ref()[start..i]);
            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(FrameError::Incomplete)
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, FrameError> {
    if !src.has_remaining() {
        return Err(FrameError::Incomplete);
    }

    Ok(src.get_u8())
}

fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, FrameError> {
    let line = get_line(src)?.to_vec();
    let string = String::from_utf8(line)?;
    let int: u64 = string.parse().unwrap();
    Ok(int)
}

#[derive(Debug)]
enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

type Error = Box<dyn std::error::Error + Send + Sync>;
enum FrameError {
    Incomplete,
    Other(crate::Error),
}

impl From<String> for FrameError {
    fn from(value: String) -> Self {
        FrameError::Other(value.into())
    }
}

impl From<&str> for FrameError {
    fn from(value: &str) -> Self {
        value.to_string().into()
    }
}

impl From<FromUtf8Error> for FrameError {
    fn from(_value: FromUtf8Error) -> Self {
        "Invalid frame format".into()
    }
}

impl From<TryFromIntError> for FrameError {
    fn from(_value: TryFromIntError) -> Self {
        "Invalid frame format".into()
    }
}

impl fmt::Display for FrameError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FrameError::Incomplete => "stream ended early".fmt(fmt),
            FrameError::Other(err) => err.fmt(fmt),
        }
    }
}
