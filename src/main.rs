// Uncomment this block to pass the first stage
use std::{
    fmt,
    io::{self, BufRead, BufReader, BufWriter, Cursor, Write},
    net::{TcpListener, TcpStream},
    num::TryFromIntError,
    string::FromUtf8Error,
};

use bytes::{Buf, Bytes};

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
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut stream_buf = BufWriter::new(&mut stream);
    let mut temp_buf = Vec::with_capacity(4 * 1024);
    let mut backoff = 1;
    let mut string = String::from("");

    let mut output_frame: Frame = Frame::Null;
    loop {
        match reader.read_line(&mut string) {
            Ok(_size) => {
                // println!("Read size {}", size);
                println!("stream {:?}", string);

                let buffer_len = reader.buffer().chunk().len();
                // println!("Buffer len: {:?}", reader.buffer().chunk().len());
                if buffer_len == 0 {
                    let _ = &mut temp_buf
                        .write(string.bytes().collect::<Vec<u8>>().as_slice())
                        .unwrap();
                    // println!("Write size {write_size}");

                    println!("Read stream bufferred {:?}", temp_buf.len());
                    let mut cursor = Cursor::new(&temp_buf[..]);
                    let frame_result = parse_frame(&mut cursor);
                    match frame_result {
                        Ok(frame) => {
                            output_frame = frame;
                            println!("Parsed frame: {:?}", &output_frame);
                            break;
                        }
                        Err(err) => {
                            println!("Error parsing frames {}", err);
                        }
                    }
                    break;
                }
            }
            Err(err) => {
                println!("Error reading stream buffer {:?}", err);
            }
        }

        if backoff == 10 {
            break;
        }
        backoff += 1;
    }

    let _ = frame_to_string(&output_frame, &mut stream_buf);
    // stream_buf.write(b"*2\r\n+PONG\r\n+PONG\r\n").unwrap();
    println!(
        "Respone {:?}",
        String::from_utf8(stream_buf.buffer().to_vec()).unwrap()
    );
    stream_buf.flush().unwrap();
}

fn frame_to_string<'a>(
    frame: &Frame,
    dst: &'a mut BufWriter<&mut TcpStream>,
) -> Result<(), std::io::Error> {
    match frame {
        Frame::Null => {
            dst.write(b"$-1\r\n")?;
        }
        Frame::Error(error) => {
            dst.write(b"-")?;
            dst.write(error.as_bytes())?;
            dst.write(b"\r\n")?;
        }
        Frame::Simple(string) => {
            dst.write(b"+")?;
            dst.write(string.as_bytes())?;
            dst.write(b"\r\n")?;
        }
        Frame::Bulk(data) => {
            dst.write(b"$")?;
            let len = data.len() as u64;
            write_decimal(dst, len)?;
            if String::from_utf8(data.to_vec()).unwrap() == "ping".to_string() {
                dst.write("pong".as_bytes())?;
            } else {
                dst.write(data)?;
            }
            dst.write(b"\r\n")?;
        }
        Frame::Array(list) => {
            dst.write(b"*")?;
            write_decimal(dst, list.len() as u64)?;

            for frame in list {
                frame_to_string(&frame, dst)?;
            }
        }
        Frame::Integer(int) => {
            dst.write(b":")?;
            write_decimal(dst, *int)?;
        }
    }

    Ok(())
}

#[allow(unused)]
fn parse_frame(cursor: &mut Cursor<&[u8]>) -> Result<Frame, FrameError> {
    // println!("Cursor remaining: {}", cursor.has_remaining());
    match get_u8(cursor)? {
        b'+' => {
            // strings frame
            let line = get_line(cursor)?.to_vec();
            let string = String::from_utf8(line)?;
            // println!("parse string {}", &string);
            Ok(Frame::Simple(string))
        }
        b'*' => {
            // arrays frame
            let len = get_decimal(cursor)?.try_into()?;
            // println!("parse decimal {}", len);
            let mut out = Vec::with_capacity(len as usize);
            for _ in 0..len {
                out.push(parse_frame(cursor)?);
            }
            Ok(Frame::Array(out))
        }
        b'$' => {
            // bulk strings frame
            if b'-' == peak_u8(cursor)? {
                let line = get_line(cursor)?;
                if line != b"-1" {
                    return Err("Invalid input format.".into());
                }
                Ok(Frame::Null)
            } else {
                let len = get_decimal(cursor)?.try_into()?;
                let n = len + 2;
                if cursor.remaining() < n {
                    return Err(FrameError::Incomplete);
                }
                let data = Bytes::copy_from_slice(&cursor.chunk()[..len]);

                // skip that number of bytes + 2
                skip(cursor, n)?;

                Ok(Frame::Bulk(data))
            }
        }
        b':' => {
            // integers frame
            let int = get_decimal(cursor)?;
            Ok(Frame::Integer(int))
        }
        b'-' => {
            // simple errors frame
            let error = get_line(cursor)?.to_vec();
            let string = String::from_utf8(error)?;
            Ok(Frame::Error(string))
        }
        b'_' => {
            // null frame
            todo!()
        }
        _ => unimplemented!(),
    }
}

#[allow(unused)]
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
    // println!("parse line");
    let start = src.position() as usize;
    let end = src.get_ref().len() - 1;

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            src.set_position((i + 2) as u64);
            // println!("parsed line {:?}", &src.get_ref()[start..i]);
            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(FrameError::Incomplete)
}

fn peak_u8(src: &mut Cursor<&[u8]>) -> Result<u8, FrameError> {
    if !src.has_remaining() {
        return Err(FrameError::Incomplete);
    }
    let peak = src.chunk()[0];
    Ok(peak)
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

fn write_decimal(dst: &mut BufWriter<&mut TcpStream>, val: u64) -> io::Result<()> {
    use std::io::Write;
    let mut buf = [0u8, 20];
    let mut buf = Cursor::new(&mut buf[..]);

    write!(&mut buf, "{}", val).unwrap();

    let pos = buf.position() as usize;
    dst.write(&buf.get_ref()[..pos])?;
    dst.write(b"\r\n")?;

    Ok(())
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), FrameError> {
    src.advance(n);
    Ok(())
}

#[allow(unused)]
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
