// Uncomment this block to pass the first stage

use redis_starter_rust::{server::Listener, DbGuard, Error};
use tokio::net::TcpListener;

// todo: implement command
// storage
// connections
// frames

#[tokio::main]
async fn main() -> Result<(), Error> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    // let mut handles = vec![];
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    let db = DbGuard::new();
    let mut server = Listener { listener, db };

    tokio::select! {
        result = server.run() => {
            if let Err(err) = result {
            // println!("Server error {:?}", err);
                Err(err)
            } else {
                Ok(())
            }
        }
    }
}

// fn handle_connections(mut stream: TcpStream) {
//     let mut reader = BufReader::new(stream.try_clone().unwrap());
//     let mut stream_buf = BufWriter::new(&mut stream);
//     // let mut temp_buf = Vec::with_capacity(4 * 1024);
//     let mut backoff = 1;
//     // let mut string = String::from("");

//     let mut output_frame: RESP = RESP::Null;
//     let mut buf = [0; 4 * 1024];
//     loop {
//         let mut cursor = Cursor::new(&buf[..]);
//         let frame_result = RESP::parse_frame(&mut cursor);
//         match frame_result {
//             Ok(frame) => {
//                 output_frame = frame;
//                 println!("Parsed frame: {:?}", &output_frame);
//                 break;
//             }
//             Err(err) => {
//                 println!("Error parsing frames {}", err);
//             }
//         }

//         match reader.read(&mut buf) {
//             Ok(size) => {
//                 println!("Read size {}", size);
//                 // println!("stream {:?}", string);

//                 let buffer_len = reader.buffer().chunk().len();
//                 println!(
//                     "Buffer len: {:?}, loaded length {:?}",
//                     reader.buffer().chunk().len(),
//                     String::from_utf8(buf[..size].to_vec()).unwrap()
//                 );
//                 if buffer_len == 0 {
//                     // let _ = &mut temp_buf
//                     //     .write(string.bytes().collect::<Vec<u8>>().as_slice())
//                     //     .unwrap();
//                     // println!("Write size {write_size}");

//                     // println!("Read stream bufferred {:?}", temp_buf.len());
//                     println!("EOF");
//                     // break;
//                 }
//             }
//             Err(err) => {
//                 println!("Error reading stream buffer {:?}", err);
//             }
//         }

//         if backoff == 100 {
//             break;
//         }
//         backoff += 1;
//     }

//     let _ = frame_to_string(&output_frame, &mut stream_buf);
//     // stream_buf.write(b"*2\r\n+PONG\r\n+PONG\r\n").unwrap();
//     println!(
//         "Respone {:?}",
//         String::from_utf8(stream_buf.buffer().to_vec()).unwrap()
//     );
//     stream_buf.flush().unwrap();
// }
