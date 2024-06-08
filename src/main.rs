// Uncomment this block to pass the first stage
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming().take(2) {
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
    let buf_reader = BufReader::new(&mut stream);
    let command = buf_reader.lines().next().unwrap().unwrap();
    println!("Command {}", command);
    stream.write_all("+PONG\r\n".as_bytes()).unwrap()
}
