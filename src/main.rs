// Uncomment this block to pass the first stage

use redis_starter_rust::{server::Listener, DbGuard, Error};
use tokio::net::TcpListener;

// todo: implement command
// storage
// connections
// frames

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut args = std::env::args();

    // dispose file path
    let _ = args.next();

    const MSG: &str = "Pass --port [port] argument to server";

    let port: u64 = match args.next() {
        Some(s) if s == "--port".to_string() => match args.next().unwrap().parse() {
            Ok(int) => int,
            Err(_) => panic!("Could not parse "),
        },
        Some(s) => {
            println!("arg {}", s);
            panic!("Invalid arg: {} passed to server, {}", s, MSG)
        }
        None => 6379,
    };

    let addr = format!("127.0.0.1:{}", port);

    let listener = TcpListener::bind(addr).await?;

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
