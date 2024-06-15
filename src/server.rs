// impl async run function that listens to tcp connections
// and shutdown signals to terminate server actions
//
//
// impl a server Listener that accepts a tcp socket, db_holder
// running the listener accepts connections and creates a Listener
// that handles the connection's lifetime
//
// impl and handler that handles the lifetime of an accepted tcp sockets
// handler process the connection, reads and parse RESP data,
// creates a command
// applies the command on the tcp connection
//

// use std::net::TcpListener;

use std::time::Duration;

use tokio::{
    net::{TcpListener, TcpStream},
    time,
};

use crate::{connection::Connection, Command};

#[derive(Debug)]
pub struct Listener {
    // db => database guard

    // Tcp listner
    pub listener: TcpListener,
}

pub struct Handler {
    // database reference
    // db: Db
    /// The TCP connection instrumented with RESP encoder and decoder
    /// uses a buffered `TcpStream`
    ///
    /// When a new tcp connection is accepted, it is passed to a `Connection::new`
    /// it allows us to interact with the protocol RESP data and encapsulate it's
    /// encoding and decoding
    pub connection: Connection,
}

/// Run the redis server
///
/// Accepts a new connection from the TcpListener in the `Listener`
/// for every accept tcp socket, a new async task is spawned to handle
/// the connection.
#[allow(unused)]
pub async fn run(listener: TcpListener) -> crate::Result<()> {
    todo!()
}

/// Listner struct implementations
impl Listener {
    pub async fn run(&mut self) -> crate::Result<()> {
        println!("Listner is running");
        loop {
            // accpet next tcp connection from client
            let stream = self.accept().await?;
            println!("Connection accepted");
            let mut handler = Handler {
                connection: Connection::new(stream),
            };

            tokio::spawn(async move {
                // pass the connection to a new handler
                // in an async thread
                if let Err(err) = handler.run().await {
                    println!("Handler error {:?}", err);
                }
            });
        }
    }

    /// accept new tcp connection from the tcp listener
    /// retry with backoff strategy for up to six times with
    /// delay
    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;
        loop {
            match self.listener.accept().await {
                Ok((stream, _)) => return Ok(stream),
                Err(err) => {
                    println!("Error accepting new connection: {}", err);
                    if backoff > 32 {
                        return Err(err.into());
                    }
                }
            }
            time::sleep(Duration::from_secs(backoff)).await;
            backoff *= 2;
        }
    }
}

/// Handler struct implementation
impl Handler {
    /// Process a single inbound connection
    ///
    /// Request RESP are parsed from the socket buffer and processed using `Command`
    /// Response is written back to the socket
    pub async fn run(&mut self) -> crate::Result<()> {
        println!("Connecton Handler is running");
        // parse RESP from connection
        let resp = self.connection.read_resp().await?;
        println!("Input RESP parsed {:?}", &resp);
        if let Some(resp) = resp {
            // Map RESP to a Command
            let command = Command::from_resp(resp)?;
            // Run Command and write result RESP to stream
            command.apply(&mut self.connection).await?;
        }
        Ok(())
    }
}
