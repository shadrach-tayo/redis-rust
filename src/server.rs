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

use bytes::Bytes;
use tokio::{
    net::{TcpListener, TcpStream},
    time,
};

use crate::{connection::Connection, frame::RESP, Command, Db, DbGuard, ReplicaInfo, Role};

#[derive(Debug)]
pub struct Listener {
    // db => database guard
    pub db: DbGuard,

    // Tcp listner
    pub listener: TcpListener,
    pub master_connection: Option<Connection>,
}

pub struct Handler {
    // database reference
    db: Db,
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
    pub fn new(listener: TcpListener, db: DbGuard) -> Self {
        Self {
            listener,
            db,
            master_connection: None,
        }
    }

    pub fn init_repl_state(&mut self) {
        self.db.db().set_role(Role::Master);
        self.db
            .db()
            .set_repl_id("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".into());
    }

    pub async fn set_master(&mut self, master: ReplicaInfo) -> crate::Result<()> {
        // send handshake to master
        let addr = format!("{}:{}", master.host.clone(), master.port.clone());
        let stream = TcpStream::connect(addr).await?;
        let mut connection = Connection::new(stream);

        // do handshake
        let mut ping_frame = RESP::array();
        ping_frame.push_bulk(Bytes::from("PING"));
        connection.write_frame(&ping_frame).await?;

        self.db.db().set_role(crate::Role::Slave);
        self.db.db().set_master(master);

        Ok(())
    }

    pub async fn run(&mut self) -> crate::Result<()> {
        println!(
            "Listner is running on port {:?}",
            self.listener.local_addr()
        );
        loop {
            // accpet next tcp connection from client
            let stream = self.accept().await?;

            // let stream = Arc::new(stream);
            // let stream = Arc::clone(&stream);
            let mut handler = Handler {
                connection: Connection::new(stream),
                db: self.db.db(),
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
        while !self.connection.closed {
            // parse RESP from connection
            let resp = self.connection.read_resp().await?;

            if let Some(resp) = resp {
                // update connection last active time
                self.connection.last_active_time = Some(std::time::Instant::now());

                // Map RESP to a Command
                let command = Command::from_resp(resp)?;

                // Run Command and write result RESP to stream
                command.apply(&mut self.connection, &self.db).await?;
            } else {
                // close connection
                break;
            }

            // check if connection idle time is passed and close connection
            let elasped = self.connection.last_active_time.unwrap();
            let elasped = elasped.elapsed();
            if elasped > self.connection.idle_close {
                println!("Idle time elasped {:?}", elasped);
                // break;
                self.connection.closed = true;
            }
        }
        Ok(())
    }
}
