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

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{self, Receiver, Sender},
        Mutex,
    },
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

    // current node's network config
    // (host, port)
    network_config: Option<(String, u64)>,
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

    // Marker to indicate if wraped connection is a replica or not
    pub is_replica: bool,

    // channel for listening for replicable commands to write to connection
    command_receiver: Arc<Mutex<Receiver<RESP>>>,
    // channel for broadcasting replicable commands from connection
    command_sender: Sender<RESP>,
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
            network_config: None,
        }
    }

    pub fn set_network_config(&mut self, config: (String, u64)) {
        self.network_config = Some(config);
    }
    pub fn init_repl_state(&mut self) {
        self.db.db().set_role(Role::Master);
        self.db
            .db()
            .set_repl_id("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".into());
    }

    pub async fn handshake(&mut self, master: ReplicaInfo) -> crate::Result<Option<Connection>> {
        // send handshake to master
        let addr = format!("{}:{}", master.host.clone(), master.port.clone());
        let stream = TcpStream::connect(addr).await?;
        let mut connection = Connection::new(stream, true);

        // HANDSHAKE PROTOCOL
        // send PING
        let mut ping_frame = RESP::array();
        ping_frame.push_bulk(Bytes::from("PING"));
        connection.write_frame(&ping_frame).await?;

        // let _ = time::sleep(Duration::from_millis(50));

        let _ = wait_for_response(&mut connection).await?; // connection.read_resp().await?;

        // send 1st REPLCONF
        let mut repl_conf_frame = RESP::array();
        repl_conf_frame.push_bulk(Bytes::from("REPLCONF"));
        repl_conf_frame.push_bulk(Bytes::from("listening-port"));
        repl_conf_frame.push_bulk(Bytes::from(
            self.network_config.as_ref().unwrap().1.to_string(),
        ));
        connection.write_frame(&repl_conf_frame).await?;

        // let _ = time::sleep(Duration::from_millis(50));

        let _ = wait_for_response(&mut connection).await?; // connection.read_resp().await?;

        // let _ = time::sleep(Duration::from_millis(100));
        // send 2nd REPLCONF
        // REPLCONF capa eof capa psync2
        let mut repl_conf_frame_2 = RESP::array();
        repl_conf_frame_2.push_bulk(Bytes::from("REPLCONF"));
        repl_conf_frame_2.push_bulk(Bytes::from("capa"));
        repl_conf_frame_2.push_bulk(Bytes::from("eof"));
        repl_conf_frame_2.push_bulk(Bytes::from("capa"));
        repl_conf_frame_2.push_bulk(Bytes::from("psync2"));
        connection.write_frame(&repl_conf_frame_2).await?;

        // let _ = time::sleep(Duration::from_millis(50));
        let _ = wait_for_response(&mut connection).await?; // connection.read_resp().await?;

        // PSYNC with master
        let mut psync_resp = RESP::array();
        psync_resp.push_bulk(Bytes::from("PSYNC"));
        psync_resp.push_bulk(Bytes::from("?"));
        psync_resp.push_bulk(Bytes::from("-1"));
        connection.write_frame(&psync_resp).await?;

        let psync_resp = wait_for_response(&mut connection).await?;
        println!("Psync Resp {:?}", psync_resp);
        let empty_db_file_resp = wait_for_rdb_response(&mut connection).await?;
        println!("Empty DB File Resp {:?}", empty_db_file_resp);

        self.db.db().set_role(crate::Role::Slave);
        self.db.db().set_master(master);

        connection.last_active_time = Some(std::time::Instant::now());

        Ok(Some(connection))
    }

    pub async fn listen_to_master(&mut self, connection: Connection) -> crate::Result<()> {
        let (tx, rx) = mpsc::channel(1);
        let mut handler = Handler {
            connection,
            db: self.db.db(),
            is_replica: false,
            command_sender: tx,
            command_receiver: Arc::new(rx.into()),
        };

        tokio::spawn(async move {
            // pass the connection to a new handler
            // in an async thread
            println!("Listen to master");
            if let Err(err) = handler.run().await {
                println!("Master Handler error {:?}", err);
            }
        });

        Ok(())
    }

    pub async fn run(&mut self) -> crate::Result<()> {
        println!(
            "Listner is running on port {:?}",
            self.listener.local_addr()
        );

        // create a channel for listening for replicable commands
        // to be sent to replica connections
        let (cmd_tx, cmd_rcv) = mpsc::channel::<RESP>(10);

        let cmd_receiver = Arc::new(Mutex::new(cmd_rcv));
        let cmd_tx = Arc::new(cmd_tx);

        loop {
            // accpet next tcp connection from client
            let stream = self.accept().await?;

            // create channel for sending commands from non-slave connections
            // to server to broadcast to slave connections
            let (handler_tx, mut handler_rcv) = mpsc::channel::<RESP>(10);
            println!("Accept new connection {:?}", stream.peer_addr());

            let mut handler = Handler {
                connection: Connection::new(stream, false),
                db: self.db.db(),
                is_replica: false,
                command_sender: handler_tx,
                command_receiver: Arc::clone(&cmd_receiver),
            };

            let command_broadcaster = Arc::clone(&cmd_tx);

            println!("Run Handler");
            tokio::spawn(async move {
                // pass the connection to a new handler
                // in an async thread
                if let Err(err) = handler.run().await {
                    println!("Handler error {:?}", err);
                }
            });

            tokio::spawn(async move {
                println!("Listen for Broadcast");
                while let Some(resp) = handler_rcv.recv().await {
                    // todo: handle send error
                    // detect if handler is no longer up and running
                    // cut ties with handler
                    println!("Command To Replicate: {:?}", &resp);
                    let _ = command_broadcaster.send(resp).await;
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
            let _ = time::sleep(Duration::from_secs(backoff)).await;
            backoff *= 2;
        }
    }
}

#[derive(Debug, Clone)]
pub struct RESPSource {
    data: Option<RESP>,
    source: Source,
}

#[derive(Debug, Clone, PartialEq)]
enum Source {
    DIRECT,
    REPLICA,
}

/// Handler struct implementation
impl Handler {
    /// Process a single inbound connection
    ///
    /// Request RESP are parsed from the socket buffer and processed using `Command`
    /// Response is written back to the socket
    pub async fn run(&mut self) -> crate::Result<()> {
        let role = self.db.get_role();
        let is_master = role == "master";
        while !self.connection.closed {
            // parse RESP from connection
            let mut recv = self.command_receiver.lock().await;
            let recv = &mut *recv;

            // println!("Listen for RESP");
            let data = tokio::select! {
                res = self.connection.read_resp() => res?.map(|data| RESPSource { data: Some(data), source: Source::DIRECT }),
                    v = tokio::time::timeout(Duration::from_millis(50), recv.recv()) => v.map_or(None, |data| Some(RESPSource { data, source: Source::REPLICA })),
            };

            if data.is_none() {
                continue;
            }

            let data = data.unwrap();
            let source = data.source;
            let resp = data.data;
            println!("Command {:?}, Source {:?}", &resp, &source);

            if let Some(resp) = resp {
                let cmd_resp = resp.clone();

                // Map RESP to a Command
                let command = Command::from_resp(resp)?;

                if is_master {
                    // if incoming command mutates network state, propagate to all replicas
                    if command.is_replicable_command()
                        && self.is_replica
                        && source == Source::REPLICA
                    {
                        // propagate resp to replicas if Server is master
                        println!(
                            "Send Command to Replica: {:?}, is_replica: {}",
                            &cmd_resp, self.is_replica
                        );
                        let _ = self.connection.write_frame(&cmd_resp).await;
                        // continue;
                    }

                    if command.is_replicable_command() && source == Source::DIRECT {
                        // broadcast command to server channel
                        println!("Broadcast Command to Server: {:?}", &cmd_resp);
                        let _ = self.command_sender.send(cmd_resp).await;
                    }

                    if command.get_name() == "psync" {
                        // mark connection as replica
                        println!("Add Replica");
                        self.is_replica = true;
                    }
                }

                if source == Source::DIRECT {
                    // Run Command and write result RESP to stream
                    command.apply(&mut self.connection, &self.db).await?;
                }
            }
        }
        Ok(())
    }
}

pub async fn wait_for_response(connection: &mut Connection) -> crate::Result<Option<RESP>> {
    let mut resp = connection.read_resp().await?;
    while resp.is_none() {
        resp = connection.read_resp().await?;
    }
    Ok(resp)
}

pub async fn wait_for_rdb_response(connection: &mut Connection) -> crate::Result<Option<Bytes>> {
    let mut resp = connection.read_rdb().await?;
    while resp.is_none() {
        resp = connection.read_rdb().await?;
    }

    println!("Parsed RDB {:?}", resp);

    Ok(resp)
}
