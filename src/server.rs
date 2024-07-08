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

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use bytes::Bytes;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::broadcast,
    time,
};

use crate::{
    connection::Connection, ping::Ping, resp::RESP, CliConfig, Command, Db, DbGuard, PSync,
    Replconf, ReplicaInfo, Role,
};

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
}

/// Run the redis server
///
/// Accepts a new connection from the TcpListener in the `Listener`
/// for every accept tcp socket, a new async task is spawned to handle
/// the connection.
#[allow(unused)]
pub async fn run(listener: TcpListener, config: CliConfig) -> crate::Result<()> {
    let db = DbGuard::new();
    let mut server = Listener::new(listener, db);

    // set  network config
    server.set_network_config(("".into(), config.port));

    if let Some(master) = config.master {
        let connection = server.handshake(master).await?;
        let _ = server.listen_to_master(connection.unwrap()).await?;
    } else {
        server.init_repl_state();
    }

    tokio::select! {
        result = server.run() => {
            if let Err(err) = result {
                println!("Server error {:?}", err);
                Err(err)
            } else {
                Ok(())
            }
        }
    }
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

    /// Initiate a handshake protocol between this replica node
    /// and the master node
    pub async fn handshake(&mut self, master: ReplicaInfo) -> crate::Result<Option<Connection>> {
        // Connect master node's port
        let addr = format!("{}:{}", master.host.clone(), master.port.clone());
        let stream = TcpStream::connect(addr).await?;
        let mut connection = Connection::new(stream, true);

        // HANDSHAKE PROTOCOL
        // send PING
        connection.write_frame(&Ping::new(None).into()).await?;
        let _ = connection.read_resp().await?;

        let listening_conf = Replconf::new(vec![
            "listening-port".into(),
            self.network_config.as_ref().unwrap().1.to_string(),
        ]);
        connection.write_frame(&listening_conf.into()).await?;
        connection.read_resp().await?;

        let replconf_capa = Replconf::new(vec!["capa".into(), "eof".into(), "psync2".into()]);
        connection.write_frame(&replconf_capa.into()).await?;
        let _ = connection.read_resp().await?;

        connection
            .write_frame(&PSync::new("?".into(), "-1".into()).into())
            .await?;
        let _psync_resp = connection.read_resp().await?;

        let _empty_rdb_resp = connection.read_resp().await?;

        self.db.db().set_role(crate::Role::Slave);
        self.db.db().set_master(master);

        Ok(Some(connection))
    }

    pub async fn listen_to_master(&mut self, connection: Connection) -> crate::Result<()> {
        println!("Listen to master");

        let mut handler = Handler {
            connection,
            db: self.db.db(),
            is_replica: false,
        };

        tokio::spawn(async move {
            // pass the connection to a new handler
            // in an async thread
            println!("Listen to master");
            if let Err(err) = handler.run_master().await {
                println!("Master Handler error {:?}", err);
            }
        });

        Ok(())
    }

    pub async fn run(&mut self) -> crate::Result<()> {
        println!("Listening on: {:?}", self.listener.local_addr());

        // create a channel for listening for replicable commands
        // to be sent to replica connections
        let (sender, _rx) = broadcast::channel::<RESP>(16);

        // let cmd_receiver = Arc::new(Mutex::new(cmd_rcv));
        let sender = Arc::new(sender);

        loop {
            // accpet next tcp connection from client
            let stream = self.accept().await?;

            println!("Accept new connection {:?}", stream.peer_addr());

            let mut handler = Handler {
                connection: Connection::new(stream, false),
                db: self.db.db(),
                is_replica: false,
            };

            let sender = Arc::clone(&sender);
            tokio::spawn(async move {
                // pass the connection to a new handler
                // in an async thread
                if let Err(err) = handler.run(sender).await {
                    println!(
                        "Handler error {:?}, Is Replica: {}",
                        err, handler.is_replica
                    );
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

/// Handler struct implementation
impl Handler {
    /// Process a single inbound connection
    ///
    /// Request RESP are parsed from the socket buffer and processed using `Command`
    /// Response is written back to the socket
    pub async fn run(&mut self, sender: Arc<broadcast::Sender<RESP>>) -> crate::Result<()> {
        let role = self.db.get_role();
        let is_master = role == "master";
        while !self.connection.closed {
            let resp = self.connection.read_resp().await?;

            if let Some((resp, _)) = resp {
                let cmd_resp = resp.clone();
                println!("Data: {:?}", &resp);

                // Map RESP to a Command
                let command = Command::from_resp(resp)?;

                if is_master {
                    if command.is_replicable_command() {
                        // broadcast command to server channel
                        let _ = sender.send(cmd_resp).unwrap();
                    }

                    let command_name = &command.get_name();

                    let subscribe = command_name == "psync";

                    command.apply(&mut self.connection, &self.db, None).await?;

                    if subscribe {
                        // respond to psync command before polling for updates

                        // mark connection as replica
                        self.is_replica = true;

                        let mut subscriber = sender.subscribe();
                        while let Ok(cmd) = subscriber.recv().await {
                            let _ = self.connection.write_frame(&cmd).await;

                            // send ack request
                            // let mut resp = RESP::array();
                            // resp.push_bulk(Bytes::from("REPLCONF GETACK *"));

                            // let _ = self
                            //     .connection
                            //     .write_frame(&RESP::Array(vec![
                            //         RESP::Bulk(Bytes::from("REPLCONF".as_bytes())),
                            //         RESP::Bulk(Bytes::from("GETACK".as_bytes())),
                            //         RESP::Bulk(Bytes::from("*".as_bytes())),
                            //     ]))
                            //     .await;
                            // println!("ACK cmd sent")
                        }
                    }
                } else {
                    command.apply(&mut self.connection, &self.db, None).await?;
                }
            }
        }
        Ok(())
    }

    /// Process a single inbound connection from master node
    ///
    /// Does the same as the run method above but
    pub async fn run_master(&mut self) -> crate::Result<()> {
        // let role = self.db.get_role();
        let offset = AtomicUsize::new(0);

        while !self.connection.closed {
            let resp = self.connection.read_resp().await?;

            let (resp, size) = match resp {
                Some(resp_and_size) => resp_and_size,
                None => continue,
            };

            println!("Replica::DATA: {:?}", &resp);

            // Map RESP to a Command
            let command = Command::from_resp(resp)?;

            command
                .apply(&mut self.connection, &self.db, Some(&offset))
                .await?;

            let _ = offset.fetch_add(size, Ordering::SeqCst);
        }
        Ok(())
    }
}

// pub async fn wait_for_response(connection: &mut Connection) -> crate::Result<Option<RESP>> {
//     let mut resp = connection.read_resp().await?;
//     while resp.is_none() {
//         resp = connection.read_resp().await?;
//     }
//     Ok(resp)
// }

// pub async fn wait_for_rdb_response(connection: &mut Connection) -> crate::Result<Option<Bytes>> {
//     let mut resp = connection.read_rdb().await?;
//     println!("Wait for RDB RESP......{:?}", &resp);
//     while resp.is_none() {
//         resp = connection.read_rdb().await?;
//         time::sleep(Duration::from_millis(10)).await;
//     }

//     println!("RDB RESP {:?} ", &resp);
//     Ok(resp)
// }
