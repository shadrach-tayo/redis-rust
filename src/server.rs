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

use tokio::{
    net::{TcpListener, TcpStream},
    sync::broadcast,
    time,
};

use crate::{
    connection::Connection, ping::Ping, resp::RESP, Command, Db, DbGuard, PSync, Replconf,
    ReplicaInfo, Role,
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
        connection.write_frame(&Ping::new(None).into()).await?;

        let _ = connection.read_resp().await?;

        // send 1st REPLCONF
        let listening_conf = Replconf::new(vec![
            "listening-port".into(),
            self.network_config.as_ref().unwrap().1.to_string(),
        ]);
        connection.write_frame(&listening_conf.into()).await?;

        connection.read_resp().await?;

        // send 2nd REPLCONF
        let replconf_capa = Replconf::new(vec!["capa".into(), "eof".into(), "psync2".into()]);
        connection.write_frame(&replconf_capa.into()).await?;

        let _ = connection.read_resp().await?;

        // PSYNC with master
        connection
            .write_frame(&PSync::new("?".into(), "-1".into()).into())
            .await?;

        let resp = connection.read_resp().await?;
        println!("FULLSYNC 1: {:?}", resp);

        let _resp = connection.read_resp().await?;
        println!("Rdb file: {:?}", _resp);

        self.db.db().set_role(crate::Role::Slave);
        self.db.db().set_master(master);

        println!("Handshake complete!!!");
        Ok(Some(connection))
    }

    pub async fn listen_to_master(&mut self, connection: Connection) -> crate::Result<()> {
        println!("Listen to master");

        let mut handler = Handler {
            connection,
            db: self.db.db(),
            is_replica: false,
            // command_sender: tx,
            // command_receiver: Arc::new(rx.into()),
        };

        let (tx, _) = broadcast::channel::<RESP>(1);
        let tx = Arc::new(tx);
        let tx = Arc::clone(&tx);

        tokio::spawn(async move {
            // pass the connection to a new handler
            // in an async thread
            println!("Listen to master");
            if let Err(err) = handler.run(tx).await {
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

            // create channel for sending commands from non-slave connections
            // to server to broadcast to slave connections
            // let (handler_tx, mut handler_rcv) = mpsc::channel::<RESP>(10);

            println!("Accept new connection {:?}", stream.peer_addr());

            let mut handler = Handler {
                connection: Connection::new(stream, false),
                db: self.db.db(),
                is_replica: false,
                // command_sender: handler_tx,
                // command_receiver: Arc::clone(&cmd_receiver),
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

            // let command_broadcaster = Arc::clone(&cmd_tx);

            // tokio::spawn(async move {
            //     while let Some(resp) = handler_rcv.recv().await {
            //         // todo: handle send error
            //         // detect if handler is no longer up and running
            //         // cut ties with handler
            //         println!("Command To Replicate: {:?}", &resp);
            //         let send_result = command_broadcaster.send(resp);
            //         println!("Sent command to replicate")
            //     }
            // });
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

            if let Some(resp) = resp {
                let cmd_resp = resp.clone();
                println!("Data: {:?}", &resp);

                // Map RESP to a Command
                let command = Command::from_resp(resp)?;

                if is_master {
                    if command.is_replicable_command() {
                        // broadcast command to server channel
                        let sx = sender.send(cmd_resp).unwrap();
                        println!("Command sent {:?}", sx);
                    }

                    let command_name = &command.get_name();

                    let subscribe = command_name == "psync";

                    command.apply(&mut self.connection, &self.db).await?;

                    if subscribe {
                        // respond to psync command before polling for updates

                        println!("Subscribe to Replication sync");
                        // mark connection as replica
                        self.is_replica = true;

                        let mut subscriber = sender.subscribe();
                        while let Ok(cmd) = subscriber.recv().await {
                            println!("Command to replicate {:?}", &cmd);
                            let _ = self.connection.write_frame(&cmd).await;
                        }
                    }
                } else {
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
