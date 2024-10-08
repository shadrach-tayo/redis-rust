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

use std::{
    future::Future,
    path::Path,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc, RwLock},
    time,
};

use crate::{
    config::ServerConfig,
    connection::Connection,
    gen_rand_string,
    ping::Ping,
    rdb::{self, DefaultFilter, RdbBuilder, RdbParser},
    resp::RESP,
    CliConfig, Command, Db, DbGuard, PSync, Replconf, ReplicaInfo, Role, Shutdown,
};

#[derive(Debug)]
pub struct Listener {
    // db => database guard
    pub db: DbGuard,

    // Tcp listner
    pub listener: TcpListener,

    // current node's network config
    // (host, port)
    // network_config: Option<(String, u64)>,
    config: ServerConfig,

    // keep track of connected slave
    replicas: Arc<RwLock<Vec<Connection>>>,

    notify_shutdown: broadcast::Sender<()>,

    shutdown_complete_tx: mpsc::Sender<()>,
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

    /// Marker to indicate if wraped connection is a replica or not
    pub is_replica: bool,

    /// keep track of server config
    pub config: ServerConfig,

    /// keep track of connected slave
    pub replicas: Arc<RwLock<Vec<Connection>>>,

    /// Indicate client is executing a transaction
    /// True if the last command is MULTI
    pub is_multi: bool,

    /// queued commands to be executed as part of a transaction
    pub transaction: Vec<RESP>,

    // shutdown listener
    shutdown: Shutdown,

    // handle to shutdown_complete_tx
    _shutdown_complete_tx: mpsc::Sender<()>,
}

/// Run the redis server
///
/// Accepts a new connection from the TcpListener in the `Listener`
/// for every accept tcp socket, a new async task is spawned to handle
/// the connection.
pub async fn run(
    listener: TcpListener,
    config: CliConfig,
    shutdown: impl Future,
) -> crate::Result<()> {
    let (notify_shutdown, _) = broadcast::channel::<()>(1);
    let (shutdown_cmpl_tx, mut shutdown_cmpl_rx) = mpsc::channel::<()>(1);

    let mut master_repl_id = None;
    let role = if config.is_replication {
        Role::Slave
    } else {
        master_repl_id = Some(gen_rand_string(40));
        Role::Master
    };

    let server_config = ServerConfig {
        role,
        master_repl_id,
        dir: config.dir.clone(),
        dbfilename: config.dir.clone(),
        network_config: Some(("".into(), config.port)),
        master_repl_offset: Arc::new(AtomicU64::new(0)),
    };

    let rdb = if config.dir.is_some() && config.dbfilename.is_some() {
        let path =
            Path::new(config.dir.unwrap().as_str()).join(config.dbfilename.unwrap().as_str());
        match rdb::read_db_file(path) {
            Ok(rdb) => Some(rdb),
            Err(err) => {
                println!("Error reading rdb file {:?}", err);
                None
            }
        }
    } else {
        None
    };

    let derived_database = if let Some(rdb) = rdb {
        let mut parser = RdbParser::new(DefaultFilter::new(), RdbBuilder::default(), rdb);
        parser.parse()?
    } else {
        None
    };

    let db = match derived_database {
        Some(database) => DbGuard::from_derived(database),
        None => DbGuard::new(),
    };

    let mut server = Listener {
        listener,
        db,
        config: server_config,
        replicas: Arc::new(RwLock::new(vec![])),
        shutdown_complete_tx: shutdown_cmpl_tx,
        notify_shutdown,
    };

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
                // error!("");
            }
        },
        _ = shutdown => {
            println!("Shutdown redis server");
        }
    }

    let Listener {
        notify_shutdown,
        shutdown_complete_tx,
        ..
    } = server;
    drop(notify_shutdown);

    drop(shutdown_complete_tx);

    let _ = shutdown_cmpl_rx.recv().await;

    Ok(())
}

/// Listner struct implementations
impl Listener {
    pub fn init_repl_state(&mut self) {
        let repl_id = gen_rand_string(40);
        self.db.db().set_repl_id(repl_id);
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
            self.config.network_config.as_ref().unwrap().1.to_string(),
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

        Ok(Some(connection))
    }

    pub async fn listen_to_master(&mut self, connection: Connection) -> crate::Result<()> {
        let mut handler = Handler {
            connection,
            db: self.db.db(),
            is_replica: false,
            replicas: self.replicas.clone(),
            config: self.config.clone(),
            is_multi: false,
            transaction: vec![],
            shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
            _shutdown_complete_tx: self.shutdown_complete_tx.clone(),
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
        // to be sent to slave connections
        let (sender, _rx) = broadcast::channel::<RESP>(16);

        // let cmd_receiver = Arc::new(Mutex::new(cmd_rcv));
        let sender = Arc::new(sender);

        loop {
            // accpet next tcp connection from client
            let stream = self.accept().await?;

            println!("Accept new connection {:?}", stream.peer_addr());

            let handler = Handler {
                connection: Connection::new(stream, false),
                db: self.db.db(),
                is_replica: false,
                config: self.config.clone(),
                replicas: self.replicas.clone(),
                is_multi: false,
                transaction: vec![],
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete_tx: self.shutdown_complete_tx.clone(),
            };

            let sender = Arc::clone(&sender);
            tokio::spawn(async move {
                // pass the connection to a new handler
                // in an async thread
                if let Err(err) = handler.run(sender).await {
                    println!("Handler error {:?}", err,);
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
    pub async fn run(mut self, _sender: Arc<broadcast::Sender<RESP>>) -> crate::Result<()> {
        while !self.shutdown.is_shutdown() && !self.connection.closed {
            let resp = tokio::select! {
                res = self.connection.read_resp() => res?,
                _ = self.shutdown.recv() => return Ok(())
            };

            let (resp, size) = match resp {
                Some(resp_and_size) => resp_and_size,
                None => continue,
            };

            if self.is_multi {
                // Map RESP to a Command
                let command = Command::from_resp(resp.clone())?;

                match command {
                    Command::Exec(_) => {
                        let mut responses = RESP::array();

                        for queued in self.transaction.iter() {
                            let command = Command::from_resp(queued.clone())?;
                            let response = command
                                .apply(
                                    &mut self.connection,
                                    &self.db,
                                    None,
                                    self.replicas.clone(),
                                    self.config.clone(),
                                )
                                .await?;
                            if let Some(resp) = response {
                                responses.push(resp);
                            }
                        }

                        self.connection.write_frame(&responses).await?;

                        self.is_multi = false;
                        self.transaction.truncate(0);
                    }
                    Command::Discard(_) => {
                        self.is_multi = false;
                        self.transaction.truncate(0);
                        self.connection
                            .write_frame(&RESP::Simple("OK".to_string()))
                            .await?;
                    }
                    _ => {
                        println!("Queue commands");
                        self.transaction.push(resp);
                        self.connection
                            .write_frame(&RESP::Simple("QUEUED".to_string()))
                            .await?;
                    }
                }
            } else {
                // Map RESP to a Command
                let command = Command::from_resp(resp.clone())?;

                match self.config.role {
                    Role::Master => match command {
                        Command::Set(_) => {
                            let replicas = &mut *self.replicas.write().await;
                            let mut remove = vec![];

                            for (idx, connection) in replicas.into_iter().enumerate() {
                                let repl_result = connection.write_frame(&resp).await;
                                println!(
                                    "Replicate: {}, offset: {:?}, Result: {:?}",
                                    idx + 1,
                                    connection.repl_offset.load(Ordering::SeqCst),
                                    repl_result
                                );

                                if repl_result.is_err() {
                                    remove.push(idx);
                                }
                            }

                            for idx in remove.iter() {
                                replicas.swap_remove(*idx);
                                println!("Remove Replica: {idx}");
                            }
                        }
                        Command::PSync(_) => {
                            command
                                .apply(
                                    &mut self.connection,
                                    &self.db,
                                    None,
                                    self.replicas.clone(),
                                    self.config.clone(),
                                )
                                .await?;

                            self.connection.repl_offset.store(0, Ordering::SeqCst);
                            self.replicas.write().await.push(self.connection);
                            return Ok(());
                        }
                        Command::Multi(_) => {
                            self.is_multi = true;
                        }
                        Command::Exec(_) => {
                            self.connection
                                .write_frame(&RESP::Error("ERR EXEC without MULTI".to_string()))
                                .await?;
                        }
                        Command::Discard(_) => {
                            self.connection
                                .write_frame(&RESP::Error("ERR DISCARD without MULTI".to_string()))
                                .await?;
                        }
                        _ => {}
                    },
                    Role::Slave => {}
                }

                if command.affects_offset() {
                    self.config
                        .master_repl_offset
                        .fetch_add(size as u64, Ordering::SeqCst);
                    for connection in &mut *self.replicas.write().await {
                        connection
                            .repl_offset
                            .fetch_add(size as u64, Ordering::SeqCst);
                    }
                }

                let resp = command
                    .apply(
                        &mut self.connection,
                        &self.db,
                        None,
                        self.replicas.clone(),
                        self.config.clone(),
                    )
                    .await?;

                if let Some(resp) = resp {
                    if !self.connection.is_master {
                        self.connection.write_frame(&resp).await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Process a single inbound connection from master node
    ///
    /// Does the same as the run method above but
    pub async fn run_master(&mut self) -> crate::Result<()> {
        let offset = AtomicUsize::new(0);

        while !self.shutdown.is_shutdown() {
            let resp = tokio::select! {
                res = self.connection.read_resp() => res?,
                _ = self.shutdown.recv() => return Ok(())
            };

            let (resp, size) = match resp {
                Some(resp_and_size) => resp_and_size,
                None => continue,
            };

            // Map RESP to a Command
            let command = Command::from_resp(resp)?;

            command
                .apply(
                    &mut self.connection,
                    &self.db,
                    Some(&offset),
                    self.replicas.clone(),
                    self.config.clone(),
                )
                .await?;

            let _ = offset.fetch_add(size, Ordering::SeqCst);
        }
        Ok(())
    }
}
