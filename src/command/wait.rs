use std::{
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use bytes::Bytes;
use tokio::{sync::RwLock, time};

use crate::{
    config::ServerConfig, connection::Connection, resp::RESP, Command, RespReader, RespReaderError,
};

#[derive(Debug, Default)]
pub struct Wait {
    pub no_of_replicas: u64,
    pub timeout: u64,
}

impl Wait {
    pub fn new(replicas: u64, timeout: u64) -> Self {
        Wait {
            no_of_replicas: replicas,
            timeout,
        }
    }
    /// Returns command name
    pub fn get_name(&self) -> &str {
        "Wait"
    }

    /// Construct new Wait command by consuming the RespReader
    ///
    /// Parse next_string()? to get the config key
    /// Parse next_string()? to get the config value
    ///
    pub fn from_parts(reader: &mut RespReader) -> Result<Self, RespReaderError> {
        let no_of_replicas = reader.next_int()?;
        let timeout = reader.next_int()?;

        Ok(Wait {
            no_of_replicas,
            timeout,
        })
    }

    /// Apply the echo command and write to the Tcp connection stream
    pub async fn apply(
        self,
        dst: &mut Connection,
        _offset: Option<&AtomicUsize>,
        replicas: Arc<RwLock<Vec<Connection>>>,
        config: ServerConfig,
    ) -> crate::Result<Option<RESP>> {
        // let total_bytes = dst.master_repl_offset.load(Ordering::SeqCst);

        let no_of_replicas = replicas.read().await.len() as u64;
        let target_replicas = if self.no_of_replicas > no_of_replicas {
            no_of_replicas
        } else {
            self.no_of_replicas
        };

        let synced_replicas = Arc::new(AtomicU64::new(0));
        let synced_replicas_count = synced_replicas.clone();

        // wait timeout
        let timeout = tokio::spawn(time::sleep(Duration::from_millis(self.timeout)));

        // let master_repl_offset = config.master_repl_offset.clone().load(Ordering::SeqCst);
        let offset = config.master_repl_offset.load(Ordering::SeqCst);

        let check_wait_task = tokio::spawn(async move {
            // Skip wait logic if no repl commands have been sent
            if config.master_repl_offset.load(Ordering::SeqCst) == 0 {
                // if no commands, set no of synced replicas to number of connected replicas
                synced_replicas_count.store(no_of_replicas, Ordering::SeqCst);
                return;
            }

            // maintain a list of the indexes of synced replicas
            let mut skips = vec![];
            let replica_connections = &mut *replicas.write().await;

            // Send REPL CONF GETACK to all replicas
            for (idx, connection) in replica_connections.into_iter().enumerate() {
                // send a GETACK command
                let send_ack_resp = tokio::time::timeout(
                    Duration::from_millis(5),
                    connection.write_frame(&RESP::Array(vec![
                        RESP::Bulk(Bytes::from("REPLCONF".as_bytes())),
                        RESP::Bulk(Bytes::from("GETACK".as_bytes())),
                        RESP::Bulk(Bytes::from("*".as_bytes())),
                    ])),
                )
                .await;

                let response = match send_ack_resp {
                    Ok(r) => r,
                    Err(_) => {
                        println!("Timout when sending ACK to Replica: {idx}");
                        continue;
                    }
                };

                if response.is_err() {
                    println!("Failed to send ACK to Replica: {idx}");
                    continue;
                }

                // add hardcoded len of REPLCONF GETACK * (37)
                connection.repl_offset.fetch_add(37, Ordering::SeqCst);
            }

            // loop through all replicas
            loop {
                // Break out of loop if sync target is reached
                if synced_replicas_count.load(Ordering::SeqCst) >= target_replicas {
                    break;
                }

                // enumerate over replica connections
                for (idx, connection) in replica_connections.into_iter().enumerate() {
                    // skip synced replica indexes
                    if skips.contains(&idx) {
                        continue;
                    }

                    // attempt to read response for ACK command
                    let ack_resp =
                        tokio::time::timeout(Duration::from_millis(2), connection.read_resp())
                            .await;

                    let ack_resp = match ack_resp {
                        Ok(r) => r,
                        Err(_) => {
                            continue;
                        }
                    };

                    match ack_resp {
                        Ok(Some((resp, _))) => {
                            let command = Command::from_resp(resp);

                            if !command.is_ok() {
                                continue;
                            }

                            let command = command.unwrap();

                            match command {
                                Command::Replconf(cmd) => {
                                    let ack: u64 = cmd
                                        .values
                                        .get(1)
                                        .map_or("0".to_string(), |val| val.clone())
                                        .parse()
                                        .unwrap_or(0);

                                    if ack >= offset {
                                        skips.push(idx);
                                        synced_replicas_count.fetch_add(1, Ordering::SeqCst);
                                    }
                                }
                                _ => {
                                    continue;
                                }
                            }
                        }
                        Ok(None) => {
                            continue;
                        }
                        Err(_) => {
                            continue;
                        }
                    };
                }
            }
        });

        tokio::select! {
            _ = timeout => println!("WAIT Timeout {:?}", self.timeout),
            _ = check_wait_task => println!("Expected {target_replicas} replicas to be synchronised, {} replicas were synchronised", synced_replicas.load(Ordering::SeqCst))
        }

        let resp = RESP::Integer(synced_replicas.load(Ordering::SeqCst));
        dst.write_frame(&resp).await?;

        Ok(None)
    }
}

impl From<Wait> for RESP {
    fn from(value: Wait) -> Self {
        let resp = RESP::Array(vec![
            RESP::Bulk("WAIT".into()),
            RESP::Bulk(Bytes::from(value.no_of_replicas.to_string())),
            RESP::Bulk(Bytes::from(value.timeout.to_string())),
        ]);

        resp
    }
}
