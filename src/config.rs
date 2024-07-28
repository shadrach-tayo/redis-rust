use std::{
    env::Args,
    sync::{atomic::AtomicU64, Arc},
};

use crate::{ReplicaInfo, Role};

#[derive(Debug, Default)]
pub struct CliConfig {
    pub port: u64,
    pub master: Option<ReplicaInfo>,
    pub is_replication: bool,
}

pub fn parse_config(args: &mut Args) -> CliConfig {
    const MSG: &str = "Pass --port <port> argument to start command";
    let mut config = CliConfig {
        port: 6379,
        master: None,
        is_replication: false,
    };

    // let mut port: u64 = 6379;
    let mut master_info: String = "".to_string();

    let mut next_arg = args.next();
    while next_arg != None {
        match next_arg {
            Some(s) if s == "--port".to_string() => match args.next().unwrap().parse() {
                Ok(int) => {
                    config.port = int;
                }
                Err(_) => panic!("Could not parse "),
            },
            Some(s) if s == "--replicaof".to_string() => match args.next() {
                Some(arg) => {
                    master_info = arg.clone();
                    config.is_replication = true;
                }
                None => panic!("Could not parse replica info "),
            },
            Some(s) => {
                println!("arg {}", s);
                panic!("Invalid arg: {} passed to server, {}", s, MSG)
            }
            None => (),
        };

        next_arg = args.next();
    }

    let info = master_info.split_whitespace().collect::<Vec<&str>>();
    if info.len() == 2 {
        let host = info[0];
        let port = info[1];
        config.master = Some(ReplicaInfo {
            host: host.to_string(),
            port: port.to_string(),
            role: Role::Master,
        });
    }

    config
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub role: Role,
    pub master_repl_offset: Arc<AtomicU64>,
    pub master_repl_id: Option<String>,
    pub network_config: Option<(String, u64)>,
}

impl ServerConfig {
    pub fn new(
        network: Option<(String, u64)>,
        role: Role,
        master_repl_id: Option<String>,
        master_repl_offset: Arc<AtomicU64>,
    ) -> Self {
        ServerConfig {
            role,
            master_repl_id,
            master_repl_offset,
            network_config: network,
        }
    }
}
