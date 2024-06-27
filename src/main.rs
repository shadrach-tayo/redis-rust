use std::env::Args;

use redis_starter_rust::{server::Listener, DbGuard, Error, ReplicaInfo, Role};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut args = std::env::args();

    // dispose file path
    let _ = args.next();

    let config = parse_config(&mut args);

    let addr = format!("127.0.0.1:{}", config.port);

    let listener = TcpListener::bind(addr).await?;

    let db = DbGuard::new();
    let mut server = Listener::new(listener, db);

    if let Some(replica_info) = config.replica_info {
        println!("Replica info {:?}", &replica_info);
        server.set_master(replica_info).await;
    }

    tokio::select! {
        result = server.run() => {
            if let Err(err) = result {
            // println!("Server error {:?}", err);s
                Err(err)
            } else {
                Ok(())
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct CliConfig {
    pub port: u64,
    pub replica_info: Option<ReplicaInfo>,
}

fn parse_config(args: &mut Args) -> CliConfig {
    const MSG: &str = "Pass --port <port> argument to start command";
    let mut config = CliConfig {
        port: 6379,
        replica_info: None,
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
        config.replica_info = Some(ReplicaInfo {
            host: host.to_string(),
            port: port.to_string(),
            role: Role::Master,
        });
    }

    config
}
