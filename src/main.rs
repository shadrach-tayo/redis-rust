use redis_starter_rust::{
    parse_config,
    server::{self},
    Error,
};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut args = std::env::args();

    // dispose file path
    let _ = args.next();

    let config = parse_config(&mut args);

    let addr = format!("127.0.0.1:{}", config.port);

    let listener = TcpListener::bind(addr).await?;

    server::run(listener, config).await?;

    Ok(())
}
