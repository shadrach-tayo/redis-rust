pub mod command;
pub mod config;
pub mod connection;
pub mod db;
pub mod replication;
pub mod resp;
pub mod server;

pub use config::{parse_config, CliConfig};

pub use resp::RESPError;

pub use command::*;
pub use db::*;
pub use replication::*;

/// Error returned from most functions
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Specilized Result returned from most functions
/// for convienience
pub type Result<T> = std::result::Result<T, Error>;
