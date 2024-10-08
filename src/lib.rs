pub mod command;
pub mod config;
pub mod connection;
pub mod db;
pub mod rdb;
pub mod replication;
pub mod resp;
pub mod server;
mod shutdown;
pub mod util;
pub mod value;

pub use config::{parse_config, CliConfig};

pub use resp::RESPError;

pub use command::*;
pub use db::*;
pub use replication::*;
use shutdown::Shutdown;
pub use util::gen_rand_string;
pub use value::*;

/// Error returned from most functions
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Specilized Result returned from most functions
/// for convienience
pub type Result<T> = std::result::Result<T, Error>;
