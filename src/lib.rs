pub mod command;
pub mod connection;
pub mod db;
pub mod frame;
pub mod server;

pub use frame::{frame_to_string, RESPError};

pub use command::*;
pub use db::*;

/// Error returned from most functions
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Specilized Result returned from most functions
/// for convienience
pub type Result<T> = std::result::Result<T, Error>;
