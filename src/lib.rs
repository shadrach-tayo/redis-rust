pub mod connection;
pub mod frame;
pub mod server;

pub use frame::{frame_to_string, RESPError};

/// Error returned from most functions
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Specilized Result returned from most functions
/// for convienience
pub type Result<T> = std::result::Result<T, Error>;
