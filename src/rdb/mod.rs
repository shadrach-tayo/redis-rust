pub mod dbfile;
pub mod filter;
pub mod parser;

pub use dbfile::*;
pub use filter::*;
pub use parser::*;

#[derive(Debug, PartialEq)]
pub enum Type {
    String,
    List,
    Set,
    Hash,
}

impl Type {
    pub fn from_encoding(enc_type: u8) -> Type {
        match enc_type {
            encoding_type::STRING => Type::String,
            encoding_type::SET => Type::Set,
            encoding_type::LIST => Type::List,
            encoding_type::HASH => Type::Hash,
            _ => {
                panic!("Unimplemented or unsuported encoding type -> Type transform");
            }
        }
    }
}
