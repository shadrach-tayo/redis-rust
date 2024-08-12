#![allow(unused_variables)]

use super::Type;
use std::str;

pub trait Filter {
    fn matches_db(&self, db: u32) -> bool {
        true
    }
    fn matches_key(&self, key: &[u8]) -> bool {
        true
    }
    fn matches_type(&self, enc_type: u8) -> bool {
        true
    }
}

#[derive(Debug, Default)]
pub struct DefaultFilter {
    databases: Vec<u32>,
    types: Vec<Type>,
    keys: Option<String>,
}

impl DefaultFilter {
    pub fn new() -> DefaultFilter {
        DefaultFilter::default()
    }
}

impl Filter for DefaultFilter {
    fn matches_db(&self, db: u32) -> bool {
        if self.databases.is_empty() {
            true
        } else {
            self.databases.iter().any(|&x| x == db)
        }
    }

    fn matches_key(&self, key: &[u8]) -> bool {
        if self.keys.is_none() {
            true
        } else {
            self.keys.clone().unwrap() == unsafe { str::from_utf8_unchecked(key) }
        }
    }

    fn matches_type(&self, enc_type: u8) -> bool {
        if self.types.is_empty() {
            true
        } else {
            let encoding_type = Type::from_encoding(enc_type);
            self.types.iter().any(|x| *x == encoding_type)
        }
    }
}
