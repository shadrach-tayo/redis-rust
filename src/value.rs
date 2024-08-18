use std::{collections::HashMap, time::Duration};

use bytes::Bytes;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct Value {
    pub expires_at: Option<Instant>,
    pub data: ValueType,
    pub _created_at: Instant,
}

#[derive(Debug, Clone)]
pub enum ValueType {
    String(Bytes),
    Stream(Vec<StreamData>),
}

#[derive(Debug, Clone)]
pub struct StreamData {
    pub id: (u64, u64),
    pub pairs: HashMap<String, String>,
    pub _created_at: Instant,
}

impl Value {
    pub fn new(data: ValueType, expiration: Option<Duration>) -> Value {
        // Convert expires at to timestamp using the .map method
        // add current timestamp to duration to get when the
        // key will expire, defaults to None
        let expires_at = expiration.map(|duration| {
            let now = tokio::time::Instant::now();
            now + duration
        });

        Value {
            expires_at,
            data,
            _created_at: Instant::now(),
        }
    }

    pub fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(expiry) => Instant::now() > expiry,
            None => false,
        }
    }
}
