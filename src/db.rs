use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
    time::{self, Duration, Instant},
};
use tokio;

use bytes::Bytes;

/// Instantiates a single db and exposes multiple references
/// of it to the server
#[derive(Debug)]
pub struct DbGuard {
    db: Db,
}

/// A database wrapper structure that encapsulates the
/// shared database state
#[derive(Debug, Clone)]
pub struct Db {
    pub inner: Arc<SharedDb>,
}

#[derive(Debug)]
pub struct SharedDb {
    pub state: Mutex<State>,
}

/// State management for protocol
///
/// # keys
/// entries: the key-value store for cached contents,
/// expirations: Stored entries expiration in BTreeSet for it's sorting benefits
#[derive(Debug)]
pub struct State {
    // key value map for storing cached entries
    entries: HashMap<String, Entry>,

    // Unique entries of expiration time sorted by time
    expirations: BTreeSet<(Instant, String)>,
}

#[derive(Debug)]
struct Entry {
    // data to store in bytes
    data: Bytes,

    // optional expiration duration of the data stored
    expires_at: Option<Instant>,
}

impl DbGuard {
    /// create a new DbGuard instance
    pub fn new() -> DbGuard {
        DbGuard { db: Db::new() }
    }

    pub fn db(&self) -> Db {
        self.db.clone()
    }
}

impl Db {
    /// Create a new Instance of the Db
    pub fn new() -> Db {
        let shared = Arc::new(SharedDb::new());

        // start background tasks
        // tokio::spawn(purge_expired_keys(shared.clone()));

        Db { inner: shared }
    }

    /// Get the byte associated with a key
    ///
    /// Returns `None` if there's no value associated with the key
    pub fn get(&self, key: &str) -> Option<Bytes> {
        let state = self.inner.state.lock().unwrap();

        let entry = state.entries.get(key);

        if entry.is_none() {
            return None;
        }

        let bytes = entry.unwrap().data.clone();

        // don't forget to release lock on state mutex
        drop(state);

        Some(bytes)
    }

    /// Set a value associated to a key with an optional expiration
    ///
    /// If the key already exists, remove it
    pub fn set(&self, key: String, value: Bytes, expires_at: Option<Duration>) {
        let mut state = self.inner.state.lock().unwrap();

        // Convert expires at to timestamp using the .map method
        // add current timestamp to duration to get when the
        // key will expire
        // defaults to None
        let expiry = expires_at.map(|duration| {
            let now = time::Instant::now();
            now + duration
        });

        // Insert key value entry into store
        let prev = state.entries.insert(
            key.clone(),
            Entry {
                data: value,
                expires_at: expiry,
            },
        );

        // if the key exist, remove the entry

        if let Some(prev) = prev {
            state.entries.remove(&key);

            if let Some(expires_at) = prev.expires_at {
                state.expirations.remove(&(expires_at, key.clone()));
            }
        };

        // insert expires_at into expiration tracker
        // when key expires it'll automatically be removed later
        if let Some(expiry) = expiry {
            state.expirations.insert((expiry, key.clone()));
        }

        drop(state);
    }
}

impl SharedDb {
    pub fn new() -> SharedDb {
        SharedDb {
            state: Mutex::new(State {
                entries: HashMap::new(),
                expirations: BTreeSet::new(),
            }),
        }
    }
}

// TODO: Implement background task to remove expired keys from
// the cache
pub async fn purge_expired_keys(_shared_db: Arc<SharedDb>) {
    // run a loop
    // wait for the next instant in the expiry and remove expired keys
    // from the cache
    todo!()
}