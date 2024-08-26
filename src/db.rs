use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
};
use tokio::time::{Duration, Instant};

use crate::{rdb::DerivedDatabase, Value, ValueType};

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
    entries: HashMap<String, Value>,

    // Unique entries of expiration time sorted by time
    #[allow(unused)]
    expirations: BTreeSet<(Instant, String)>,

    // Replication state identifiers
    replid: Option<String>,
    repl_offset: u64,
}

impl DbGuard {
    /// create a new DbGuard instance
    pub fn new() -> DbGuard {
        DbGuard { db: Db::new() }
    }

    /// create a new DbGuard instance from derived rdb database
    pub fn from_derived(database: DerivedDatabase) -> DbGuard {
        let db = Db::from_derived(database);
        DbGuard { db }
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
        tokio::spawn(purge_expired_keys(shared.clone()));

        Db { inner: shared }
    }

    /// Create a new Instance of the Db using derived rdb database data
    pub fn from_derived(database: DerivedDatabase) -> Db {
        let shared = Arc::new(SharedDb::from_derived(database));

        // start background tasks
        tokio::spawn(purge_expired_keys(shared.clone()));

        Db { inner: shared }
    }

    /// Get the byte associated with a key
    ///
    /// Returns `None` if there's no value associated with the key
    pub fn get(&self, key: &str) -> Option<ValueType> {
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

    /// Get the all Keys
    ///
    /// Returns `None` if there's no value associated with the key
    pub fn keys(&self) -> Vec<String> {
        let state = self.inner.state.lock().unwrap();

        let keys = state
            .entries
            .keys()
            .map(|key| key.to_owned())
            .collect::<Vec<String>>();

        // don't forget to release lock on state mutex
        drop(state);

        keys
    }

    /// Set a value associated to a key with an optional expiration
    ///
    /// If the key already exists, remove it
    pub fn set(&self, key: String, value: crate::ValueType, expires_at: Option<Duration>) {
        let value = Value::new(value, expires_at);
        let mut state = self.inner.state.lock().unwrap();

        // insert expires_at into expiration tracker
        // when key expires it'll automatically be removed later
        if let Some(expiry) = value.expires_at {
            state.expirations.insert((expiry, key.clone()));
        }

        // Insert key value entry into store
        state.entries.insert(key.clone(), value);

        drop(state);
    }

    pub fn set_repl_id(&self, replid: String) {
        let mut state = self.inner.state.lock().unwrap();
        let state = &mut *state;
        state.replid = Some(replid);
    }

    pub fn get_repl_info(&self) -> (Option<String>, u64) {
        let state = self.inner.state.lock().unwrap();

        let replid = state.replid.clone();
        let repl_offset = state.repl_offset.clone();

        drop(state);

        (replid, repl_offset)
    }
}

impl SharedDb {
    pub fn new() -> SharedDb {
        SharedDb {
            state: Mutex::new(State {
                entries: HashMap::new(),
                expirations: BTreeSet::new(),
                replid: None,
                repl_offset: 0,
            }),
        }
    }

    pub fn from_derived(datbase: DerivedDatabase) -> SharedDb {
        SharedDb {
            state: Mutex::new(State {
                entries: datbase.entries,
                expirations: datbase.expirations,
                replid: None,
                repl_offset: 0,
            }),
        }
    }

    /// Purge expired keys and return Instant of the next
    /// expiration
    pub fn clear_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();

        let state = &mut *state;

        let now = Instant::now();

        while let Some((expires_at, key)) = state.expirations.iter().next() {
            let expires_at = expires_at.to_owned();
            if expires_at > now {
                return Some(expires_at);
            }

            state.entries.remove(key.as_str());
            state
                .expirations
                .remove(&(expires_at, key.clone().to_owned()));
        }

        None
    }
}

impl State {
    pub fn next_expiration(&self) -> Option<Instant> {
        self.expirations.iter().next().map(|entry| entry.0)
    }
}

// TODO: Implement background task notifier and shutdown listner
// the cache
pub async fn purge_expired_keys(shared_db: Arc<SharedDb>) {
    // run a loop
    // wait for the next instant in the expiry and remove expired keys
    // from the cache
    loop {
        if let Some(when) = shared_db.clear_expired_keys() {
            // expired entries have been purged and the next entry is returned
            // wait until when to purge state again
            // println!("Wait until {:?} to purge state", &when);
            tokio::time::sleep_until(when).await;
        } else {
            // println!("Sleep for 1 sec");
            tokio::time::sleep_until(Instant::now() + Duration::from_millis(10)).await;
        }
    }
}
