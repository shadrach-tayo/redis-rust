#![allow(unused_variables)]

use std::{
    cell::RefCell,
    collections::{BTreeSet, HashMap},
    ops::{Add, Sub},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::Value;
use bytes::Bytes;
use tokio::time::Instant;

// database
// -- key-val hashmap
// -- expiry btreset
// -- aux hashmap
// -- metadata hashmap
//

#[derive(Debug, Default, Clone)]
pub struct Database {
    pub hash: RefCell<HashMap<String, Value>>,
    pub expirations: RefCell<BTreeSet<(Instant, String)>>,
}

#[derive(Debug, Default, Clone)]
pub struct DerivedDatabase {
    pub entries: HashMap<String, Value>,
    pub expirations: BTreeSet<(Instant, String)>,
}

impl Database {
    fn set(&self, key: String, value: Vec<u8>, expiry: Option<u64>) {
        let expire_at = {
            if expiry.is_none() {
                None
            } else {
                println!("Expires at: {:?}", expiry);
                let now = SystemTime::now();
                let elasped = now
                    .duration_since(UNIX_EPOCH)
                    .expect("Time since epoch not")
                    .as_millis();

                let now = tokio::time::Instant::now();

                let expires_at = if elasped > expiry.unwrap() as u128 {
                    let diff = Duration::from_millis((elasped - expiry.unwrap() as u128) as u64);
                    let dur = Duration::from_millis(elasped as u64).sub(diff);

                    now - dur
                } else {
                    let diff = Duration::from_millis(expiry.unwrap() - elasped as u64);
                    let dur = Duration::from_millis(elasped as u64).add(diff);

                    now + dur
                };

                Some(expires_at)
            }
        };

        println!(
            "Set Value: {key}, {:?}, {:?}",
            String::from_utf8(value.clone()),
            expire_at
        );
        self.hash.borrow_mut().insert(
            key.clone(),
            Value {
                data: crate::ValueType::String(Bytes::from(value)),
                _created_at: Instant::now(),
                expires_at: expire_at,
            },
        );

        if let Some(expire_at) = expire_at {
            self.expirations
                .borrow_mut()
                .insert((expire_at, key.clone()));
        }
    }

    fn get_db(&self) -> DerivedDatabase {
        println!("Keys: {:?}", self.hash.borrow().keys());
        DerivedDatabase {
            entries: self.hash.borrow_mut().to_owned(),
            expirations: self.expirations.borrow_mut().to_owned(),
        }
    }
}

#[derive(Debug, Default)]
pub struct RdbBuilder {
    pub databases: RefCell<Vec<Database>>,
    pub aux: RefCell<HashMap<String, String>>,
    pub current_db: RefCell<Option<Database>>,
    pub db_size: u32,
    pub expire_size: u32,
}

pub trait Builder {
    fn start_rdb(&self) {}
    fn end_rdb(&self) {}

    fn start_database(&self) {}
    fn end_database(&self) {}

    fn resizedb(&self, db_size: u32, expiry_size: u32) {}
    fn set(&self, key: String, value: Vec<u8>, expire_time: Option<u64>) {}
    fn set_aux_field(&self, key: String, value: String) {}

    fn checksum(&self) {}

    fn get_database(&self) -> Option<DerivedDatabase>;
}

impl Builder for RdbBuilder {
    fn start_rdb(&self) {
        unimplemented!()
    }

    fn end_rdb(&self) {
        unimplemented!()
    }

    fn start_database(&self) {
        // println!("Start database");
        *self.current_db.borrow_mut() = Some(Database::default());
    }

    fn end_database(&self) {
        unimplemented!()
        // self.databases
        //     .borrow_mut()
        //     .push(self.current_db.borrow_mut().clone());
    }

    fn resizedb(&self, db_size: u32, expiry_size: u32) {
        // todo!()
        println!("Resize DB--- Entries: {db_size}, Expirations: {expiry_size}")
    }

    fn set(&self, key: String, value: Vec<u8>, expire_time: Option<u64>) {
        self.current_db
            .borrow_mut()
            .as_mut()
            .unwrap()
            .set(key, value, expire_time);
    }

    fn set_aux_field(&self, key: String, value: String) {
        self.aux.borrow_mut().insert(key, value);
    }

    fn checksum(&self) {
        unimplemented!()
    }

    fn get_database(&self) -> Option<DerivedDatabase> {
        if self.current_db.borrow().is_none() {
            return None;
        }

        Some(self.current_db.borrow_mut().as_mut().unwrap().get_db())
    }
}
