use std::{io::Cursor, ops::Mul, path::Path};

use byteorder::ByteOrder;
use bytes::{Buf, BytesMut};
use redis_derive::gen_cursor_util;

use crate::{rdb::Filter, Result};

use super::{Builder, DerivedDatabase, Type};

pub mod constants {
    pub const RDB_6BITLEN: u8 = 0;
    pub const RDB_14BITLEN: u8 = 1;
    pub const RDB_ENCVAL: u8 = 3;
    // pub const RDB_MAGIC: &'static str = "REDIS";
}

pub mod opcodes {
    pub const EOF: u8 = 255;
    pub const SELECTDB: u8 = 254;
    pub const EXPIRETIME: u8 = 253;
    pub const EXPIRETIME_MS: u8 = 252;
    pub const RESIZEDB: u8 = 251;
    pub const AUX: u8 = 250;
}

pub mod encoding_type {
    pub const STRING: u8 = 0;
    pub const LIST: u8 = 1;
    pub const SET: u8 = 2;
    pub const ZSET: u8 = 3;
    pub const HASH: u8 = 4;
    pub const HASH_ZIPMAP: u8 = 9;
    pub const LIST_ZIPLIST: u8 = 10;
    pub const SET_INTSET: u8 = 11;
    pub const ZSET_ZIPLIST: u8 = 12;
    pub const HASH_ZIPLIST: u8 = 13;
    pub const LIST_QUICKLIST: u8 = 14;
}

pub mod encoding {
    pub const INT8: u32 = 0;
    pub const INT16: u32 = 1;
    pub const INT32: u32 = 2;
    pub const LZF: u32 = 3;
}

pub mod helpers {
    pub fn convert_int_to_vec(num: i32) -> crate::Result<Vec<u8>> {
        let string = num.to_string();
        let mut vec = vec![];
        for &byte in string.as_bytes().iter() {
            vec.push(byte);
        }

        Ok(vec)
    }
}

pub struct RdbParser<F: Filter, B: Builder> {
    filter: F,
    builder: B,
    last_expiry_time: Option<u64>,
    rdb: Vec<u8>,
}

pub fn read_db_file<P: AsRef<Path>>(path: P) -> Result<Vec<u8>> {
    let file = std::fs::read(path)?;
    Ok(file)
}

impl<F: Filter, B: Builder> RdbParser<F, B> {
    pub fn new(filter: F, builder: B, rdb: Vec<u8>) -> RdbParser<F, B> {
        RdbParser {
            builder,
            filter,
            rdb,
            last_expiry_time: None,
        }
    }

    pub fn parse(&mut self) -> crate::Result<Option<DerivedDatabase>> {
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(&self.rdb);

        let mut cursor = Cursor::new(&buffer[..]);

        verify_magic(&mut cursor)?;
        verify_version(&mut cursor)?;

        let mut db_count = 0;

        loop {
            let next_op = get_u8(&mut cursor)?;

            match next_op {
                opcodes::EOF => {
                    // end database
                    // end rdb
                    let pos = cursor.position() as usize;
                    if cursor.has_remaining() {
                        let checksum: Vec<u8> = cursor.get_ref()[pos..].to_vec();
                        println!("Checksum: {:?}", checksum);
                    }
                    break;
                }
                opcodes::AUX => {
                    let key = self.read_string(&mut cursor)?;
                    let value = self.read_string(&mut cursor)?;
                    // println!("Aux: {}: {:?}", key, value);

                    // set aux key-val pair
                    self.builder.set_aux_field(key, value)
                }
                opcodes::SELECTDB => {
                    // start database and read database till the end
                    (db_count, _) = get_length_with_encoding(&mut cursor)?;
                    println!("Start database: {db_count}");
                    self.builder.start_database();
                }
                opcodes::RESIZEDB => {
                    let db_size = get_length(&mut cursor)?;
                    let expiry_size = get_length(&mut cursor)?;

                    self.builder.resizedb(db_size, expiry_size);
                }
                opcodes::EXPIRETIME => {
                    let pos = cursor.position() as usize;
                    let buf: Vec<u8> = cursor.get_ref()[pos..pos + 4].to_vec();
                    cursor.advance(4);

                    let expiry_time: u64 =
                        u64::from(byteorder::LittleEndian::read_u32(&buf)).mul(1000);
                    println!("Expire time; {expiry_time}");
                    self.last_expiry_time = Some(expiry_time);
                }
                opcodes::EXPIRETIME_MS => {
                    let pos = cursor.position() as usize;

                    let buf: Vec<u8> = cursor.get_ref()[pos..pos + 8].to_vec();
                    cursor.advance(8);

                    let expiry_time: u64 = byteorder::LittleEndian::read_u64(&buf);
                    self.last_expiry_time = Some(expiry_time);
                    println!("EXPIRETIME_MS: {expiry_time}");
                }
                _ => {
                    if self.filter.matches_db(db_count) {
                        let key = self.read_data(&mut cursor)?;
                        if self.filter.matches_key(&key) && self.filter.matches_type(next_op) {
                            self.read_type(&mut cursor, &key, next_op)?;
                        } else {
                            self.skip_object(&mut cursor, next_op)?;
                        }
                    } else {
                        self.skip_key_and_object(&mut cursor, next_op)?;
                    }

                    self.last_expiry_time = None;
                }
            }
        }

        Ok(self.builder.get_database())
    }

    fn read_data(&self, src: &mut Cursor<&[u8]>) -> crate::Result<Vec<u8>> {
        let (len, is_encoded) = get_length_with_encoding(src)?;

        if is_encoded {
            match len {
                encoding::INT8 => helpers::convert_int_to_vec(get_i8(src)? as i32),
                encoding::INT16 => helpers::convert_int_to_vec(get_i16(src)? as i32),
                encoding::INT32 => helpers::convert_int_to_vec(get_i32(src)? as i32),
                encoding::LZF => {
                    todo!()
                }
                _ => {
                    panic!("Unknown encoding 🤧: {}", len)
                }
            }
        } else {
            let pos = src.position() as usize;
            let data = &src.get_ref()[pos..pos + len as usize].to_vec();
            src.advance(len as usize);
            Ok(data.to_owned())
        }
    }

    fn read_type(&self, src: &mut Cursor<&[u8]>, key: &[u8], enc_type: u8) -> crate::Result<()> {
        match enc_type {
            encoding_type::STRING => {
                let val = self.read_data(src)?;
                self.builder.set(
                    String::from_utf8(key.to_owned())?,
                    val,
                    self.last_expiry_time,
                );
            }
            _ => panic!(
                "Unimplemented Type encoding: {:?}",
                Type::from_encoding(enc_type)
            ),
        }
        Ok(())
    }

    fn read_string(&self, src: &mut Cursor<&[u8]>) -> crate::Result<String> {
        let data = self.read_data(src)?;
        let string = String::from_utf8(data)?;
        Ok(string)
    }

    fn skip(&self, src: &mut Cursor<&[u8]>, len: usize) {
        src.advance(len)
    }

    fn skip_blob(&self, src: &mut Cursor<&[u8]>) -> crate::Result<()> {
        let (len, is_encoded) = get_length_with_encoding(src)?;

        let skip_bytes;
        if is_encoded {
            skip_bytes = match len {
                encoding::INT8 => 1,
                encoding::INT16 => 2,
                encoding::INT32 => 4,
                encoding::LZF => {
                    let compressed_len = get_length(src)?;
                    let _actual_len = get_length(src)?;
                    compressed_len
                }
                _ => {
                    panic!("Unknown encoding 🤧: {}", len)
                }
            };
        } else {
            skip_bytes = len;
        }

        self.skip(src, skip_bytes as usize);

        Ok(())
    }

    fn skip_object(&self, src: &mut Cursor<&[u8]>, enc_type: u8) -> crate::Result<()> {
        let blobs_to_skip = match enc_type {
            encoding_type::STRING
            | encoding_type::HASH_ZIPMAP
            | encoding_type::LIST_ZIPLIST
            | encoding_type::SET_INTSET
            | encoding_type::ZSET_ZIPLIST
            | encoding_type::HASH_ZIPLIST => 1,
            encoding_type::LIST | encoding_type::SET | encoding_type::LIST_QUICKLIST => {
                get_length(src)?
            }
            encoding_type::ZSET | encoding_type::HASH => get_length(src)? * 2,
            _ => {
                panic!("Unknown encoding type: {}", enc_type)
            }
        };

        for _ in 0..blobs_to_skip {
            self.skip_blob(src)?;
        }

        Ok(())
    }

    fn skip_key_and_object(&self, src: &mut Cursor<&[u8]>, enc_type: u8) -> crate::Result<()> {
        self.skip_blob(src)?;
        self.skip_object(src, enc_type)?;

        Ok(())
    }
}

fn verify_magic(src: &mut Cursor<&[u8]>) -> crate::Result<()> {
    if !src.has_remaining() {
        return Err("Invalid RDB magic string".into());
    }

    let magic_bytes = &src.get_ref()[..5].to_vec();
    let string = String::from_utf8(magic_bytes.to_owned())?;

    if string.as_str() != "REDIS" {
        return Err("Incorrect magic string".into());
    }

    src.advance(5);

    Ok(())
}

fn verify_version(src: &mut Cursor<&[u8]>) -> crate::Result<u64> {
    if !src.has_remaining() {
        return Err("Invalid RDB file".into());
    }

    let pos = src.position() as usize;
    let magic_bytes = &src.get_ref()[pos..pos + 4].to_vec();
    let version: u64 = bytes_to_u64(magic_bytes)?;
    println!("RDB Version: {version}");

    src.advance(4);

    Ok(version)
}

fn bytes_to_u64(bytes: &Vec<u8>) -> crate::Result<u64> {
    let string = String::from_utf8(bytes.to_owned())?;
    Ok(string.parse()?)
}

// convert to macro 👀
gen_cursor_util!(get_i8);
// pub fn get_i8(src: &mut Cursor<&[u8]>) -> crate::Result<i8> {
//     if !src.has_remaining() {
//         return Err("Invalid i8".into());
//     }

//     Ok(src.get_i8())
// }

gen_cursor_util!(get_i16);
// pub fn get_i16(src: &mut Cursor<&[u8]>) -> crate::Result<i16> {
//     if !src.has_remaining() {
//         return Err("Invalid i16".into());
//     }

//     Ok(src.get_i16())
// }

gen_cursor_util!(get_i32);
// pub fn get_i32(src: &mut Cursor<&[u8]>) -> crate::Result<i32> {
//     if !src.has_remaining() {
//         return Err("Invalid i32".into());
//     }

//     Ok(src.get_i32())
// }

gen_cursor_util!(get_u8);
// pub fn get_u8(src: &mut Cursor<&[u8]>) -> crate::Result<u8> {
//     if !src.has_remaining() {
//         return Err("Invalid u8".into());
//     }

//     Ok(src.get_u8())
// }

gen_cursor_util!(get_u32);
// pub fn get_u32(src: &mut Cursor<&[u8]>) -> crate::Result<u32> {
//     if !src.has_remaining() {
//         return Err("Invalid u32".into());
//     }

//     Ok(src.get_u32())
// }

gen_cursor_util!(get_u64);
// pub fn get_u64(src: &mut Cursor<&[u8]>) -> crate::Result<u64> {
//     if !src.has_remaining() {
//         return Err("Invalid u64".into());
//     }

//     Ok(src.get_u64())
// }

fn get_length_with_encoding(src: &mut Cursor<&[u8]>) -> crate::Result<(u32, bool)> {
    let enc_byte = get_u8(src)?;

    match (enc_byte & 0xCF) >> 6 {
        constants::RDB_ENCVAL => Ok(((enc_byte & 0x3F) as u32, true)),
        constants::RDB_6BITLEN => Ok(((enc_byte & 0x3F) as u32, false)),
        constants::RDB_14BITLEN => {
            let next_byte = get_u8(src)?;
            Ok(((((enc_byte & 0x3F) as u32) << 8) | next_byte as u32, false))
        }
        _ => Ok((get_u32(src)?, false)),
    }
}

fn get_length(src: &mut Cursor<&[u8]>) -> crate::Result<u32> {
    let (len, _) = get_length_with_encoding(src)?;
    Ok(len)
}
