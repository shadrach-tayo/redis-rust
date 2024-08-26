use std::{
    hash::{Hash, Hasher},
    time::UNIX_EPOCH,
};

use rand::{thread_rng, Rng};

pub fn gen_rand_string(len: usize) -> String {
    thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

pub fn gen_rand_number() -> u32 {
    let time_ms = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    time_ms.hash(&mut hasher);
    hasher.finish() as u32
}

#[cfg(test)]
mod test {
    use super::gen_rand_string;

    #[test]
    fn gen_random_string() {
        let rnd_str = gen_rand_string(20);
        assert_eq!(rnd_str.len(), 20);
        let rnd_str = gen_rand_string(40);
        assert_eq!(rnd_str.len(), 40);
    }
}
