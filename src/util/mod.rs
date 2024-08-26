use std::{
    hash::{Hash, Hasher},
    time::UNIX_EPOCH,
};

pub fn gen_rand_string(len: usize) -> String {
    let mut rnd_str = String::new();
    for _ in 0..len {
        let rnd_char = std::char::from_u32(gen_rand_number() % 27 + 96).unwrap();
        rnd_str.push(rnd_char);
    }
    rnd_str
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
