use std::fmt;

#[derive(Debug, Clone)]
pub struct ReplicaInfo {
    pub host: String,
    pub port: String,
    pub role: Role,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Role {
    Master,
    Slave,
}

impl ReplicaInfo {
    pub fn key(&self) -> String {
        format!("{}:{}", self.host.clone(), self.port.clone())
    }
}

impl fmt::Display for Role {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Role::Master => "master".fmt(fmt),
            Role::Slave => "slave".fmt(fmt),
        }
    }
}

// impl PartialEq for Role {
//     fn eq(&self, other: &Self) -> bool {
//         self.to_string() == other.to_string()
//     }
// }
