use crate::{connection::Connection, frame::RESP};

#[derive(Debug, Default)]
pub struct Unknown {
    command_name: String,
}

impl Unknown {
    /// contruct new Unknown command
    pub fn new(command_name: String) -> Self {
        Unknown { command_name }
    }

    /// Returns command name
    pub fn get_name(&self) -> &str {
        &self.command_name
    }

    /// Apply the echo command and write to the Tcp connection stream
    pub async fn apply(self, _dst: &mut Connection) -> crate::Result<Option<RESP>> {
        #[allow(unused_assignments)]
        let resp = RESP::Error(format!("Err unknown command: {}", self.command_name));

        Ok(Some(resp))
    }
}

// impl From<Unknown> for RESP {
//     fn from(value: Unknown) -> Self {
//         let mut resp = RESP::array();
//         resp.push_bulk(Bytes::from("Unknown"));
//         if let Some(msg) = value.msg {
//             resp.push_bulk(Bytes::from(msg));
//         }
//         resp
//     }
// }
