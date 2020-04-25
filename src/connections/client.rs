use tokio::net::UnixStream;
use tokio::prelude::*;
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub struct Client {
    socket_path: String,
}

impl Client {
    pub fn new(socket_path: &str) -> Self {
        Client {
            socket_path: socket_path.to_string(),
        }
    }

    pub async fn connect(&self) -> tokio::io::Result<UnixStream> {
        UnixStream::connect(&self.socket_path).await
    }

    pub fn is_server_starting(&self) -> bool {
        PathBuf::from_str(&self.socket_path).unwrap().exists()
    }
}