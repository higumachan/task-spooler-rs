use tokio::net::UnixStream;
use tokio::prelude::*;
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub struct Client {
    server_pid_path: String,
    socket_path: String,
}

impl Client {
    pub fn new(server_pid_path: &str, socket_path: &str) -> Self {
        Client {
            server_pid_path: server_pid_path.to_string(),
            socket_path: socket_path.to_string(),
        }
    }

    pub async fn connect(&self) -> tokio::io::Result<UnixStream> {
        UnixStream::connect(&self.socket_path).await
        //TcpStream::connect("127.0.0.1:7135").await
    }

    pub fn is_server_starting(&self) -> bool {
        // TODO(higumachan): pidが生きてるかも確認する。
        PathBuf::from_str(&self.server_pid_path).unwrap().exists()
    }
}