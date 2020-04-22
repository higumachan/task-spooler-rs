use tokio::net::UnixStream;
use tokio::prelude::*;

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
}