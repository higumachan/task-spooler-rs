use tokio::prelude::*;

extern crate taskspooler;
use taskspooler::server;

#[tokio::main]
async fn main() {
    server::run_server().await;
}
