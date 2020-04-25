use tokio::prelude::*;
use daemonize::Daemonize;
use std::path::PathBuf;
use std::str::FromStr;
use std::thread::sleep;
use std::time::Duration;
use std::fs::File;

extern crate taskspooler;
use taskspooler::server;

#[tokio::main]
async fn main() {
    let pid_file = PathBuf::from_str("/tmp/tsp.pid").unwrap();
    let stdout_path = PathBuf::from_str("/tmp/tsp.out").unwrap();
    let stderr_path = PathBuf::from_str("/tmp/tsp.err").unwrap();
    let stdout = File::create(&stdout_path).unwrap();
    let stderr = File::create(&stderr_path).unwrap();

    let daemonize = Daemonize::new()
        .pid_file(pid_file)
        .working_directory("/tmp")
        .umask(0o777)
        .stdout(stdout)
        .stderr(stderr)
        ;

    match daemonize.start() {
        Ok(_) => {
            server::run_server().await;
        },
        Err(e) => {
            println!("error daemonize {:?}", e)
        }
    }
}
