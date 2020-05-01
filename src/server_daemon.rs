use tokio::prelude::*;
use tokio::runtime::Runtime;
use daemonize::Daemonize;
use std::path::PathBuf;
use std::str::FromStr;
use std::thread::sleep;
use std::time::Duration;
use std::fs::File;
use std::env;

extern crate taskspooler;
use taskspooler::server;
use users::{get_current_uid, get_current_gid};

fn main() {
    let c: Vec<String> = env::args().collect();
    let socket_path = c[1].clone();

    let pid_file = PathBuf::from_str("/tmp/tsp.pid").unwrap();
    let stdout_path = PathBuf::from_str("/tmp/tsp.out").unwrap();
    let stderr_path = PathBuf::from_str("/tmp/tsp.err").unwrap();
    let stdout = File::create(&stdout_path).unwrap();
    let stderr = File::create(&stderr_path).unwrap();

    let daemonize = Daemonize::new()
        .pid_file(pid_file)
        .working_directory(".")
        .user(get_current_uid())
        .group(get_current_gid())
        .umask(0o022)
        .stdout(stdout)
        .stderr(stderr)
        .privileged_action(move || {
            let mut rt = Runtime::new().unwrap();
            rt.block_on(server::run_server(socket_path.as_str()));
        })
    ;

    match daemonize.start() {
        Ok(_) => {
            panic!("block privileged action")
        },
        Err(e) => {
            println!("error daemonize {:?}", e)
        }
    }
}
