use tokio::net::TcpListener;
use tokio::prelude::*;

mod task_spooler;
use task_spooler::TaskSpooler;
use std::cell::{Cell, RefCell};
use std::borrow::Borrow;
use std::sync::{Once};
use config::Source;
use std::path::PathBuf;
use std::str::FromStr;

mod connections;

fn singleton() -> Box<TaskSpooler> {
    static mut SINGLETON: Option<Box<TaskSpooler>>=None;
    static mut ONCE: Once = Once::new();

    unsafe {
        ONCE.call_once(|| {
            let mut config = config::Config::default();
            config.merge(config::File::with_name(
                dirs::home_dir().unwrap().join(PathBuf::from_str(".tsp.yaml").unwrap()).to_str().unwrap()));
            let singleton = TaskSpooler::from_config(&config);
            SINGLETON = Some(Box::new(singleton));
        });

        SINGLETON.clone().unwrap()
    }
}

#[tokio::main]
async fn main() {
    println!("start server");
    let task_spooler= singleton();
    tokio::join!(task_spooler.run(), server_loop("127.0.0.1".to_string(), 7135));
}

async fn server_loop(addr: String, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let mut listener = TcpListener::bind(format!("{}:{}", addr, port)).await?;

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut buf = [0; 1024];

            // In a loop, read data from the socket and write the data back.
            loop {
                let n = match socket.read(&mut buf).await {
                    // socket closed
                    Ok(n) if n == 0 => return,
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("failed to read from socket; err = {:?}", e);
                        return;
                    }
                };
                let r: connections::types::RequestType = bincode::deserialize(&buf[0..n]).unwrap();
                println!("got {:?}", r);
                let task_spooler = singleton();
                match r {
                    connections::types::RequestType::Enqueue(command_part, priority, resource_requirements) => {
                        task_spooler.task_queue.write().unwrap().enqueue(command_part, priority, resource_requirements);
                    }
                    connections::types::RequestType::ShowQueue() => {
                        socket.write_all(&bincode::serialize(&task_spooler.task_list()).unwrap()).await;
                    }
                    _ => {
                        panic!("unknown request");
                    }
                }

            }
        });
    }
}