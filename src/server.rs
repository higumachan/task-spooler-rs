use tokio::net::TcpListener;
use tokio::prelude::*;
use tokio::process::Command;
use futures::future::*;
use std::sync::RwLock;
use std::sync::{Arc};
use std::collections::{VecDeque, HashMap};

mod task_spooler;
use task_spooler::{TaskSpooler, Consumer, TaskQueue, CommandPart};

mod connections;

#[tokio::main]
async fn main() {
    let tsp = TaskSpooler::default();

    /*
    for i in 1..5 {

        tsp.task_queue.write().unwrap().enqueue(
            CommandPart::new("sleep").args(&vec![i.to_string()]),
            None,
            None,
        );
    }
    */

    println!("start server");
    tokio::join!(tsp.run(), server_loop("127.0.0.1", 7135));
}

async fn server_loop(addr: &str, port: u16) -> Result<(), Box<dyn std::error::Error>> {
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
            }
        });
    }
}