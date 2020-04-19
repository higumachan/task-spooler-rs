use tokio::net::TcpListener;
use tokio::prelude::*;
use tokio::process::Command;
use futures::future::*;
use std::sync::RwLock;
use std::sync::{Arc};
use std::collections::{VecDeque, HashMap};

mod task_spooler;
use task_spooler::{TaskSpooler, Consumer, TaskQueue, CommandPart};

mod connection;

#[tokio::main]
async fn main() {
    let tsp = TaskSpooler::default();

    for i in 1..5 {

        tsp.task_queue.write().unwrap().enqueue(
            CommandPart::new("sleep").args(&vec![i.to_string()]),
            None,
            None,
        );
    }

    tsp.run().await;
}

async fn server_loop(addr: &str, port: u16) {
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

                // Write the data back
                if let Err(e) = socket.write_all(&buf[0..n]).await {
                    eprintln!("failed to write to socket; err = {:?}", e);
                    return;
                }
            }
        });
    }
}