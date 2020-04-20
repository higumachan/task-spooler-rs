use std::str::FromStr;
use tokio::net::TcpStream;
use tokio::prelude::*;
use argparse::{ArgumentParser, List, Store};
use std::io::{stdout, stderr};
use crate::task_spooler::CommandPart;

mod task_spooler;
mod connections;


#[allow(non_camel_case_types)]
#[derive(Debug)]
enum Command {
    enqueue,
}

impl FromStr for Command {
    type Err = ();
    fn from_str(src: &str) -> Result<Command, ()> {
        return match src {
            "enqueue" => Ok(Command::enqueue),
            _ => Err(()),
        };
    }
}

#[tokio::main]
async fn main() {
    let mut subcommand = Command::enqueue;
    let mut args = vec!();
    {
        let mut ap = ArgumentParser::new();
        ap.set_description("Task spooling command");
        ap.refer(&mut subcommand).required()
            .add_argument("command", Store,
                          r#"Command to run (either "enqueue",)"#);
        ap.refer(&mut args)
            .add_argument("arguments", List,
                          r#"Arguments for command"#);
        ap.stop_on_first_argument(true);
        ap.parse_args_or_exit();
    }

    (match subcommand {
        Command::enqueue => enqueue_command(args),
    }).await;
}

async fn enqueue_command(mut args: Vec<String>) {
    args.insert(0, "enqueue".to_string());
    let mut program = "".to_string();
    let mut program_args = vec!();
    {
        let mut ap = ArgumentParser::new();
        ap.set_description("enqueue task");
        ap.refer(&mut program).required().add_argument("program", Store, "TODO");
        ap.refer(&mut program_args).add_argument("program_arguments", List, "TODO");
        match ap.parse(args, &mut stdout(), &mut stderr()) {
            Ok(()) =>  {}
            Err(x) => {
                std::process::exit(x);
            }
        }
    }

    let request = connections::types::RequestType::Enqueue(
        CommandPart::new(program.as_str()).args(&program_args),
        None,
        None
    );

    let bytes = bincode::serialize(&request).unwrap();
    let mut stream = TcpStream::connect("127.0.0.1:7135").await.expect("connection fail");
    stream.write(&bytes).await.unwrap();
}
