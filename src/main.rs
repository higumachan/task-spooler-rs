use std::str::FromStr;
use tokio::net::TcpStream;
use tokio::prelude::*;
use argparse::{ArgumentParser, List, Store};
use std::io::{stdout, stderr};
use crate::task_spooler::{CommandPart, Task, ResourceRequirements, TaskStatus, ResourceType};
use crate::connections::types::RequestType;

pub mod task_spooler;
pub mod connections;


#[allow(non_camel_case_types)]
#[derive(Debug)]
enum Command {
    enqueue,
    show_queue,
}

impl FromStr for Command {
    type Err = ();
    fn from_str(src: &str) -> Result<Command, ()> {
        return match src {
            "enqueue" => Ok(Command::enqueue),
            "show" => Ok(Command::show_queue),
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

    match subcommand {
        Command::enqueue => enqueue_command(args).await,
        Command::show_queue => show_queue_command(args).await,
    };
}

async fn enqueue_command(mut args: Vec<String>) {
    args.insert(0, "enqueue".to_string());
    let mut program = "".to_string();
    let mut program_args = vec!();
    let mut requires: Vec<String> = vec!();
    {
        let mut ap = ArgumentParser::new();
        ap.set_description("enqueue task");
        ap.refer(&mut requires).add_option(&["--require", "-r"], List, "TODO");
        ap.refer(&mut program).required().add_argument("program", Store, "TODO");
        ap.refer(&mut program_args).add_argument("program_arguments", List, "TODO");
        match ap.parse(args, &mut stdout(), &mut stderr()) {
            Ok(()) =>  {}
            Err(x) => {
                std::process::exit(x);
            }
        }
    }
    let requires = requires.iter().map(|x| {
        let ts: Vec<_> = x.split(":").collect();
        (ResourceType::from_str(&ts[0]).unwrap(), usize::from_str(ts[1]).unwrap())
    }).collect::<ResourceRequirements>();

    let request = connections::types::RequestType::Enqueue(
        CommandPart::new(program.as_str()).args(&program_args),
        None,
        Some(requires),
    );

    let bytes = bincode::serialize(&request).unwrap();
    let mut stream = TcpStream::connect("127.0.0.1:7135").await.expect("connection fail");
    stream.write(&bytes).await.unwrap();
}

async fn show_queue_command(mut args: Vec<String>) {
    let bytes = bincode::serialize(&RequestType::ShowQueue()).unwrap();
    let mut stream = TcpStream::connect("127.0.0.1:7135").await.expect("connection fail");
    stream.write(&bytes).await.unwrap();
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await.unwrap();
    let tasklist: Vec<(TaskStatus, Task)> = bincode::deserialize(&buf[0..n]).unwrap();

    println!("id\tstatus    \tcommand          \trequirements\toutput");
    for (status, task) in tasklist {
        let command = format!("{}", task.command_part);
        let command = truncate_string(&command, 16).unwrap();
        println!("{}\t{: ^10}\t{: <12}\t{}\t{}", task.id, status, command, format_resource(&task.requirements), "");
    };
}

fn truncate_string(s: &str, width: usize) -> Option<String> {
    if width <= 3 {
        return None;
    }
    let real_width = width - 3;

    let mut res = s[0..std::cmp::min(real_width, s.len())].to_string();
    if real_width < s.len() {
        res.extend("...".chars());
    }
    Some(res)
}

fn format_resource(requirements: &ResourceRequirements) -> String {
    let mut res = String::new();
    for (k, v) in requirements {
        res.extend(format!("{}:{} ", k, v).chars());
    }
    res
}
