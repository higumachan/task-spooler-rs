use std::str::FromStr;
use argparse::{ArgumentParser, List, Store, StoreOption};
use crate::task_spooler::{CommandPart, Task, ResourceRequirements, TaskStatus, ResourceType, Argument};
use crate::connections::types::RequestType;
use crate::connections::client::Client;
use std::path::PathBuf;
use tokio::io::{AsyncWriteExt, AsyncReadExt};

extern crate taskspooler;
use taskspooler::task_spooler;
use taskspooler::connections;


#[allow(non_camel_case_types)]
#[derive(Debug)]
enum Command {
    enqueue,
    show_queue,
}

impl FromStr for Command {
    type Err = ();
    fn from_str(src: &str) -> Result<Self, ()> {
        return match src {
            "enqueue" => Ok(Command::enqueue),
            "eq" => Ok(Command::enqueue),
            "show" => Ok(Command::show_queue),
            _ => Err(()),
        };
    }
}


#[tokio::main]
async fn main() {
    let client = Client::new("test.unix");

    if !client.is_server_starting() {
    }

    let mut subcommand = Command::show_queue;
    let mut args = vec!();
    let mut parse_result = Ok(());
    {
        let mut ap = ArgumentParser::new();
        ap.set_description("Task spooling command");
        ap.refer(&mut subcommand).add_argument("command", Store,
                                               r#"Command to run (either "enqueue",)"#);
        ap.refer(&mut args)
            .add_argument("arguments", List,
                          r#"Arguments for command"#);
        ap.stop_on_first_argument(true);
        parse_result = ap.parse(std::env::args().collect(),
                                &mut std::io::sink(),
                                &mut std::io::sink());
    }
    if let Err(_) = parse_result {
        {
            let mut ap = ArgumentParser::new();
            ap.refer(&mut args)
                .add_argument("arguments", List,
                              r#"Arguments for command"#);
            ap.stop_on_first_argument(true);
            ap.parse_args_or_exit();
        }
        if args.len() == 0 {
            subcommand = Command::show_queue;
        } else {
            subcommand = Command::enqueue;
        }
    }

    match subcommand {
        Command::enqueue => enqueue_command(&client, args).await,
        Command::show_queue => show_queue_command(&client, args).await,
    };
}

async fn enqueue_command(client: &Client, mut args: Vec<String>) {
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
        ap.stop_on_first_argument(true);
        ap.parse_args_or_exit();
    }
    let requires = requires.iter().map(|x| {
        let ts: Vec<_> = x.split(":").collect();
        (ResourceType::from_str(&ts[0]).unwrap(), usize::from_str(ts[1]).unwrap())
    }).collect::<ResourceRequirements>();

    let command_part = CommandPart::new(program.as_str());
    let command_part = command_part.args(&program_args);
    let request = connections::types::RequestType::Enqueue(
        command_part,
        None,
        Some(requires),
    );

    let bytes = bincode::serialize(&request).unwrap();
    let mut stream = client.connect().await.expect("connection fail");
    stream.write(&bytes).await.unwrap();
}

async fn show_queue_command(client: &Client, mut args: Vec<String>) {
    let bytes = bincode::serialize(&RequestType::ShowQueue()).unwrap();
    let mut stream = client.connect().await.expect("connection fail");
    stream.write(&bytes).await.unwrap();
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await.unwrap();
    let tasklist: Vec<(TaskStatus, Task)> = bincode::deserialize(&buf[0..n]).unwrap();

    println!("id\tstatus    \tcommand          \trequirements\toutput");
    for (status, task) in tasklist {
        let command_part: Box<dyn std::fmt::Display> = if task.command_part_exec.is_some() {
            Box::new(task.command_part_exec.unwrap())
        } else { Box::new(task.command_part_plan) };
        let command = format!("{}", command_part);
        let command = truncate_string(&command, 16).unwrap();
        println!("{}\t{:^10}\t{:<12}\t{}\t{}",
                 task.id,
                 status,
                 command,
                 format_resource(&task.requirements),
                 task.output_filepath.as_ref().map_or("", |x| x.as_os_str().to_str().unwrap()));
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
