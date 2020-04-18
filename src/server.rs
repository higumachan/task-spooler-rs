use tokio::net::TcpListener;
use tokio::prelude::*;
use tokio::process::Command;
use futures::future::*;
use std::sync::RwLock;
use std::sync::{Arc};
use std::collections::{VecDeque, HashMap};


type ResourceRequirements = HashMap<Resource, usize>;

#[derive(Clone)]
struct CommandPart {
    program: String,
    arguments: Vec<String>,
}

impl CommandPart {
    fn to_command(&self) -> Command {
        let mut com = Command::new(&self.program);
        com.args(&self.arguments);
        com
    }
}

#[derive(Clone)]
struct Task {
    return_code: Option<i32>,
    requirements: ResourceRequirements,
    priority: i64,
    command_part: CommandPart,
}

impl Task {
    fn new(command_part: CommandPart, priority: i64, requirements: ResourceRequirements) -> Self {
        Self {
            return_code: Option::None,
            requirements,
            priority,
            command_part,
        }
    }
    fn with_return_code(&self, return_code: i32) -> Self {
        Self {
            return_code: Some(return_code),
            priority: self.priority,
            command_part: self.command_part.clone(),
            requirements: self.requirements.clone(),
        }
    }
}

struct TaskQueue {
    waiting: Vec<Task>,
    finished: Vec<Task>,
}

impl Default for TaskQueue {
    fn default() -> Self {
        TaskQueue {
            waiting: vec![],
            finished: vec![],
        }
    }
}

impl TaskQueue {
    fn enqueue(&mut self, command_part: CommandPart, priority: Option<i64>, requirements: Option<ResourceRequirements>) {
        let priority = priority.unwrap_or(self.next_default_priority());
        let requirements = requirements.unwrap_or(HashMap::new());

        self.waiting.push(Task::new(command_part, priority, requirements));
    }
    fn dequeue_with_constraints(&mut self, consumer: &Consumer) -> Task {
        self.waiting[0].clone()
    }

    fn next_default_priority(&self) -> i64 {
        0
    }
}

#[derive(Clone, Hash, Eq, PartialEq)]
enum Resource {
    GPU,
    CPU,
}

struct Consumer {
    resources: HashMap<Resource, Vec<usize>>,
}

impl Default for Consumer {
    fn default() -> Self {
        let mut resources = HashMap::new();

        resources.insert(Resource::CPU, vec![0]);

        Self {
            resources,
        }
    }
}

impl Consumer {
    async fn consume(&self, task_queue: &Arc<RwLock<TaskQueue>>) {
        loop {
            let task = task_queue.write().unwrap().dequeue_with_constraints(self);
            let mut command = task.command_part.to_command();
            let status = command.status().await.expect("fail child command");

            let task = task.with_return_code(status.code().unwrap());
            task_queue.write().unwrap().waiting.push(task);
        }
    }
}

struct TaskSpooler {
    consumers: Arc<RwLock<Vec<Consumer>>>,
    task_queue: Arc<RwLock<TaskQueue>>,
}

impl TaskSpooler {
    async fn run(&self) {
        let consumers = self.consumers.write().unwrap();
        join_all(consumers.iter().map(|x| x.consume(&self.task_queue))).await;
    }
}

#[tokio::main]
async fn main() {
    let tsp = TaskSpooler {
        consumers: Arc::new(RwLock::new(vec![Consumer::default()])),
        task_queue: Arc::new(RwLock::new(TaskQueue::default())),
    };

    tsp.run().await;
}
