use tokio::process::Command;
use futures::future::*;
use std::sync::RwLock;
use std::sync::{Arc};
use std::collections::{HashMap};
use serde::{Deserialize, Serialize};
use tokio::time::{delay_for};
use std::time::Duration;


pub type Resources = HashMap<ResourceType, Vec<usize>>;
pub type ResourceRequirements = HashMap<ResourceType, usize>;

trait ResourceRequirementsExt {
    fn is_satisfy(&self, resources: &Resources) -> bool;
}

impl ResourceRequirementsExt for ResourceRequirements {
    fn is_satisfy(&self, resources: &Resources) -> bool {
        self.iter()
            .all(|(k, v)| { resources.get(k).map_or(0usize, |x| x.len()) >= *v })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CommandPart {
    program: String,
    arguments: Vec<String>,
}

impl CommandPart {
    pub fn new(program: &str) -> Self {
        CommandPart {
            program: program.to_string(),
            arguments: vec![],
        }
    }

    pub fn args(&self, arguments: &Vec<String>) -> Self {
        CommandPart {
            program: self.program.clone(),
            arguments: arguments.clone(),
        }
    }

    fn to_command(&self) -> Command {
        let mut com = Command::new(&self.program);
        com.args(&self.arguments);
        com
    }
}

#[derive(Clone, Debug)]
struct Task {
    id: usize,
    return_code: Option<i32>,
    requirements: ResourceRequirements,
    priority: i64,
    command_part: CommandPart,
}

impl Task {
    fn new(id: usize, command_part: CommandPart, priority: i64, requirements: ResourceRequirements) -> Self {
        Self {
            id,
            return_code: Option::None,
            requirements,
            priority,
            command_part,
        }
    }
    fn with_return_code(&self, return_code: i32) -> Self {
        Self {
            id: self.id,
            return_code: Some(return_code),
            priority: self.priority,
            command_part: self.command_part.clone(),
            requirements: self.requirements.clone(),
        }
    }
}


#[derive(Clone)]
pub struct TaskQueue {
    waiting: Vec<Task>,
    finished: Vec<Task>,
    next_task_id: usize,
}

impl Default for TaskQueue {
    fn default() -> Self {
        TaskQueue {
            waiting: vec![],
            finished: vec![],
            next_task_id: 0,
        }
    }
}

impl TaskQueue {
    pub fn enqueue(&mut self, command_part: CommandPart, priority: Option<i64>, requirements: Option<ResourceRequirements>) -> usize {
        let priority = priority.unwrap_or(self.next_default_priority());
        let requirements = requirements.unwrap_or(HashMap::new());

        let id = self.allocate_task_id();
        let task = Task::new(id, command_part, priority, requirements);
        self.waiting.push(task);
        id
    }
    fn dequeue_with_constraints(&mut self, consumer: &Consumer) -> Option<Task> {
        let mut waiting_with_index: Vec<(usize, &Task)> = self.waiting.iter().enumerate().collect();  // TODO(higumachan): いつか直す
        waiting_with_index.sort_by_key(|(_, v)| -v.priority);
        let target_index = waiting_with_index.iter().filter(|(_, task)| { task.requirements.is_satisfy(&consumer.resources) }).next()?.0;
        let r = self.waiting.remove(target_index);
        Some(r)
    }

    fn next_default_priority(&self) -> i64 {
        0
    }

    fn allocate_task_id(&mut self) -> usize {
        let r = self.next_task_id;
        self.next_task_id += 1;
        r
    }
}

#[derive(Clone, Hash, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum ResourceType {
    GPU,
    CPU,
}

pub struct Consumer {
    resources: HashMap<ResourceType, Vec<usize>>,
}

impl Default for Consumer {
    fn default() -> Self {
        let mut resources = HashMap::new();

        resources.insert(ResourceType::CPU, vec![0]);

        Self {
            resources,
        }
    }
}

impl Consumer {
    async fn consume(&self, task_queue: &Arc<RwLock<TaskQueue>>) {
        loop {
            let mut task: Option<Task> = None;
            while task.is_none() {
                task = task_queue.write().unwrap().dequeue_with_constraints(self);
                delay_for(Duration::from_millis(100)).await;
            }
            let task = task.unwrap();
            let mut command = task.command_part.to_command();
            println!("start: {} {}", task.command_part.program, task.command_part.arguments.join(" "));
            let status = command.status().await.expect("fail child command");

            let task = task.with_return_code(status.code().unwrap());
            task_queue.write().unwrap().finished.push(task);
        }
    }

    fn with_resource(&self, resource_type: ResourceType, amount: Vec<usize>) -> Self {
        let mut resources = self.resources.clone();
        resources.insert(resource_type, amount);
        Self {
            resources
        }
    }
}

#[derive(Clone)]
pub struct TaskSpooler {
    consumers: Arc<RwLock<Vec<Consumer>>>,
    pub task_queue: Arc<RwLock<TaskQueue>>,
}

impl Default for TaskSpooler {
    fn default() -> Self {
        TaskSpooler {
            consumers: Arc::new(RwLock::new(vec![Consumer::default()])),
            task_queue: Arc::new(RwLock::new(TaskQueue::default())),
        }
    }
}

impl TaskSpooler {
    pub async fn run(&self) {
        let consumers = self.consumers.write().unwrap();
        join_all(consumers.iter().map(|x| x.consume(&self.task_queue))).await;
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;
    use std::time::Duration;


    impl Default for CommandPart {
        fn default() -> Self {
            CommandPart::new("ls")
        }
    }

    #[test]
    fn test_dequeue_with_constraints() {
        let consumer = Consumer::default();
        let consumer_with_gpu = Consumer::default().with_resource(ResourceType::GPU, vec![0]);

        let mut tq = TaskQueue::default();
        let task1_id = tq.enqueue(CommandPart::default(), None, Some([
            (ResourceType::GPU, 1),
        ].iter().cloned().collect()));
        let task2_id = tq.enqueue(CommandPart::default(), None, None);

        let dq_task = tq.clone().dequeue_with_constraints(&consumer).unwrap();
        assert_eq!(dq_task.id, task2_id);
        let dq_task = tq.clone().dequeue_with_constraints(&consumer_with_gpu).unwrap();
        assert_eq!(dq_task.id, task1_id);
    }

    #[test]
    fn test_dequeue_with_priority() {
        let consumer = Consumer::default();

        let mut tq = TaskQueue::default();
        let task1_id = tq.enqueue(CommandPart::default(), Some(1), None);
        let task2_id = tq.enqueue(CommandPart::default(), Some(10), None);
        let task3_id = tq.enqueue(CommandPart::default(), Some(1), None);

        let dq_task = tq.dequeue_with_constraints(&consumer).unwrap();
        assert_eq!(dq_task.id, task2_id);
        let dq_task = tq.dequeue_with_constraints(&consumer).unwrap();
        assert_eq!(dq_task.id, task1_id);
        let dq_task = tq.dequeue_with_constraints(&consumer).unwrap();
        assert_eq!(dq_task.id, task3_id);
    }

    #[tokio::test]
    async fn test_dequeue_when_empty() {
        let consumer = Consumer::default();
        let tq = TaskQueue::default();

        let r = timeout(Duration::from_millis(10), consumer.consume(&Arc::new(RwLock::new(tq)))).await;

        assert!(r.is_err());
    }

    #[tokio::test]
    async fn test_timeout() {

        delay_for(Duration::from_millis(100)).await;
    }
}
