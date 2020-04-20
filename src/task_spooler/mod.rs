use tokio::process::Command;
use futures::future::*;
use std::sync::RwLock;
use std::sync::{Arc};
use std::collections::{HashMap};
use serde::{Deserialize, Serialize};
use tokio::time::{delay_for};
use std::time::Duration;
use std::path::{Path, PathBuf};
use std::io::Write;
use serde::export::Formatter;
use std::str::FromStr;
use std::string::ParseError;


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

impl std::fmt::Display for CommandPart {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.program, self.arguments.join(" "))
    }
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Task {
    pub id: usize,
    pub return_code: Option<i32>,
    pub requirements: ResourceRequirements,
    pub priority: i64,
    pub command_part: CommandPart,
    pub output_filepath: Option<PathBuf>,
}

impl Task {
    fn new(id: usize, command_part: CommandPart, priority: i64, requirements: ResourceRequirements) -> Self {
        Self {
            id,
            return_code: Option::None,
            requirements,
            priority,
            command_part,
            output_filepath: None,
        }
    }
    async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>>  {
        let mut command = self.command_part.to_command();
        let mut tmp = tempfile::NamedTempFile::new_in("/tmp").expect("fail create tempfile for output");
        self.output_filepath = Some(tmp.path().to_path_buf());
        let f = tmp.reopen().unwrap();
        command.stdout(f);
        let status = command.status().await?;
        self.return_code = Some(status.code().unwrap());
        let filename = PathBuf::from(format!("/tmp/tsp_{}.log", self.id));
        self.output_filepath = Some(filename.clone());
        tmp.persist(filename);
        Result::Ok(())
    }
    fn with_return_code(&self, return_code: i32) -> Self {
        Self {
            id: self.id,
            return_code: Some(return_code),
            priority: self.priority,
            command_part: self.command_part.clone(),
            requirements: self.requirements.clone(),
            output_filepath: None,
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
    fn dequeue_with_constraints(&mut self, resources: &Resources) -> Option<Task> {
        let mut waiting_with_index: Vec<(usize, &Task)> = self.waiting.iter().enumerate().collect();  // TODO(higumachan): いつか直す
        waiting_with_index.sort_by_key(|(_, v)| -v.priority);
        let target_index = waiting_with_index.iter().filter(|(_, task)| { task.requirements.is_satisfy(resources) }).next()?.0;
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

#[derive(Debug)]
pub struct ResourceTypeParseError;

#[derive(Clone, Hash, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub enum ResourceType {
    GPU,
    CPU,
}

impl FromStr for ResourceType {
    type Err = ResourceTypeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        println!("{}", s);
        match s {
            "gpu" => Ok(ResourceType::GPU),
            "cpu" => Ok(ResourceType::CPU),
            _ => Err(ResourceTypeParseError{}),
        }
    }
}

impl std::fmt::Display for ResourceType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", match self {
            GPU => "gpu",
            CPU => "cpu",
        })
    }
}

pub struct Consumer {
    resources: HashMap<ResourceType, Vec<usize>>,
    processing: Option<Task>,
}

impl Default for Consumer {
    fn default() -> Self {
        let mut resources = HashMap::new();

        resources.insert(ResourceType::CPU, vec![0]);

        Self {
            resources,
            processing: None,
        }
    }
}

impl Consumer {
    pub fn new(resources: Resources) -> Self {
        Self {
            resources,
            processing: None,
        }
    }
    async fn consume(self_: &RwLock<Self>, task_queue: &Arc<RwLock<TaskQueue>>) {
        loop {
            let mut task: Option<Task> = None;
            while task.is_none() {
                task = task_queue.write().unwrap().dequeue_with_constraints(&self_.read().unwrap().resources);
                delay_for(Duration::from_millis(100)).await;
            }
            let mut run_task = task.clone().unwrap();
            self_.write().unwrap().processing = task;
            run_task.run().await.expect("fail child command");
            task_queue.write().unwrap().finished.push(run_task);
            self_.write().unwrap().processing = None;
        }
    }

    fn with_resource(&self, resource_type: ResourceType, amount: Vec<usize>) -> Self {
        let mut resources = self.resources.clone();
        resources.insert(resource_type, amount);
        Self {
            resources,
            processing: self.processing.clone(),
        }
    }
}

#[derive(Clone)]
pub struct TaskSpooler {
    consumers: Arc<Vec<RwLock<Consumer>>>,
    pub task_queue: Arc<RwLock<TaskQueue>>,
}

impl Default for TaskSpooler {
    fn default() -> Self {
        TaskSpooler {
            consumers: Arc::new(vec![RwLock::new(Consumer::default())]),
            task_queue: Arc::new(RwLock::new(TaskQueue::default())),
        }
    }
}

impl TaskSpooler {
    pub fn from_config(config: &config::Config) -> Self {
        let consumers_config = config.get_array("consumers").expect("not found consumers in config");
        println!("{:?}", consumers_config);
        let mut consumers = vec!();
        for c in consumers_config {
            let t = c.into_table().unwrap();
            let t = t.get("resources").unwrap().clone().into_table().unwrap();
            let resources = t.keys()
                .map(|rname| ResourceType::from_str(rname.as_str()).unwrap())
                .zip(t.values().map(|x| x.clone().into_array().unwrap().iter().map(|y| y.clone().into_int().unwrap() as usize).collect())).collect::<HashMap<_, _>>();
            consumers.push(RwLock::new(Consumer::new(resources)));
        }
        TaskSpooler {
            consumers: Arc::new(consumers),
            task_queue: Arc::new(RwLock::new(TaskQueue::default())),
        }
    }
    pub async fn run(&self) {
        join_all(self.consumers.iter().map(|x| Consumer::consume(x, &self.task_queue))).await;
    }

    pub fn task_list(&self) -> Vec<Task> {
        let mut tasks = vec![];
        let processing: Vec<Task> = self.consumers.iter().filter_map(|x| x.read().unwrap().processing.clone()).collect();
        tasks.extend(processing);
        tasks.extend(self.task_queue.read().unwrap().waiting.clone());
        tasks.extend(self.task_queue.read().unwrap().finished.clone());
        tasks
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;
    use std::time::Duration;
    use std::fs::File;
    use std::io::Read;
    use std::str::from_utf8;

    impl Default for CommandPart {
        fn default() -> Self {
            CommandPart::new("ls")
        }
    }

    impl CommandPart {
        fn sleep(t: usize) -> Self {
            CommandPart::new("sleep").args(&vec![t.to_string()])
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

        let dq_task = tq.clone().dequeue_with_constraints(&consumer.resources).unwrap();
        assert_eq!(dq_task.id, task2_id);
        let dq_task = tq.clone().dequeue_with_constraints(&consumer_with_gpu.resources).unwrap();
        assert_eq!(dq_task.id, task1_id);
    }

    #[test]
    fn test_dequeue_with_priority() {
        let consumer = Consumer::default();

        let mut tq = TaskQueue::default();
        let task1_id = tq.enqueue(CommandPart::default(), Some(1), None);
        let task2_id = tq.enqueue(CommandPart::default(), Some(10), None);
        let task3_id = tq.enqueue(CommandPart::default(), Some(1), None);

        let dq_task = tq.dequeue_with_constraints(&consumer.resources).unwrap();
        assert_eq!(dq_task.id, task2_id);
        let dq_task = tq.dequeue_with_constraints(&consumer.resources).unwrap();
        assert_eq!(dq_task.id, task1_id);
        let dq_task = tq.dequeue_with_constraints(&consumer.resources).unwrap();
        assert_eq!(dq_task.id, task3_id);
    }

    #[test]
    fn test_config_gpu2() {
        let mut config = config::Config::default();
        config.merge(config::File::with_name("./config/gpu2_example.yaml")).unwrap();
        let task_spooler = TaskSpooler::from_config(&config);

        assert_eq!(2, task_spooler.consumers.len());
        assert_eq!(vec![0usize],
                   *task_spooler.consumers[0].read().unwrap().resources.get(&ResourceType::GPU).unwrap());
    }

    #[tokio::test]
    async fn test_dequeue_when_empty() {
        let mut consumer = Consumer::default();
        let tq = TaskQueue::default();

        let r = timeout(Duration::from_millis(10), Consumer::consume(&RwLock::new(consumer), &Arc::new(RwLock::new(tq)))).await;

        assert!(r.is_err());
    }

    #[tokio::test]
    async fn test_timeout() {
        delay_for(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_task_run() {
        let mut task = Task::new(
            1,
            CommandPart::new("cargo").args(&vec!(
                "run".to_string(), "--bin".to_string(), "helloworld".to_string()
            )),
            1,
            ResourceRequirements::new(),
        );

        task.run().await.unwrap();

        assert_eq!(task.return_code.unwrap(), 0);
        assert!(task.output_filepath.is_some());
        let output_filepath = task.output_filepath.unwrap();
        let mut file = File::open(output_filepath).unwrap();
        let mut buf = [0; 1024];
        let n = file.read(&mut buf).unwrap();
        let result = from_utf8(&buf[0..n]).unwrap();
        assert_eq!("helloworld\n", result);
    }

    #[tokio::test]
    async fn test_consume_and_list() {
        let mut tsp = TaskSpooler::default();

        assert_eq!(tsp.task_list().len(), 0);
        let task1_id = tsp.task_queue.write().unwrap().enqueue(CommandPart::sleep(1), None, None);
        let task2_id = tsp.task_queue.write().unwrap().enqueue(CommandPart::sleep(10), None, None);
        assert_eq!(tsp.task_list().len(), 2);
        assert_eq!(tsp.task_list()[0].id, task1_id);
        assert_eq!(tsp.task_list()[1].id, task2_id);

        assert_eq!(tsp.task_queue.read().unwrap().waiting.len(), 2);
        assert_eq!(tsp.task_queue.read().unwrap().finished.len(), 0);
        assert_eq!(tsp.task_list().len(), 2);

        let mut delay1 = delay_for(Duration::from_millis(500));
        let mut delay2 = delay_for(Duration::from_millis(1500));
        let d1 = std::sync::Once::new();
        tokio::pin! {
            let f = tsp.run();
        }
        loop {
            tokio::select! {
                _ = &mut delay1 => {
                    d1.call_once(|| {
                        assert_eq!(tsp.task_queue.read().unwrap().waiting.len(), 1);
                        assert_eq!(tsp.task_queue.read().unwrap().finished.len(), 0);
                        assert_eq!(tsp.task_list().len(), 2);
                        assert_eq!(tsp.task_list()[0].id, task1_id);
                        assert_eq!(tsp.task_list()[1].id, task2_id);
                    })
                }
                _ = &mut f => {
                }
                _ = &mut delay2 => {
                    assert_eq!(tsp.task_queue.read().unwrap().waiting.len(), 0);
                    assert_eq!(tsp.task_queue.read().unwrap().finished.len(), 1);
                    assert_eq!(tsp.task_list().len(), 2);
                    assert_eq!(tsp.task_list()[0].id, task2_id);
                    assert_eq!(tsp.task_list()[1].id, task1_id);
                    break;
                }
            }
        }
    }
}
