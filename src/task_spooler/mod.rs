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
use strum_macros::{Display, EnumString};
use serde::de::DeserializeOwned;
use std::fmt::{Debug};
use std::fs::{read_to_string, File};
use std::panic::resume_unwind;
use std::mem::replace;
use std::borrow::BorrowMut;
use std::ops::DerefMut;


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


trait Error : Send + Clone + Debug {
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceNotFoundError;

impl std::fmt::Display for ResourceNotFoundError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ResourceNotFoundError")
    }
}

impl Error for ResourceNotFoundError {
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

    pub fn args(&self, arguments: Vec<String>) -> Self {

        CommandPart {
            program: self.program.clone(),
            arguments: arguments,
        }
    }

    pub fn to_command(&self, resources: &Resources) -> Command {
        let mut com = Command::new(&self.program);
        let arguments: Vec<String> = self.arguments
            .clone()
            .into_iter()
            .map(|x| {
                if x.starts_with("$") {
                    let x = &x[1..];
                    for (k, v) in resources {
                        let ks = k.to_string();
                        if x.starts_with(&ks) {
                            let l = &x[ks.len()..];
                            if let Ok(l) = usize::from_str(l) {
                                return v[l].to_string();
                            }
                            else {
                                return v[0].to_string();
                            }
                        }
                    }
                }
                x
            }).collect()
        ;
        com.args(&arguments);
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
    pub error: Option<ResourceNotFoundError>,
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
            error: None,
        }
    }
    fn prepare(&mut self) {
        let mut tmp = tempfile::NamedTempFile::new_in("/tmp").expect("fail create tempfile for output");
        let (_, path) = tmp.keep().unwrap();
        self.output_filepath = Some(path);
    }

    async fn run(&mut self, resources: &Resources)  {
        if self.error.is_some() {
            return;
        }
        let mut command = self.command_part.to_command(resources);
        command.envs(resources_to_envs(resources));

        let f = File::create(&self.output_filepath.as_ref().unwrap()).unwrap();
        command.stdout(f);
        let status = command.status().await.unwrap();
        self.return_code = Some(status.code().unwrap());
    }

    pub async fn run_rwlock(self_: &RwLock<Option<Self>>, resources: &Resources) {
        self_.write().unwrap().as_mut().unwrap().prepare();

        let mut command = self_.read().unwrap().as_ref().unwrap().command_part.to_command(resources);
        command.envs(resources_to_envs(resources));

        let f = File::create(&self_.read().unwrap().as_ref().unwrap().output_filepath.as_ref().unwrap()).unwrap();
        command.stdout(f);
        let status = command.status().await.unwrap();
        self_.write().unwrap().as_mut().unwrap().return_code = Some(status.code().unwrap());
    }

    fn try_set_error<T>(&mut self, r: Result<T, ResourceNotFoundError>) -> Option<T> {
        if let Err(e) = r {
            self.error = Some(e);
            None
        } else {
            r.ok()
        }
    }
    fn with_return_code(&self, return_code: i32) -> Self {
        let mut ret = self.clone();
        ret.return_code = Some(return_code);
        ret
    }
}

fn resources_to_envs(resources: &Resources) -> HashMap<String, String> {
    let mut r = HashMap::new();
    for (k, v) in resources {
        if let Some(l) = v.first() {
            r.insert(k.to_string(), l.to_string());
        }
        for (i, l) in v.iter().enumerate() {
            r.insert(format!("{}{}", k, i), l.to_string());
        }
    }
    r
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

#[derive(Clone, Hash, Eq, PartialEq, Debug, Serialize, Deserialize, EnumString, Display)]
pub enum ResourceType {
    GPU,
    CPU,
}


pub struct Consumer {
    resources: HashMap<ResourceType, Vec<usize>>,
    processing: Arc<RwLock<Option<Task>>>,
}

impl Default for Consumer {
    fn default() -> Self {
        let mut resources = HashMap::new();

        resources.insert(ResourceType::CPU, vec![0]);

        Self {
            resources,
            processing: Arc::new(RwLock::new(None)),
        }
    }
}

impl Consumer {
    pub fn new(resources: Resources) -> Self {
        Self {
            resources,
            processing: Arc::new(RwLock::new(None)),
        }
    }
    async fn consume(&self, task_queue: &Arc<RwLock<TaskQueue>>) {
        loop {
            let mut task: Option<Task> = None;
            while task.is_none() {
                task = task_queue.write().unwrap().dequeue_with_constraints(&self.resources);
                delay_for(Duration::from_millis(10)).await;
            }
            let mut run_task = task.unwrap();
            self.processing.write().unwrap().replace(run_task);
            Task::run_rwlock(&self.processing, &self.resources).await;
            let task: Option<Task> = replace(self.processing.write().unwrap().deref_mut(), None);
            task_queue.write().unwrap().finished.push(task.unwrap());
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
    consumers: Arc<Vec<Consumer>>,
    pub task_queue: Arc<RwLock<TaskQueue>>,
}

#[derive(Display, Serialize, Deserialize)]
pub enum TaskStatus {
    Waiting,
    Processing,
    Finished,
}

impl Default for TaskSpooler {
    fn default() -> Self {
        TaskSpooler {
            consumers: Arc::new(vec![Consumer::default()]),
            task_queue: Arc::new(RwLock::new(TaskQueue::default())),
        }
    }
}

impl TaskSpooler {
    pub fn from_config(config: &config::Config) -> Self {
        // TODO(higumachan): ちゃんとパースエラーなどを上げられるようにする
        let consumers_config = config.get_array("consumers");
        let consumers = if consumers_config.is_ok() {
            let consumers_config = consumers_config.unwrap();
            let mut consumers = vec!();
            for c in consumers_config {
                let t = c.into_table().unwrap();
                let t = t.get("resources").unwrap().clone().into_table().unwrap();
                let resources = t.keys()
                    .map(|rname| ResourceType::from_str(rname.as_str()).unwrap())
                    .zip(t.values().map(|x| x.clone().into_array().unwrap().iter().map(|y| y.clone().into_int().unwrap() as usize).collect())).collect::<HashMap<_, _>>();
                consumers.push(Consumer::new(resources));
            }
            consumers
        } else {
            vec![Consumer::default()]
        };
        TaskSpooler {
            consumers: Arc::new(consumers),
            task_queue: Arc::new(RwLock::new(TaskQueue::default())),
        }
    }
    pub async fn run(&self) {
        join_all(self.consumers.iter().map(|x| x.consume(&self.task_queue))).await;
    }

    pub fn task_list(&self) -> Vec<(TaskStatus, Task)> {
        let mut tasks: Vec<(TaskStatus, Task)> = vec![];
        let processing: Vec<Task> = self.consumers.iter().filter_map(|x| x.processing.read().unwrap().clone()).collect();
        tasks.extend(processing.into_iter().map(|x| (TaskStatus::Processing, x)));
        tasks.extend(self.task_queue.read().unwrap().waiting
            .clone().into_iter().map(|x| (TaskStatus::Waiting, x)));
        tasks.extend(self.task_queue.read().unwrap().finished
            .clone().into_iter().map(|x| (TaskStatus::Finished, x)));
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
    use futures::StreamExt;
    use crate::server::run_server;

    impl Default for CommandPart
    {
        fn default() -> Self {
            CommandPart::new("ls")
        }
    }

    impl CommandPart
    {
        fn sleep(t: usize) -> Self {
            CommandPart::new("sleep").args(vec![t.to_string()])
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
                   *task_spooler.consumers[0].resources.get(&ResourceType::GPU).unwrap());
    }

    #[tokio::test]
    async fn test_dequeue_when_empty() {
        let mut consumer = Consumer::default();
        let tq = TaskQueue::default();

        let r = timeout(Duration::from_millis(10), consumer.consume(&Arc::new(RwLock::new(tq)))).await;

        assert!(r.is_err());
    }

    #[tokio::test]
    async fn test_timeout() {
        delay_for(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_task_run() {
        let cp = CommandPart::new("cargo").args(vec!(
            "run".to_string(), "--bin".to_string(), "helloworld".to_string()
        ));
        let result = run_command_with_task(cp, Box::new(|task, resources| {
            resources.insert(ResourceType::GPU, vec![1, 2]);
        })).await;

        assert_eq!("helloworld\n", result);
    }


    #[tokio::test]
    async fn test_task_run_with_env() {
        let cp = CommandPart::new("printenv");
        let result = run_command_with_task(cp, Box::new(|task, resources| {
            resources.insert(ResourceType::GPU, vec![1, 2]);
        })).await;

        assert!(result.contains("GPU=1"));
        assert!(result.contains("GPU0=1"));
        assert!(result.contains("GPU1=2"));

        let cp = CommandPart::new("echo").args(vec!["$GPU".to_string()]);
        let result = run_command_with_task(cp, Box::new(|task, resources| {
            resources.insert(ResourceType::GPU, vec![1, 2]);
        })).await;

        assert_eq!("1\n", result);
    }

    #[tokio::test]
    async fn test_consume_and_list() {
        let mut tsp = TaskSpooler::default();

        assert_eq!(tsp.task_list().len(), 0);
        let task1_id = tsp.task_queue.write().unwrap().enqueue(CommandPart::sleep(1), None, None);
        let task2_id = tsp.task_queue.write().unwrap().enqueue(CommandPart::sleep(10), None, None);
        assert_eq!(tsp.task_list().len(), 2);
        assert_eq!(tsp.task_list()[0].1.id, task1_id);
        assert_eq!(tsp.task_list()[1].1.id, task2_id);

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
                        let p = tsp.consumers[0].processing.read().unwrap();
                        assert!(p.as_ref().unwrap().output_filepath.is_some());
                        assert_eq!(tsp.task_queue.read().unwrap().waiting.len(), 1);
                        assert_eq!(tsp.task_queue.read().unwrap().finished.len(), 0);
                        assert_eq!(tsp.task_list().len(), 2);
                        assert_eq!(tsp.task_list()[0].1.id, task1_id);
                        assert_eq!(tsp.task_list()[1].1.id, task2_id);
                    })
                }
                _ = &mut f => {
                }
                _ = &mut delay2 => {
                    assert_eq!(tsp.task_queue.read().unwrap().waiting.len(), 0);
                    assert_eq!(tsp.task_queue.read().unwrap().finished.len(), 1);
                    assert_eq!(tsp.task_list().len(), 2);
                    assert_eq!(tsp.task_list()[0].1.id, task2_id);
                    assert_eq!(tsp.task_list()[1].1.id, task1_id);
                    break;
                }
            }
        }
    }

    async fn run_command_with_task(command: CommandPart, f: Box<dyn FnOnce(&mut Task, &mut Resources)>) -> String {
        let cp = command;
        let mut task = Task::new(
            1,
            cp,
            1,
            ResourceRequirements::new(),
        );
        let mut resources = Resources::new();

        f(&mut task, &mut resources);

        task.prepare();
        task.run(&resources).await;

        assert_eq!(task.return_code.unwrap(), 0);
        assert!(task.output_filepath.is_some());
        let output_filepath = task.output_filepath.unwrap();
        let mut file = File::open(output_filepath).unwrap();
        let mut buf = [0; 1024];
        let n = file.read(&mut buf).unwrap();
        from_utf8(&buf[0..n]).unwrap().to_string()
    }
}
