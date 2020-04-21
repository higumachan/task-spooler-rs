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
use std::fs::read_to_string;


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

#[derive(Debug)]
struct ParseArgumentError;

impl std::fmt::Display for ParseArgumentError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ParseArgumentError")
    }
}

impl std::error::Error for ParseArgumentError {
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum Argument {
    Normal(String),
    Placeholder{resource_type: ResourceType, id: usize},
}

impl std::fmt::Display for Argument {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Normal(s) => write!(f, "{}", s),
            Self::Placeholder{ resource_type, id } => write!(f, "{}:{}", resource_type, id)
        }
    }
}


impl std::str::FromStr for Argument {
    type Err = Box<dyn std::error::Error>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(t) = s.chars().nth(0) {
            if t == '#' {
                let remain = &s[1..];
                let kv: Vec<_> = remain.split(":").collect();
                let resource_type = ResourceType::from_str(kv.get(0).ok_or(ParseArgumentError)?)?;
                let id = usize::from_str(kv.get(1).ok_or(ParseArgumentError)?)?;
                Ok(Self::Placeholder{resource_type, id})
            } else {
                Ok(Self::Normal(s.to_string()))
            }
        } else {
            Ok(Self::Normal(s.to_string()))
        }
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

impl Argument {
    fn replace_resource(&self, resources: &Resources) -> Result<String, ResourceNotFoundError> {
        match self {
            Self::Normal(s) => Ok(s.to_string()),
            Self::Placeholder{ resource_type, id } =>
                Ok(resources.get(resource_type).ok_or(ResourceNotFoundError{})?
                    .get(*id).ok_or(ResourceNotFoundError{})?.to_string())
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CommandPart<T> where T:
    Clone + Debug + Eq + PartialEq + std::fmt::Display + FromStr
 {
    program: String,
    arguments: Vec<T>,
}

impl<T> std::fmt::Display for CommandPart<T> where T:
    Clone + Debug + Eq + PartialEq + std::fmt::Display + FromStr
{
fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let arguments: Vec<String> = self.arguments.iter().map(|x| x.to_string()).collect();
        write!(f, "{} {}", self.program, arguments.join(" "))
    }
}

impl<T> CommandPart<T> where T:
    Clone + Debug + Eq + PartialEq + std::fmt::Display + FromStr
{
    pub fn new(program: &str) -> Self {
        CommandPart {
            program: program.to_string(),
            arguments: vec![],
        }
    }

    pub fn args(&self, arguments: &Vec<String>) -> Self where
        <T as std::str::FromStr>::Err : std::fmt::Debug {
        let arguments = arguments
            .clone()
            .iter()
            .map(|x| T::from_str(x).expect("parse arguments error"))
            .collect();

        CommandPart {
            program: self.program.clone(),
            arguments: arguments,
        }
    }

}

impl CommandPart<Argument> {
    fn to_string_command_part(&self, resources: &Resources) -> Result<CommandPart<String>, ResourceNotFoundError> {
        let mut com = CommandPart::new(&self.program);
        let args = self.arguments
            .iter()
            .map(|x| x.replace_resource(resources)).collect::<Result<Vec<_>, _>>()?;
        Ok(com.args(&args))
    }
}

impl CommandPart<String> {
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
    pub command_part_plan: CommandPart<Argument>,
    pub command_part_exec: Option<CommandPart<String>>,
    pub output_filepath: Option<PathBuf>,
    pub error: Option<ResourceNotFoundError>,
}

impl Task {
    fn new(id: usize, command_part: CommandPart<Argument>, priority: i64, requirements: ResourceRequirements) -> Self {
        Self {
            id,
            return_code: Option::None,
            requirements,
            priority,
            command_part_plan: command_part,
            command_part_exec: None,
            output_filepath: None,
            error: None,
        }
    }
    async fn run(&mut self, resources: &Resources)  {
        self.command_part_exec =
            self.try_set_error(self.command_part_plan.to_string_command_part(resources));
        if self.command_part_exec.is_none() {
            return;
        }
        let mut command = self.command_part_exec.as_ref().unwrap().to_command();

        let mut tmp = tempfile::NamedTempFile::new_in("/tmp").expect("fail create tempfile for output");
        self.output_filepath = Some(tmp.path().to_path_buf());
        let f = tmp.reopen().unwrap();
        command.stdout(f);
        let status = command.status().await.unwrap();
        self.return_code = Some(status.code().unwrap());
        let filename = PathBuf::from(format!("/tmp/tsp_{}.log", self.id));
        self.output_filepath = Some(filename.clone());
        tmp.persist(filename);
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
    pub fn enqueue(&mut self, command_part: CommandPart<Argument>, priority: Option<i64>, requirements: Option<ResourceRequirements>) -> usize {
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
            run_task.run(&self_.read().unwrap().resources.clone()).await;
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

#[derive(Display, Serialize, Deserialize)]
pub enum TaskStatus {
    Waiting,
    Processing,
    Finished,
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
                consumers.push(RwLock::new(Consumer::new(resources)));
            }
            consumers
        } else {
            vec![RwLock::new(Consumer::default())]
        };
        TaskSpooler {
            consumers: Arc::new(consumers),
            task_queue: Arc::new(RwLock::new(TaskQueue::default())),
        }
    }
    pub async fn run(&self) {
        join_all(self.consumers.iter().map(|x| Consumer::consume(x, &self.task_queue))).await;
    }

    pub fn task_list(&self) -> Vec<(TaskStatus, Task)> {
        let mut tasks: Vec<(TaskStatus, Task)> = vec![];
        let processing: Vec<Task> = self.consumers.iter().filter_map(|x| x.read().unwrap().processing.clone()).collect();
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

    impl Default for CommandPart<Argument>
    {
        fn default() -> Self {
            CommandPart::new("ls")
        }
    }

    impl CommandPart<Argument>
    {
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

    #[test]
    fn test_replace_argument() {
        let mut arguments = vec![
            Argument::Normal("test".to_string()),
            Argument::Placeholder {resource_type: ResourceType::GPU, id: 0},
            Argument::Placeholder {resource_type: ResourceType::GPU, id: 1},
        ];
        let resource1 = [(ResourceType::GPU, vec![0usize, 1usize])].iter().cloned().collect::<Resources>();
        let resource2 = [(ResourceType::GPU, vec![1usize, 2usize])].iter().cloned().collect::<Resources>();
        let resource3 = [].iter().cloned().collect::<Resources>();

        assert_eq!(arguments[0].replace_resource(&resource1).unwrap(), "test");
        assert_eq!(arguments[1].replace_resource(&resource1).unwrap(), "0");
        assert_eq!(arguments[2].replace_resource(&resource1).unwrap(), "1");

        assert_eq!(arguments[0].replace_resource(&resource2).unwrap(), "test");
        assert_eq!(arguments[1].replace_resource(&resource2).unwrap(), "1");
        assert_eq!(arguments[2].replace_resource(&resource2).unwrap(), "2");

        assert_eq!(arguments[0].replace_resource(&resource3).unwrap(), "test");
        assert!(arguments[1].replace_resource(&resource3).is_err());
        assert!(arguments[2].replace_resource(&resource3).is_err());
    }

    #[test]
    fn test_parse_arguments() {
        let s = "nadeko";
        assert_eq!(Argument::from_str(s).unwrap(), Argument::Normal("nadeko".to_string()));
        let s = "#GPU:0";
        assert_eq!(Argument::from_str(s).unwrap(), Argument::Placeholder{resource_type: ResourceType::GPU, id: 0});
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
        let cp = CommandPart::new("cargo").args(&vec!(
            "run".to_string(), "--bin".to_string(), "helloworld".to_string()
        ));
        let mut task = Task::new(
            1,
            cp,
            1,
            ResourceRequirements::new(),
        );

        let resources = Resources::new();
        task.run(&resources).await;

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
    async fn test_task_run_fail() {
        let cp = CommandPart::new("cargo").args(&vec!(
            "run".to_string(), "--bin".to_string(), "#GPU:20".to_string()
        ));
        let mut task = Task::new(
            1,
            cp,
            1,
            ResourceRequirements::new(),
        );

        let resources = Resources::new();
        task.run(&resources).await;
        assert!(task.error.is_some());
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
}
