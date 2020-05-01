use serde::{Deserialize, Serialize};
use crate::task_spooler::{CommandPart, ResourceRequirements, Argument};

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
pub enum RequestType {
    Enqueue(CommandPart, Option<i64>, Option<ResourceRequirements>),
    UpdateConsumers(),
    ShowQueue(),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_type_serialize() {
        let rt = RequestType::Enqueue(CommandPart::default(), None, None);
        let serialized = bincode::serialize(&rt).unwrap();
        let deserialized = bincode::deserialize(&serialized).unwrap();

        assert_eq!(rt, deserialized);
    }
}