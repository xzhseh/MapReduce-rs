use super::function::wc::{map, reduce};

#[derive(Debug, Clone)]
pub struct KeyValue {
    key: String,
    value: String,
}

impl KeyValue {
    pub fn new(key: String, value: String) -> Self {
        Self { key, value }
    }
}

pub struct Worker {
    /// The current state, `false` represents `map phase`, `true` represents `reduce phase`
    state: bool,
    /// The map task id, indicating which input files to read & map, will be -1 if the current job finished
    map_task_id: i32,
    /// The reduce task id, indicating which intermediate files to read & reduce, will be -1 if the current job finished
    reduce_task_id: i32,
    /// The total reduce tasks, used to generate intermediate files (Usually with hash function)
    reduce_n: i32,
    /// The user-defined map function
    map_func: Box<dyn Fn(&str) -> Vec<KeyValue>>,
    /// The user-defined reduce function
    reduce_func: Box<dyn Fn(&str, Vec<&str>) -> String>,
}

impl Worker {
    pub fn new(reduce_n: i32) -> Self {
        Self {
            // The initial state should be false
            state: false,
            map_task_id: -1,
            reduce_task_id: -1,
            reduce_n,
            // We will just use word count here
            map_func: Box::new(map),
            reduce_func: Box::new(reduce),
        }
    }

    pub fn get_state(&self) -> bool {
        self.state
    }

    pub fn get_map_id(&self) -> i32 {
        self.map_task_id
    }

    pub fn get_reduce_id(&self) -> i32 {
        self.reduce_task_id
    }

    /// Do the current job in map phase
    pub async fn map(&self) {

    }
}