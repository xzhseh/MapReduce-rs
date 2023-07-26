use std::{fs::File, io::Read, collections::hash_map::DefaultHasher, hash::{Hash, Hasher}};

use tokio::io::AsyncWriteExt;

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

    /// Change the current state from `map` to `reduce`
    /// Can only be used in map phase
    pub fn change_state(&mut self) {
        assert!(!self.state);
        self.state = true;
    }

    pub fn get_map_id(&self) -> i32 {
        self.map_task_id
    }

    pub fn set_map_id(&mut self, map_task_id: i32) {
        assert!(self.map_task_id == -1);
        self.map_task_id = map_task_id;
    }

    pub fn get_reduce_id(&self) -> i32 {
        self.reduce_task_id
    }

    pub fn set_reduce_id(&mut self, reduce_task_id: i32) {
        assert!(self.reduce_task_id == -1);
        self.reduce_task_id = reduce_task_id;
    }

    fn read_file_to_mem(&self) -> String {
        let file_name = "pg-".to_string() + &self.map_task_id.to_string() + ".txt";
        let mut file = File::open(file_name).unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
        contents
    }

    fn cal_hash_for_key(key: &str) -> u64 {
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        s.finish()
    }

    async fn write_key_value_to_file(&self, key_value_pairs: Vec<KeyValue>) -> anyhow::Result<bool> {
        let mut file_vec = Vec::new();
        for i in 0..self.reduce_n {
            let file_name= "mr-".to_string() + &self.map_task_id.to_string() + "-" + &i.to_string() + ".txt";
            let file = tokio::fs::File::open(file_name).await?;
            file_vec.push(file);
        }

        for kv in key_value_pairs {
            let (key, value) = (kv.key, kv.value);
            let index = (Self::cal_hash_for_key(&key) as i32) % self.reduce_n;
            // Sanity check
            assert!(index >= 0 && index < self.reduce_n);
            // Append the current key-value pair to the intermediate file asynchronously
            file_vec[index as usize].write_all(format!("{} {}\n", key, value).as_bytes()).await?;
        }

        Ok(true)
    }

    /// Do the current job in map phase
    pub async fn map(&self) -> anyhow::Result<()> {
        // The current state of worker must be in map phase
        assert!(!self.state);
        // The task id must not be -1
        assert!(self.map_task_id != -1);
        // Let's read the file into the memory
        let contents = self.read_file_to_mem();
        // Then get the key-value pairs that we'd like to map to intermediate files
        let key_value_pairs = (self.map_func)(&contents);
        // Write the key-value pairs to the intermediate files according to the index (hash(key) % reduce_n)
        assert!(self.write_key_value_to_file(key_value_pairs).await?);
        Ok(())
    }
}