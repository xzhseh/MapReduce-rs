use std::{fs::File, io::Read, collections::hash_map::DefaultHasher, hash::{Hash, Hasher}};

use tokio::io::AsyncWriteExt;

use crate::mr::function::wc;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct KeyValue {
    key: String,
    value: String,
}

impl KeyValue {
    pub fn new(key: String, value: String) -> Self {
        Self { key, value }
    }
}

/// One worker will only be touched by one worker process, there is no need to synchronize anything
/// We are thus lock-free!
pub struct Worker {
    /// The current state, `false` represents `map phase`, `true` represents `reduce phase`
    state: bool,
    /// The map task id, indicating which input files to read & map, will be -1 if the current job finished
    map_task_id: i32,
    /// The reduce task id, indicating which intermediate files to read & reduce, will be -1 if the current job finished
    reduce_task_id: i32,
    /// The total map tasks, used to read intermediate files
    map_n: i32,
    /// The total reduce tasks, used to generate intermediate files (Usually with hash function)
    reduce_n: i32,
}

/// Calls the user-defined map function
pub fn call_map_func(map_func: Box<dyn Fn(&str) -> Vec<KeyValue> + Send>, contents: &str) -> Vec<KeyValue> {
    map_func(contents)
}

/// Calls the user-defined reduce function
pub fn call_reduce_func(
        reduce_func: Box<dyn Fn(&str, Vec<&str>) -> String + Send>,
        key: &str,
        value: Vec<&str>) -> String {
    reduce_func(key, value)
}

impl Worker {
    pub fn new(map_n: i32, reduce_n: i32) -> Self {
        Self {
            // The initial state should be false
            state: false,
            map_task_id: -1,
            reduce_task_id: -1,
            map_n,
            reduce_n,
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
        self.map_task_id = map_task_id;
    }

    pub fn get_reduce_id(&self) -> i32 {
        self.reduce_task_id
    }

    pub fn set_reduce_id(&mut self, reduce_task_id: i32) {
        self.reduce_task_id = reduce_task_id;
    }

    fn read_file_to_mem_map(&self) -> String {
        let file_name = "pg-".to_string() + &self.map_task_id.to_string() + ".txt";
        println!(
            "[Map] Worker is reading input file {} for map task #{}",
            file_name,
            self.map_task_id
        );
        let mut file = File::open(file_name).unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
        contents
    }

    fn read_file_to_mem_reduce(&self) -> Vec<KeyValue> {
        // The intermediate files to read is from `mr-0-{reduce_task_id}.txt` to `mr-{map_n - 1}-{reduce_task_id}.txt`
        // The output files should be `mr-{reduce_task_id}.txt`
        let mut key_value_vec = Vec::new();

        for i in 0..self.map_n {
            let file_name = "mr-".to_string() + &i.to_string() + "-" + &self.reduce_task_id.to_string() + ".txt";
            println!(
                "[Reduce] Worker is reading intermediate file {} for reduce task #{}",
                file_name,
                self.reduce_task_id
            );
            let mut file = File::open(file_name).unwrap();
            let mut contents = String::new();
            file.read_to_string(&mut contents).unwrap();
            // Process the contents line by line
            let mut key_value_pairs = contents
                .split("\n")
                .filter(|x| !x.is_empty())
                .map(|x| {
                    let line = x.split(" ").collect::<Vec<&str>>();
                    assert!(line.len() == 2);
                    let (key, value) = (line[0], line[1]);
                    KeyValue::new(key.to_owned(), value.to_owned())
                }).collect::<Vec<KeyValue>>();
            // Append the newly generated key-value pairs to the result vector
            key_value_vec.append(&mut key_value_pairs);
        }

        key_value_vec
    }

    fn cal_hash_for_key(key: &str) -> u64 {
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        s.finish()
    }

    async fn write_key_value_to_file(&self, key_value_pairs: Vec<KeyValue>) -> anyhow::Result<bool> {
        let mut file_vec = Vec::new();
        let mut file_name_vec = Vec::new();
        for i in 0..self.reduce_n {
            let file_name= "mr-".to_string() + &self.map_task_id.to_string() + "-" + &i.to_string() + ".txt";
            let file = tokio::fs::File::create(file_name.clone()).await?;
            file_vec.push(file);
            file_name_vec.push(file_name);
        }

        for kv in key_value_pairs {
            let (key, value) = (kv.key, kv.value);
            let index = ((Self::cal_hash_for_key(&key)) % self.reduce_n as u64) as i32;
            // Sanity check
            assert!(index >= 0 && index < self.reduce_n);
            // Append the current key-value pair to the intermediate file asynchronously
            file_vec[index as usize].write_all(format!("{} {}\n", key, value).as_bytes()).await?;
            println!(
                "[Map] Worker finish mapping task #{}, the intermediate result has been written to {}",
                self.map_task_id,
                file_name_vec[index as usize]
            );
        }

        Ok(true)
    }

    /// Do the current job in map phase
    pub async fn map(&mut self) -> anyhow::Result<bool> {
        // The current state of worker must be in map phase
        assert!(!self.get_state());
        // The task id must not be -1
        assert!(self.map_task_id != -1);
        // Let's read the file into memory
        let contents = self.read_file_to_mem_map();
        // Then get the key-value pairs that we'd like to map to intermediate files
        let key_value_pairs = call_map_func(
            Box::new(wc::map),
            &contents
        );
        // Write the key-value pairs to the intermediate files according to the index (hash(key) % reduce_n)
        assert!(self.write_key_value_to_file(key_value_pairs).await?);
        // Finish the current map task, set the task id back to -1
        self.set_map_id(-1);
        Ok(true)
    }

    /// Do the current job in reduce phase
    pub async fn reduce(&mut self) -> anyhow::Result<bool> {
        // The current state of worker must be in reduce phase
        assert!(self.get_state());
        // The task id must not be -1
        assert!((self.map_task_id == -1) && (self.reduce_task_id != -1));
        // Let's read the intermediate files into memory
        let mut key_value_contents = self.read_file_to_mem_reduce();
        // Sort the key-value pairs based on key
        key_value_contents.sort_by(|lhs, rhs| {
            lhs.key.cmp(&rhs.key)
        });
        // Traverse the key-value pairs for the actual reduce phase
        let mut kv_vec = Vec::new();
        let mut prev = String::new();
        let file_name = "mr-".to_string() + &self.reduce_task_id.to_string() + ".txt";
        let mut file = tokio::fs::File::create(file_name.clone()).await?;
        for kv in &key_value_contents {
            if prev.is_empty() {
                prev = kv.key.clone();
            }
            if kv.key != prev {
                // Let's reduce!
                let reduce_result = call_reduce_func(
                    Box::new(wc::reduce),
                    &prev,
                    kv_vec.clone()
                );
                // The end of the collection for one key, need to write the result to output file
                file.write_all(format!("{} {}\n", prev, reduce_result).as_bytes()).await?;
                // Clear the kv vector
                kv_vec.clear();
                // Update prev
                prev = kv.key.clone();
            }
            kv_vec.push(&kv.value);
        }
        println!(
            "[Reduce] Worker finish reducing task #{}, the final output has been written to {}",
            self.reduce_task_id,
            file_name
        );
        // Finish the current reduce task, set the task id back to -1
        self.set_reduce_id(-1);
        Ok(true)
    }
}