use std::{sync::{Arc, Mutex}, collections::HashMap};

use futures::future::{Ready, ready};
use tarpc::context;

#[derive(Debug, Clone)]
pub struct Coordinator {
    /// Here `true` indicates the map task has finished, `false` indicates the map task is running
    map_tasks: Arc<Mutex<HashMap<i32, bool>>>,
    /// The global unique map task id, which starts from 0
    map_id: Arc<Mutex<i32>>,
    /// Same as `map_tasks`
    reduce_tasks: Arc<Mutex<HashMap<i32, bool>>>,
    /// The global unique reduce task id, which starts from 0
    reduce_id: Arc<Mutex<i32>>,
    /// The number of input files, which is also the number of map tasks
    map_n: i32,
    /// The number of reduce tasks
    reduce_n: i32,
}

impl Coordinator {
    /// Create a new coordinator
    pub fn new(map_n: i32, reduce_n: i32) -> Self {
        Self {
            map_tasks: Arc::new(Mutex::new(HashMap::new())),
            map_id: Arc::new(Mutex::new(0)),
            reduce_tasks: Arc::new(Mutex::new(HashMap::new())),
            reduce_id: Arc::new(Mutex::new(0)),
            map_n,
            reduce_n,
        }
    }

    /// Check if the overall MapReduce process has finished
    pub fn done(&self) -> bool {
        let cur_reduce_tasks = self.reduce_tasks.lock().unwrap();
        let cur_reduce_id = self.reduce_id.lock().unwrap();
        // The length of reduce_tasks map should not be 0
        (*cur_reduce_id == self.reduce_n) &&
        // If any of the flag is not `true`, means the overall MapReduce 
        cur_reduce_tasks
            .iter()
            .any(|(_, flag)| *flag != true)
    }
}

/// RPC related for Coordinator
#[tarpc::service]
pub trait Server {
    /// Get corresponding map task
    async fn get_map_task() -> i32;
    /// Get corresponding reduce task
    /// Note that reduce phase won't begin until all map tasks have finished
    async fn get_reduce_task() -> i32;
    /// Report map task has finished
    async fn report_map_task_finish(id: i32) -> bool;
    /// Report reduce task has finished
    async fn report_reduce_task_finish(id: i32) -> bool;
}

/// Register the four RPC functions on Coordinator, which is also the RPC server
impl Server for Coordinator {
    type GetMapTaskFut = Ready<i32>;
    type GetReduceTaskFut = Ready<i32>;
    type ReportMapTaskFinishFut = Ready<bool>;
    type ReportReduceTaskFinishFut = Ready<bool>;

    /// The worker will call this during map phase through RPC, to get a map task id, represents a input text file
    fn get_map_task(self, _: context::Context) -> Self::GetMapTaskFut {
        let mut cur_map_id = self.map_id.lock().unwrap();
        if *cur_map_id == self.map_n {
            // No more map tasks are available
            return ready(-1);
        }
        let mut cur_map_tasks = self.map_tasks.lock().unwrap();
        cur_map_tasks.insert(*cur_map_id, false);
        let ret = ready(*cur_map_id);
        // Increase the map task id by one
        *cur_map_id += 1;
        // Return the map task id
        ret
    }

    /// The worker will call this during reduce phase through RPC, to get a reduce task id, represents a output file
    fn get_reduce_task(self, _:context::Context) -> Self::GetReduceTaskFut {
        let mut cur_reduce_id = self.reduce_id.lock().unwrap();
        if *cur_reduce_id == self.reduce_n {
            // No more reduce tasks are available
            return ready(-1);
        }
        let mut cur_reduce_tasks = self.reduce_tasks.lock().unwrap();
        cur_reduce_tasks.insert(*cur_reduce_id, false);
        let ret = ready(*cur_reduce_id);
        // Increase the reduce task id by one
        *cur_reduce_id += 1;
        // Return the reduce task id
        ret
    }

    /// The worker will call this when finishing the map task
    fn report_map_task_finish(self, _:context::Context, id: i32) -> Self::ReportMapTaskFinishFut {
        let mut cur_map_tasks = self.map_tasks.lock().unwrap();
        assert!(cur_map_tasks.contains_key(&id) && *cur_map_tasks.get(&id).unwrap() == false);
        // Set the value to `true`, indicating the finish of the map task
        cur_map_tasks.insert(id, true);
        ready(true)
    }

    /// The worker will call this when finishing the reduce task
    fn report_reduce_task_finish(self, _:context::Context, id: i32) -> Self::ReportReduceTaskFinishFut {
        let mut cur_reduce_tasks = self.reduce_tasks.lock().unwrap();
        assert!(cur_reduce_tasks.contains_key(&id) && *cur_reduce_tasks.get(&id).unwrap() == false);
        // Set the value to `true`, indicating the finish of the reduce task
        cur_reduce_tasks.insert(id, true);
        ready(true)
    }
}