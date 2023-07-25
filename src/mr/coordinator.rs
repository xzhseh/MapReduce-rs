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
}

impl Coordinator {
    /// Create a new coordinator
    pub fn new() -> Self {
        Self {
            map_tasks: Arc::new(Mutex::new(HashMap::new())),
            map_id: Arc::new(Mutex::new(0)),
            reduce_tasks: Arc::new(Mutex::new(HashMap::new())),
            reduce_id: Arc::new(Mutex::new(0)),
        }
    }

    /// Check if the overall MapReduce process has finished
    pub fn done() -> bool {
        false
    }
}

#[tarpc::service]
pub trait Server {
    /// Get corresponding map task
    async fn get_map_task() -> i32;
    /// Get corresponding reduce task
    /// Note that reduce task won't begin until all map tasks have finished
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

    /// The worker will call this during map phase, to get a map task id, represents a input text file
    fn get_map_task(self, _: context::Context) -> Self::GetMapTaskFut {
        let mut cur_id = self.map_id.lock().unwrap();
        let ret = ready(*cur_id);
        // Increase the map task id by one
        *cur_id += 1;
        // Return the map task id
        ret
    }

    fn get_reduce_task(self, _:context::Context) -> Self::GetReduceTaskFut {
        
    }

    fn report_map_task_finish(self, _:context::Context, id:i32) -> Self::ReportMapTaskFinishFut {
        
    }

    fn report_reduce_task_finish(self, _:context::Context, id:i32) -> Self::ReportReduceTaskFinishFut {
        
    }
}