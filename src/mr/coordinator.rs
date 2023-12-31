use std::{sync::{Arc, Mutex}, collections::{HashMap, HashSet}, time::Duration, io::{Read, Write}};

use futures::future::{Ready, ready};
use tarpc::context;
use tokio::time::Instant;

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
    /// The number of worker processes
    worker_n: i32,
    /// Indicates if the map phase has finished
    map_finish: Arc<Mutex<bool>>,
    /// Indicates if the reduce phase has finished
    reduce_finish: Arc<Mutex<bool>>,
    /// The global unique worker id, will assign to each worker through RPC, starts from 0 to {worker_n - 1}
    worker_id: Arc<Mutex<i32>>,
    /// The map lease, used to track the map tasks granted to workers (Will be checked every 5 seconds by default)
    map_leases: Arc<Mutex<HashMap<i32, Instant>>>,
    /// The reduce lease, used to track the reduce tasks granted to workers (The time period is the same with above)
    reduce_leases: Arc<Mutex<HashMap<i32, Instant>>>,
    /// The name of logging directory, containing all the logs
    log_dir_name: String,
    /// The file name of the underlying write-ahead-log for coordinator inside log directory
    wal_name: String,
}

impl Coordinator {
    /// Create a new coordinator
    pub fn new(map_n: i32, reduce_n: i32, worker_n: i32) -> Self {
        Self {
            map_tasks: Arc::new(Mutex::new(HashMap::new())),
            map_id: Arc::new(Mutex::new(0)),
            reduce_tasks: Arc::new(Mutex::new(HashMap::new())),
            reduce_id: Arc::new(Mutex::new(0)),
            map_n,
            reduce_n,
            worker_n,
            map_finish: Arc::new(Mutex::new(false)),
            reduce_finish: Arc::new(Mutex::new(false)),
            worker_id: Arc::new(Mutex::new(0)),
            map_leases: Arc::new(Mutex::new(HashMap::new())),
            reduce_leases: Arc::new(Mutex::new(HashMap::new())),
            log_dir_name: String::from("log"),
            wal_name: String::from("coordinator.wal"),
        }
    }

    /// Check if the specified number of worker processes have connected
    /// The MapReduce won't start until all worker processes are assigned a worker id
    pub fn prepare(&self) -> bool {
        *self.worker_id.lock().unwrap() == self.worker_n
    }

    /// Check if the overall MapReduce process has finished
    pub fn done(&self) -> bool {
        *self.map_finish.lock().unwrap() && *self.reduce_finish.lock().unwrap()
    }

    /// Check the current lease based on the state, reset the task status if the task has staled
    pub fn check_lease(&mut self) -> bool {
        // We should all the locks at first, since the intermediate state may change between the period
        let _map_id = self.map_id.lock().unwrap();
        let _reduce_id = self.reduce_id.lock().unwrap();
        let _worker_id = self.worker_id.lock().unwrap();
        // The resources that will be used later
        let mut map_tasks = self.map_tasks.lock().unwrap();
        let mut reduce_tasks = self.reduce_tasks.lock().unwrap();
        let mut map_leases = self.map_leases.lock().unwrap();
        let mut reduce_leases = self.reduce_leases.lock().unwrap();
        let reduce_finish = self.reduce_finish.lock().unwrap();
        let map_finish = self.map_finish.lock().unwrap();

        if *map_finish && *reduce_finish {
            // The MapReduce has finished, nothing to check
            return true;
        }

        if *map_finish {
            // The MapReduce should be in the reduce phase
            println!("[Check Lease] The MapReduce is in reduce phase, begin to check reduce tasks leases");
            // Sanity check
            assert!(!*reduce_finish);
            // Check every reduce task lease, get the outdated ones
            let stale_reduce_tasks = reduce_leases
                .iter()
                // If the lease has not been updated for 5 seconds, mark it as stale
                .filter(|(_, time)| time.elapsed() >= Duration::new(5, 0))
                .map(|x| *x.0)
                .collect::<HashSet<i32>>();
            // Update the corresponding reduce task map and refresh the reduce lease
            for stale_id in &stale_reduce_tasks {
                assert!(reduce_tasks.get(stale_id).unwrap());
                println!("[Check Lease] Staled reduce task #{} detected, mark it as staled", stale_id);
                reduce_tasks.insert(*stale_id, false);
                reduce_leases.remove_entry(stale_id);
            }
            return true;
        }

        // Then the MapReduce must in the map phase
        assert!(!*map_finish && !*reduce_finish);
        println!("[Check Lease] The MapReduce is in map phase, begin to check map tasks leases");
        // Check every map task lease, get the outdated ones
        let stale_map_tasks = map_leases
            .iter()
            // If the lease has not been updated for 5 seconds, mark it as stale
            .filter(|(_, time)| time.elapsed() >= Duration::new(5, 0))
            .map(|x| *x.0)
            .collect::<HashSet<i32>>();
        // Update the corresponding reduce task map and refresh the reduce lease
        for stale_id in &stale_map_tasks {
            assert!(map_tasks.get(stale_id).unwrap());
            println!("[Check Lease] Staled map task #{} detected, mark it as staled", stale_id);
            map_tasks.insert(*stale_id, false);
            map_leases.remove_entry(stale_id);
        }

        true
    }

    /// This function will serialize the current status of the coordinator to the underlying Write-Ahead-Log
    fn serialize(&self) -> bool {
        // Hold the lock of all the resources that need to be serialized at first
        let map_id = self.map_id.lock().unwrap();
        let reduce_id = self.reduce_id.lock().unwrap();
        let map_tasks = self.map_tasks.lock().unwrap();
        let reduce_tasks = self.reduce_tasks.lock().unwrap();
        let map_leases = self.map_leases.lock().unwrap();
        let reduce_leases = self.reduce_leases.lock().unwrap();
        let reduce_finish = self.reduce_finish.lock().unwrap();
        let map_finish = self.map_finish.lock().unwrap();

        println!("[Serialize] Serializing the status of the current coordinator to `coordinator.wal`");

        if let Ok(mut wal_log) = std::fs::File::create(self.log_dir_name.clone() + "/" + &self.wal_name) {
            wal_log.write_all(format!("BEGIN\n").as_bytes()).unwrap();

            // The length of map tasks
            wal_log.write_all(format!("{}\n", map_tasks.len()).as_bytes()).unwrap();
            // The individual tasks
            for (&k, &v) in &*map_tasks {
                wal_log.write_all(format!("{} {}\n", k, v).as_bytes()).unwrap();
            }

            // The global unique map id
            wal_log.write_all(format!("{}\n", *map_id).as_bytes()).unwrap();

            // The length of reduce tasks
            wal_log.write_all(format!("{}\n", reduce_tasks.len()).as_bytes()).unwrap();
            // The individual tasks
            for (&k, &v) in &*reduce_tasks {
                wal_log.write_all(format!("{} {}\n", k, v).as_bytes()).unwrap();
            }

            // The global unique reduce id
            wal_log.write_all(format!("{}\n", *reduce_id).as_bytes()).unwrap();

            // `map_finish`
            wal_log.write_all(format!("{}\n", *map_finish).as_bytes()).unwrap();

            // `reduce_finish`
            wal_log.write_all(format!("{}\n", *reduce_finish).as_bytes()).unwrap();

            // The length of map leases
            wal_log.write_all(format!("{}\n", map_leases.len()).as_bytes()).unwrap();
            // The individual leases
            // FIXME: Now only recording the map id
            for (&k, _) in &*map_leases {
                wal_log.write_all(format!("{}\n", k).as_bytes()).unwrap();
            }

            // The length of reduce leases
            wal_log.write_all(format!("{}\n", reduce_leases.len()).as_bytes()).unwrap();
            // The individual leases
            // FIXME: Now only recording the map id
            for (&k, _) in &*reduce_leases {
                wal_log.write_all(format!("{}\n", k).as_bytes()).unwrap();
            }

            wal_log.write_all(format!("END\n").as_bytes()).unwrap();
        } else {
            println!("[Serialize] Failed to open `coordinator.wal`");
            return false;
        }
        // Successfully serialized to the underlying Write-Ahead-Log
        true
    }

    /// This function will deserialize the status of coordinator when recovering
    fn deserialize(&mut self, _wal_vec: Vec<&str>) -> bool {
        true
    }

    pub fn recover(&mut self) -> bool {
        // First check if there is existed `Write-Ahead-Log` inside the log directory
        println!("[Recovery] Check if there is `coordinator.wal` existed");
        if let Ok(mut wal_log) = std::fs::File::open(self.log_dir_name.clone() + "/" + &self.wal_name) {
            let mut wal_contents = String::new();
            wal_log.read_to_string(&mut wal_contents).unwrap();
            let wal_vec = wal_contents
                .split("\n")
                .filter(|x| !x.is_empty())
                .collect::<Vec<&str>>();
            // Begin the actual recover process by process the latest MapReduce status
            assert!(self.deserialize(wal_vec));
        } else {
            println!("[Recovery] Found no `coordinator.wal`, starts the coordinator in normal mode");
            return false;
        }
        // Successfully recover from the underlying Write-Ahead-Log
        true
    }
}

/// RPC related for Coordinator
#[tarpc::service]
pub trait Server {
    /// Get the corresponding map task
    async fn get_map_task() -> i32;
    /// Get the corresponding reduce task
    /// Note that reduce phase won't begin until all map tasks have finished
    async fn get_reduce_task() -> i32;
    /// Get the corresponding worker id
    async fn get_worker_id() -> i32;
    /// Report map task has finished
    async fn report_map_task_finish(id: i32) -> bool;
    /// Report reduce task has finished
    async fn report_reduce_task_finish(id: i32) -> bool;
    /// Renew the current map task lease
    async fn renew_map_lease(id: i32) -> bool;
    /// Renew the current reduce task lease
    async fn renew_reduce_lease(id: i32) -> bool;
}

/// Register the four RPC functions on Coordinator, which is also the RPC server
#[tarpc::server]
impl Server for Coordinator {
    type GetMapTaskFut = Ready<i32>;
    type GetReduceTaskFut = Ready<i32>;
    type GetWorkerIdFut = Ready<i32>;
    type ReportMapTaskFinishFut = Ready<bool>;
    type ReportReduceTaskFinishFut = Ready<bool>;
    type RenewMapLeaseFut = Ready<bool>;
    type RenewReduceLeaseFut = Ready<bool>;

    /// The worker will call this every 1 second to renew the current map task lease
    fn renew_map_lease(self, _: context::Context, id: i32) -> Self::RenewMapLeaseFut {
        let mut map_lease = self.map_leases.lock().unwrap();
        // Sanity check
        assert!(map_lease.contains_key(&id));
        // Renew the map lease
        map_lease.insert(id, Instant::now());
        ready(true)
    }

    /// The worker will call this every 1 second to renew the current reduce task lease
    fn renew_reduce_lease(self, _: context::Context, id: i32) -> Self::RenewReduceLeaseFut {
        let mut reduce_lease = self.reduce_leases.lock().unwrap();
        // Sanity check
        assert!(reduce_lease.contains_key(&id));
        // Renew the reduce lease
        reduce_lease.insert(id, Instant::now());
        ready(true)
    }

    /// The worker will call this during map phase through RPC, to get a map task id, represents a input text file
    fn get_map_task(self, _: context::Context) -> Self::GetMapTaskFut {
        // First lock the resources
        let mut cur_map_id = self.map_id.lock().unwrap();
        let mut cur_map_tasks = self.map_tasks.lock().unwrap();
        let mut cur_map_leases = self.map_leases.lock().unwrap();

        if !self.prepare() {
            // This indicates the worker that the preparation phase hasn't ended
            return ready(-2);
        }

        if *cur_map_id == self.map_n || *self.map_finish.lock().unwrap() {
            // Check if every task is properly holding by a single worker
            // FIXME: This may lead to infinite map phase, if the worker crash after being assigned the last map task
            // Since the other worker may already turn into reduce phase
            // One way to fix is to notify the reduce phase worker to change state back to map to finish the stale task
            // But this solution is not so elegant and we must hard-coded some magic number to return to the worker
            // When the `get_reduce_task` is called, so...
            for (&k, &v) in &cur_map_tasks.clone() {
                if v {
                    continue;
                }
                println!("[Map] Staled map task #{} detected, the previous worker may have gone offline, assigned this task to a new worker", k);
                // Otherwise, there is staled task, assign this task to the worker
                // Also update the status
                cur_map_tasks.insert(k, true);
                // Sanity check
                assert!(!cur_map_leases.contains_key(&k));
                // Update the lease
                cur_map_leases.insert(k, Instant::now());
                return ready(k);
            }
            if !cur_map_leases.is_empty() {
                // Should wait to check if there are more stale tasks
                return ready(-3);
            }
            // No more map tasks are available
            return ready(-1);
        }

        // Otherwise, this should be the normal process
        cur_map_tasks.insert(*cur_map_id, true);
        // Insert the new lease
        cur_map_leases.insert(*cur_map_id, Instant::now());
        let cur_map = *cur_map_id;
        let ret = ready(cur_map);
        // Increase the global unique map task id by one
        *cur_map_id += 1;
        println!("[Map] Assigned map task #{} to worker", cur_map);
        if cur_map + 1 == self.map_n {
            println!("[Map] All available map tasks have been assigned to worker, wait til all worker processes finish the map phase");
        }
        // Return the map task id
        ret
    }

    /// The worker will call this during reduce phase through RPC, to get a reduce task id, represents a output file
    fn get_reduce_task(self, _: context::Context) -> Self::GetReduceTaskFut {
        // First lock the resources
        let mut cur_reduce_id = self.reduce_id.lock().unwrap();
        let mut cur_reduce_tasks = self.reduce_tasks.lock().unwrap();
        let mut cur_reduce_leases = self.reduce_leases.lock().unwrap();

        if !*self.map_finish.lock().unwrap() {
            // The map phase has not yet finished
            return ready(-2);
        }

        if *cur_reduce_id == self.reduce_n || *self.reduce_finish.lock().unwrap() {
            // FIXME: Same as `get_map_tasks`...
            for (&k, &v) in &cur_reduce_tasks.clone() {
                if v {
                    continue;
                }
                println!("[Reduce] Staled reduce task #{} detected, the previous worker may have gone offline, assigned this task to a new worker", k);
                // Otherwise, there is staled task, assign this task to the worker
                // Also update the status
                cur_reduce_tasks.insert(k, true);
                // Sanity check
                assert!(!cur_reduce_leases.contains_key(&k));
                // Update the lease
                cur_reduce_leases.insert(k, Instant::now());
                return ready(k);
            }
            if !cur_reduce_leases.is_empty() {
                // Same as `get_map_tasks`
                return ready(-3);
            }
            // No more reduce tasks are available
            return ready(-1);
        }

        // Otherwise, this should be the normal process
        cur_reduce_tasks.insert(*cur_reduce_id, true);
        // Insert the new lease
        cur_reduce_leases.insert(*cur_reduce_id, Instant::now());
        let cur_reduce = *cur_reduce_id;
        let ret = ready(cur_reduce);
        // Increase the global unique reduce task id by one
        *cur_reduce_id += 1;
        println!("[Reduce] Assigned reduce task #{} to worker", cur_reduce);
        if cur_reduce + 1 == self.reduce_n {
            println!("[Reduce] All available reduce tasks have been assigned to worker, wait til all worker processes finish the reduce phase");
        }
        // Return the reduce task id
        ret
    }

    /// The worker will call this function first when connecting, to get a unique worker process identifier
    fn get_worker_id(self, _: context::Context) -> Self::GetWorkerIdFut {
        let mut cur_worker_id = self.worker_id.lock().unwrap();
        let cur_num = *cur_worker_id;
        // If the number of worker processes exceeds the preset limit, the server will panic
        assert!(cur_num != self.worker_n);
        let left_num = self.worker_n - cur_num - 1;
        let ret = ready(cur_num);
        *cur_worker_id += 1;
        println!("[Preparation] Worker #{} connected, #{} more worker(s) needed!", cur_num, left_num);
        if cur_num + 1 == self.worker_n {
            println!("[Preparation] All worker processes have connected, Map Phase will then begin!");
        }
        ret
    }

    /// The worker will call this when finishing the map task
    fn report_map_task_finish(self, _: context::Context, id: i32) -> Self::ReportMapTaskFinishFut {
        // Serialize the current status before each report
        println!("[Map] Begin serialize the Coordinator");
        self.serialize();
        let cur_map_tasks = self.map_tasks.lock().unwrap();
        let mut cur_map_leases = self.map_leases.lock().unwrap();
        // Sanity check
        assert!(cur_map_tasks.contains_key(&id) && *cur_map_tasks.get(&id).unwrap() == true);
        assert!(cur_map_leases.contains_key(&id));
        println!("[Map] Map task #{} has been finished", id);
        // Remove the lease, since the task has been finished
        cur_map_leases.remove_entry(&id);

        // No need to do the following since the semantic of the map has changed
        // Set the value to `true`, indicating the finish of the map task
        // cur_map_tasks.insert(id, true);

        // First let's check if there is staled map task
        // FIXME: Same as `get_map_tasks`
        for (&k, &v) in &cur_map_tasks.clone() {
            if v {
                continue;
            }
            println!("[Map] Staled map task #{} detected when reporting, the previous worker may have gone offline, will assigned this task to a new worker", k);
            return ready(true);
        }

        if !cur_map_leases.is_empty() {
            println!("[Map] The map lease is not empty, there's still unfinished map tasks");
            return ready(true);
        }

        let mut map_finish = self.map_finish.lock().unwrap();
        let map_id = self.map_id.lock().unwrap();

        if *map_id == self.map_n {
            // Otherwise, it's safe to set the `map_finish` to true
            *map_finish = true;
            println!("[Map] All map tasks have been finished by worker processes, the reduce phase will then begin!");
        }

        ready(true)
    }

    /// The worker will call this when finishing the reduce task
    fn report_reduce_task_finish(self, _: context::Context, id: i32) -> Self::ReportReduceTaskFinishFut {
        // Serialize the current status before each report
        println!("[Reduce] Begin serialize the Coordinator");
        self.serialize();
        let cur_reduce_tasks = self.reduce_tasks.lock().unwrap();
        let mut cur_reduce_leases = self.reduce_leases.lock().unwrap();
        // Sanity check
        assert!(cur_reduce_tasks.contains_key(&id) && *cur_reduce_tasks.get(&id).unwrap() == true);
        assert!(cur_reduce_leases.contains_key(&id));
        println!("[Reduce] Reduce task #{} has been finished", id);
        // Remove the lease, since the task has been finished
        cur_reduce_leases.remove_entry(&id);

        // No need to do the following since the semantic of the map has changed
        // Set the value to `true`, indicating the finish of the reduce task
        // cur_reduce_tasks.insert(id, true);

        // First let's check if there is staled reduce task
        // FIXME: Same as `get_map_tasks`
        for (&k, &v) in &cur_reduce_tasks.clone() {
            if v {
                continue;
            }
            println!("[Reduce] Staled reduce task #{} detected when reporting, the previous worker may have gone offline, will assigned this task to a new worker", k);
            return ready(true);
        }

        if !cur_reduce_leases.is_empty() {
            println!("[Reduce] The reduce lease is not empty, there's still unfinished reduce tasks");
            return ready(true);
        }

        let mut reduce_finish = self.reduce_finish.lock().unwrap();
        let reduce_id = self.reduce_id.lock().unwrap();
        
        if *reduce_id == self.reduce_n {
            // Otherwise, it's safe to set the `reduce_finish` to true
            *reduce_finish = true;
            println!("[Reduce] All reduce tasks have been finished by worker processes, MapReduce has finished!");
        }

        ready(true)
    }
}