use std::{net::SocketAddr, env, time::Duration};

use map_reduce_rs::mr::{coordinator::ServerClient, worker::Worker};
use tarpc::{tokio_serde::formats::Json, client, context};
use tokio::time::sleep;

/// Basically, the worker processes will only do two things in general
/// 1. If there is a not yet finished job, no matter `map` or `reduce`, just do it
/// 2. If the previously assigned job has been finished, ask the coordinator for a new job
///  2.1. If the current state is map phase and there is no available map job, wait til the beginning of reduce phase
///  2.2. If the current state is reduce phase and there is no available reduce job, simply exit
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = env::args().collect::<Vec<String>>();
    if args.len() != 3 {
        // Note here the `input file number` is number of files to read for each map task
        // Which is the `map_n` in `Coordinator`
        // The input file will start from `pg-0.txt` to `pg-{0 + map_n - 1}.txt`
        println!("Usage: cargo run --bin mrworker -- <input files number> <reduce task number>");
        return Ok(());
    }

    let (map_n, reduce_n) = (args[1].parse::<i32>()?, args[2].parse::<i32>()?);

    println!("[Worker Configuration] #{} Map Tasks | #{} Reduce Tasks", map_n, reduce_n);

    // The server address, you'll want to substitute this with your own configuration
    let server_address = "127.0.0.1:1030".parse::<SocketAddr>().unwrap();

    // Connect to the server
    let client_transport = match tarpc::serde_transport::tcp::connect(server_address, Json::default).await {
        Ok(t) => t,
        Err(e) => {
            println!(
                "[Preparation] Worker failed to connect to the RPC server, please check the Coordinator status!\n{}{}",
                "Error Message: ",
                e
            );
            return Ok(());
        }
    };

    let client = ServerClient::new(client::Config::default(), client_transport).spawn();

    // First get the worker global unique id from the server
    let worker_id = client.get_worker_id(context::current()).await?;
    println!("[Preparation] Get worker id #{} from server", worker_id);

    // Let's create a worker
    let mut worker = Worker::new(map_n, reduce_n);

    // Then let's start the worker logic
    loop {
        let cur_state = worker.get_state();
        match cur_state {
            false => {
                // In map phase
                assert!(worker.get_map_id() == -1);
                // Ask the coordinator for a new map task id
                let map_task_id = client.get_map_task(context::current()).await?;
                if map_task_id == -2 {
                    // Still in preparation phase
                    // Just go to sleep
                    println!("[Preparation] There is no enough worker process to start the MapReduce, go to sleep");
                    // Sleep for a while
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                if map_task_id == -1 {
                    // There is no more available map task
                    // Let's prepare for the reduce phase
                    println!("[Map] No available map tasks at present, change the state to reduce and go to sleep");
                    worker.change_state();
                    // Let's sleep for a while, waiting the coordinator to change the above state
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                // Otherwise, let's do the map job!
                worker.set_map_id(map_task_id);
                // Assert the map succeeds
                assert!(worker.map().await?);
                // Report to the coordinator
                assert!(client.report_map_task_finish(context::current(), map_task_id).await?);
            }
            true => {
                // In reduce phase
                assert!(worker.get_map_id() == -1 && worker.get_reduce_id() == -1);
                // Ask the coordinator for a new reduce task id
                let reduce_task_id = client.get_reduce_task(context::current()).await?;
                if reduce_task_id == -2 {
                    // The reduce phase has not yet started, go back to sleep
                    println!("[Reduce] The reduce phase has not yet started due to unfinished map tasks, go to sleep");
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                if reduce_task_id == -1 {
                    // Meaning the MapReduce is at an end, this worker can thus safely exit
                    println!("[Reduce] No available reduce tasks at present, this worker process will thus terminate\nWish you a good day :)");
                    return Ok(());
                }
                // Otherwise, let's do the reduce job!
                worker.set_reduce_id(reduce_task_id);
                assert!(worker.reduce().await?);
                // Report to the coordinator
                assert!(client.report_reduce_task_finish(context::current(), reduce_task_id).await?);
            }
        }
    }
}