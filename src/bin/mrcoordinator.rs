use std::{env, sync::{Arc, Mutex}, time::Duration};

use anyhow::Ok;
use map_reduce_rs::mr::{coordinator::*, worker::Worker};
use tarpc::{server::{self, Channel}, client, context};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = env::args().collect::<Vec<String>>();
    if args.len() != 4 {
        // Note here the `input file number` is number of files to read for each map task
        // Which is the `map_n` in `Coordinator`
        // The input file will start from `pg-0.txt` to `pg-{0 + map_n - 1}.txt`
        println!("Usage: cargo run mrcoordinator -- <input files number> <reduce task number> <worker number>");
        return Ok(());
    }

    let (map_n, reduce_n, _worker_num) = (args[1].parse::<i32>()?, args[2].parse::<i32>()?, args[3].parse::<i32>()?);

    // Create a new Coordinator
    let coordinator = Arc::new(Mutex::new(Coordinator::new(map_n, reduce_n)));

    // Create a clone for RPC server
    let coordinator_clone = Arc::clone(&coordinator);

    // Prepare for the RPC server configuration
    let (client_transport, server_transport) = tarpc::transport::channel::unbounded();

    // Launch the RPC server
    let rpc_server = server::BaseChannel::with_defaults(server_transport);
    tokio::spawn(rpc_server.execute(coordinator_clone.lock().unwrap().clone().serve()));

    // Launch the worker threads

    // Basically, the worker threads will only do two things in general
    // 1. If there is a not yet finished job, no matter `map` or `reduce`, just do it
    // 2. If the previously assigned job has been finished, ask the coordinator for a new job
    //  2.1. If the current state is map phase and there is no available map job, wait til the beginning of reduce phase
    //  2.2. If the current state is reduce phase and there is no available reduce job, simply exit
    tokio::spawn(async move {
        // First create a client
        let client = ServerClient::new(client::Config::default(), client_transport).spawn();
        // Then let's create a worker
        let mut worker = Worker::new(map_n, reduce_n);
        loop {
            let cur_state = worker.get_state();
            match cur_state {
                false => {
                    // In map phase
                    assert!(worker.get_map_id() == -1);
                    // Ask the coordinator for a new map task id
                    let map_task_id = client.get_map_task(context::current()).await.unwrap();
                    if map_task_id == -1 {
                        // There is no more available map task
                        // Let's prepare for the reduce phase
                        worker.change_state();
                        // Let's sleep for a while, waiting the coordinator to change the above state
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    // Otherwise, let's do the map job!
                    worker.set_map_id(map_task_id);
                    // Assert the map succeeds
                    assert!(worker.map().await.unwrap());
                    // Report to the coordinator
                    assert!(client.report_map_task_finish(context::current(), map_task_id).await.unwrap());
                }
                true => {
                    // In reduce phase
                    assert!(worker.get_map_id() == -1 && worker.get_reduce_id() == -1);
                    // Ask the coordinator for a new reduce task id
                    let reduce_task_id = client.get_reduce_task(context::current()).await.unwrap();
                    if reduce_task_id == -1 {
                        // Meaning the MapReduce is at an end, this worker can thus safely exit
                        return;
                    }
                    // Otherwise, let's do the reduce job!
                    worker.set_reduce_id(reduce_task_id);
                    assert!(worker.reduce().await.unwrap());
                    // Report to the coordinator
                    assert!(client.report_reduce_task_finish(context::current(), reduce_task_id).await.unwrap());
                }
            }
        }
    });

    while !coordinator.lock().unwrap().done() {
        // If not finished, sleep for a while in the main thread
        sleep(Duration::from_secs(1)).await;
    }

    println!(
        "\nThe MapReduce process has finished, please check the results at `mr-*.txt`\n{}\n{}",
        "You could run `make clean` to clean the generated intermediate files",
        "To aggregate the MapReduce results, run `make generate`, this will sort and generate `final.txt`"
    );

    Ok(())
}