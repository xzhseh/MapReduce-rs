use std::{env, sync::{Arc, Mutex}, time::Duration};

use anyhow::Ok;
use map_reduce_rs::mr::{coordinator::*, worker::Worker};
use tarpc::{server::{self, Channel}, client, context};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> anyhow::Result<()>{
    let args = env::args().collect::<Vec<String>>();
    if args.len() != 4 {
        // Note here the `input file number` is number of files to read for each map task
        // Which is the `map_n` in `Coordinator`
        // The input file will start from `pg-0.txt` to `pg-{0 + map_n - 1}.txt`
        println!("Usage: cargo run mrcoordinator -- <input files number> <reduce task number> <worker number>");
        return Ok(());
    }

    let (map_n, reduce_n, worker_num) = (args[1].parse::<i32>()?, args[2].parse::<i32>()?, args[3].parse::<i32>()?);

    // Create a new Coordinator
    let coordinator = Arc::new(Mutex::new(Coordinator::new(map_n, reduce_n)));

    // Create a clone for RPC server
    let coordinator_clone = Arc::clone(&coordinator);

    // Prepare for the RPC server configuration
    let (client_transport, server_transport) = tarpc::transport::channel::unbounded();

    // Launch the RPC server
    let rpc_server = server::BaseChannel::with_defaults(server_transport);
    tokio::spawn(rpc_server.execute(coordinator_clone.lock().unwrap().clone().serve()));


    // let mut client = ServerClient::new(client::Config::default(), client_transport).spawn();

    // let map_id = client.get_map_task(context::current()).await?;

    // Basically, the worker threads will only do two things in general
    // 1. If there is a not yet finished job, no matter `map` or `reduce`, just do it
    // 2. If the previously assigned job has been finished, ask the coordinator for a new job
    //  2.1. If the current state is map phase and there is no available map job, wait til the beginning of reduce phase
    //  2.2. If the current state is reduce phase and there is no available reduce job, simply exit
    let mut t_vec = Vec::new();
    for _ in 0..worker_num as usize {
        t_vec.push(tokio::spawn(async move {
            // Let's first create a worker
            let mut worker = Worker::new(reduce_n);
            loop {
                
            }
        }));
    }

    while !coordinator.lock().unwrap().done() {
        // If not finished, sleep for a while in the main thread
        sleep(Duration::from_secs(1)).await;
    }

    println!("The MapReduce process has finished, please check the results at `mr-*.txt`");

    Ok(())
}