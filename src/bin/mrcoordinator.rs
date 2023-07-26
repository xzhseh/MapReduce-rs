use std::{env, sync::{Arc, Mutex}, time::Duration, net::SocketAddr};

use anyhow::Ok;
use futures::StreamExt;
use map_reduce_rs::mr::coordinator::*;
use tarpc::{server::incoming::Incoming, tokio_serde::formats::Json};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = env::args().collect::<Vec<String>>();
    if args.len() != 4 {
        // Note here the `input file number` is number of files to read for each map task
        // Which is the `map_n` in `Coordinator`
        // The input file will start from `pg-0.txt` to `pg-{0 + map_n - 1}.txt`
        println!("Usage: cargo run --bin mrcoordinator -- <input files number> <reduce task number> <worker number>");
        return Ok(());
    }

    let (map_n, reduce_n, worker_n) = (args[1].parse::<i32>()?, args[2].parse::<i32>()?, args[3].parse::<i32>()?);

    println!(
        "[Coordinator Configuration] #{} Map Tasks | #{} Reduce Tasks | #{} Worker Processes",
        map_n,
        reduce_n,
        worker_n
    );

    // Create a new Coordinator
    let coordinator = Arc::new(Mutex::new(Coordinator::new(map_n, reduce_n, worker_n)));

    // Create a clone for RPC server
    let coordinator_clone = Arc::clone(&coordinator);

    // Prepare for the RPC server configuration
    let server_address = "127.0.0.1:1030".parse::<SocketAddr>().unwrap();
    let server_transport = tarpc::serde_transport::tcp::listen(
        server_address, 
        Json::default
    ).await?;

    tokio::spawn(
        server_transport
            // Accepts if this is a valid connection, otherwise ignores this connection
            .filter_map(|r| async { r.ok() })
            .map(tarpc::server::BaseChannel::with_defaults)
            .execute(coordinator_clone.lock().unwrap().clone().serve())
    );

    println!("[Preparation] The Coordinator RPC server has launched and is currently serving, please launch #{} worker process(es) to begin MapReduce", worker_n);

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