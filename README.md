Please note that this is a simplified, single-threaded MapReduce implementation.

In general, there will only be three threads running concurrently when you start the program
- **Coordinator**, which in this case is registered as a RPC server, control the whole MapReduce process
- **Worker**, which will ask for and do the assigned MapReduce task from coordinator through RPC
- **Main Thread**, this is the main routine that will start the coordinator & worker threads, will hang until the MapReduce process is done

To start processing, follow the instructions:
- Please make sure you have `cargo` installed
- First enter the `src` directory, run `cd src`
- Then start the main routine by running `cargo run --bin mapreduce -- <map_n> <reduce_n>`