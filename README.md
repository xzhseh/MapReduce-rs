### For the single-threaded MapReduce implementation, please check the `single-thread` branch.

This is a simplified, easy-to-learn MapReduce framework written in Rust.
- The user runtime is using `tokio`, an asynchronous runtime for Rust, make it efficient under I/O-Bound scenario like MapReduce
- The RPC framework is using `tarpc`, nodes will communicate through `TCP` connection
- You may start the MapReduce process with **one** `Coordinator` & **multiple** `Worker(s)`, the number of worker processes can be specified by users
- The default application is a word count application, which will use the provided `pg-*.txt` files to count the total words and the corresponding frequency
- You can define your own map reduce function in `src/mr/function.rs` & `src/mr/worker.rs`, see the comment for details
- You may want to change the `server_address` to adapt to your environment, check `src/bin/mrcoordinator.rs` & `src/bin/mrworker.rs` for details

To run the program, follow the instructions:
- Please make sure you are in the `src` directory, if not (Assumes you are in the root directory), run `cd src`
- You can start the `Coordinator` by running `cargo run --bin mrcoordinator <map tasks number> <reduce tasks number> <worker number>`
- Then start one or more `Worker(s)` in **different** terminal windows by running `cargo run --bin mrworker <map tasks number> <reduce tasks number>`, please make sure the server has been started before you starts the `Worker`
- The number of `Worker(s)` should exactly match with the specified number when you started the server, otherwise the MapReduce process either will hang (The `Worker(s)` is not enough) or will panic (The `Worker(s)` exceed the preset limit)
- Check the final result by running `make generate`, feel free to change anything in the provided `Makefile`

TODO:
- Fault tolerance mechanism
- Utilize `tokio` in a more efficient way
- Improve logging method, add different logging level
- Parallel processing for each worker
- Better serialization & deserialization format especially for intermediate files