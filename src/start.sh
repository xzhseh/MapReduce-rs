# First build the `Worker` & `Coordinator` through cargo
cargo build
# Then start the `Coordinator` first
echo "Start the Coordinator"
nohup cargo run --bin mrcoordinator -- 8 10 3 > coordinator.nohup &
# Sleep for 1 second, to allow the preparation of Coordinator
sleep 1
# Start three workers
echo "Start the first Worker"
nohup cargo run --bin mrworker -- 8 10 > worker1.nohup &
echo "Start the second Worker"
nohup cargo run --bin mrworker -- 8 10 > worker2.nohup &
echo "Start the third Worker"
nohup cargo run --bin mrworker -- 8 10 > worker3.nohup &