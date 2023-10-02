Ideally, map reduce algorithm will run on a shared file system where all map functions and reduce functions put there intermediate files or final output files. Due to resource constraints, I implemented this in a PC where all threads and processes share a common file system. 

## Getting Started
This implemenation follows the implementation model (section 3.1) in the paper: MapReduce:Simplified Data Processing on Large Cluster, JeffreyDean and SanjayGhemawat

1. There will be only one coordinator(boss) that will supply tasks(map or reduce) to workers.
2. Worker will keep on asking for tasks until it gets notice from the coordiantor that there are no more tasks
3. Coordinator will give map task to workers until there are no more map tasks(create intermediate files) then it starts giving reducing tasks
4. Once a worker finishes its given map task, it will notify the coordinator by giving the intermediate file location it generated. Coordinator will store the location for intermediate file for reduce task. If coordinator has any map task, it will give it to the free worker. 
5. After map tasks are completed then the coordinator will give reduce tasks to the free workerss until the reduce tasks at the coordinator exhausts (processes all intermediate files)
6. In real world, crash of worker an happen due to many reasons. Some of them are workers did not get enough resource for processing leading to significant delay of communication between worker and coordinator, system may crash. To avoid coordinator waiting for long time for worker to respond, Coordinator will only wait for 10 seconds in this project and assign the same task to another free worker.
7. Note that coordinator will span a thread that will handle producing taskings using channels and the main coordinator routine will manage the tasks by ensuring the time given to a worker is 10 seconds by creating a routine for each worker that had got a task. As mentioned in the paper, there are 3 states for each task. They are 1. not-started 2. in-progress 3. Completed

## Run Your Code
Build your code using the wrappers inside the mrapps that will have specific map function and reduce function
```
rm -f mr-*
go build -buildmode=plugin ../mrapps/wc.go 
```
Start your coordinator in a terminal by using the following command
```
go run mrcoordinator.go pg-*.txt
```
Start multiple workers in different terminals with the following command
```
go run mrworker.go wc.so
```
Added a test script that will test multiple test case. Running the below command will show all tests passed
```
bash test-mr.sh | grep test
```