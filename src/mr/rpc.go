package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

const (
	SeekingTask = "Hi, Boss! Give me a task"
	NotifyMapFinished = "Map Task Completed"
	NotifyReduceFinished = "Reduce Task Completed"
)

type SeekingTaskArgs struct{
	Msg string
	File_name string // to nofity the coordinator for the completion of map task
	Reduce_task_idx int
}

type ReplyingTaskArgs struct{
	Err_msg string
	File string				// input file for map
	X int					// map task number or Reduce Task number
	NReduce int
	Task string				// kind of task map or reduce
	Intermediate_file_names []string           // intermediate file locations for reduce workers
}

type StoreIntermediate struct{
	Intermediate_file_name string
	ReduceNum int
}
// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
