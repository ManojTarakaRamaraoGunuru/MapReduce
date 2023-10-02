package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "sync"
import "time"
import "strconv"


type Coordinator struct {
	// Your definitions here.
	input_files []string
	nReduce int
	map_task_num int
	intermediate_file_names [][]string

	input_file_map map[string]string
	reduce_files_map map[int]string
	all_maps_done bool
	all_reduces_done bool
	RWlock *sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

// cooordinator needs to span another routine that will supply map tasks a and reduce tasks

var map_tasks chan string 
var reduce_tasks chan int

func (c* Coordinator)TaskManager(args *SeekingTaskArgs, reply *ReplyingTaskArgs)error{
	switch (args.Msg) {
		case SeekingTask:
			select{
				case file_name := <- map_tasks:
					reply.File = file_name
					reply.X = c.map_task_num
					reply.NReduce = c.nReduce
					reply.Task = "map"	

					c.RWlock.Lock()
					c.map_task_num += 1
					fmt.Println(file_name, c.map_task_num)
					c.input_file_map[file_name] = "in-progress"
					c.RWlock.Unlock()
					go c.timer(file_name, reply.Task)
					return nil
				
				case files_idx := <- reduce_tasks:
					reply.Intermediate_file_names = c.intermediate_file_names[files_idx]
					reply.X = files_idx
					reply.Task = "reduce"

					c.RWlock.Lock()
					fmt.Println(files_idx)
					c.reduce_files_map[files_idx] = "in-progress"
					c.RWlock.Unlock()
					go c.timer(strconv.Itoa(files_idx), reply.Task)
					return nil
			}
		case NotifyMapFinished:
			c.RWlock.Lock()
			c.input_file_map[args.File_name] = "completed"
			c.RWlock.Unlock()
		case NotifyReduceFinished:
			c.RWlock.Lock()
			c.reduce_files_map[args.Reduce_task_idx] = "completed"
			c.RWlock.Unlock()
	}

	// fmt.Println("intermediate_file_names : ",c.intermediate_file_names)
	return nil
}

func (c *Coordinator)timer(file_name string, task string){
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for{
		select{
			case <- timer.C:
				if task == "map"{
					c.RWlock.Lock()
					c.input_file_map[file_name] = "not-started"
					c.RWlock.Unlock()
					map_tasks <- file_name
					return
				}else if task == "reduce"{
					file_idx, err := strconv.Atoi(file_name)
					if err != nil{
						fmt.Println("There is a string conversion issue")
					}
					c.RWlock.Lock()
					c.reduce_files_map[file_idx] = "not-started"
					c.RWlock.Unlock()
					reduce_tasks <- file_idx
					return
				}
			default:
				if task == "map"{
					c.RWlock.RLock()
					if c.input_file_map[file_name] == "completed"{
						c.RWlock.RUnlock()
						return
					}else{
						c.RWlock.RUnlock()
					}				
				}else if task == "reduce"{
					file_idx, err := strconv.Atoi(file_name)
					if err != nil{
						fmt.Println("There is a string conversion issue")
					}
					c.RWlock.RLock()
					if c.reduce_files_map[file_idx] == "completed"{
						c.RWlock.RUnlock()
						return
					}else{
						c.RWlock.RUnlock()
					}
				}
		}
	}
}

func (c *Coordinator)TaskProducer(){
	for _,v := range c.input_files{
		if c.input_file_map[v] == "not-started"{
			map_tasks <- v
		}
	}
	ok:=false
	for !ok{
		ok = c.is_all_map_done()
	}
	c.all_maps_done = true
	fmt.Println("All Maps Done")

	for i:=0; i<c.nReduce; i++{
		if c.reduce_files_map[i] == "not-started"{
			reduce_tasks <- i
		}
	}
	ok = false
	for !ok{
		ok = c.is_all_reduce_done()
	}
	c.all_reduces_done = true
}

func (c *Coordinator)StoreIntermediateFile(args *StoreIntermediate, reply *ReplyingTaskArgs)error{
	
	c.intermediate_file_names[args.ReduceNum] = append(c.intermediate_file_names[args.ReduceNum], args.Intermediate_file_name)
	// fmt.Println(c.intermediate_file_names)
	return nil
}

func (c *Coordinator)is_all_map_done()bool{
	c.RWlock.RLock()
	defer c.RWlock.RUnlock()
	for _,v := range(c.input_file_map){
		if v != "completed"{
			return false
		}
	}
	return true
}

func (c *Coordinator)is_all_reduce_done()bool{
	c.RWlock.RLock()
	defer c.RWlock.RUnlock()
	for i:=0; i<c.nReduce; i++{
		if c.reduce_files_map[i] != "completed"{
			return false
		}
	}
	return true
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")

	go c.TaskProducer()										//task producer needed to be a separate routine that handles channels

	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = c.all_reduces_done

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.input_files = files
	c.nReduce = nReduce
	c.map_task_num = 0
	c.intermediate_file_names = make([][]string, nReduce)
	c.all_maps_done = false
	c.all_reduces_done = false
	c.input_file_map = make(map[string]string)
	c.reduce_files_map = make(map[int]string)
	c.RWlock = new(sync.RWMutex)

	map_tasks = make(chan string, 3)			// buffer capacity of channel is 3
	reduce_tasks = make(chan int, 3)
	for _,v := range files{
		c.input_file_map[v] = "not-started"
	}
	for i:=0; i<nReduce;i++{
		c.reduce_files_map[i] = "not-started"
	}

	c.server()
	return &c
}
