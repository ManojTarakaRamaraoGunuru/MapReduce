package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "strconv"
import "sort"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	args:= SeekingTaskArgs{}
	for true{
		args.Msg = SeekingTask
		reply := AskingTask(args)
		
		if reply.Task == "map"{

			MapTask(reply, mapf)
			args.Msg = NotifyMapFinished
			args.File_name = reply.File
			AskingTask(args)

		}else if reply.Task == "reduce"{
			ReduceTask(reply, reducef)
			args.Msg = NotifyReduceFinished
			args.Reduce_task_idx = reply.X
			AskingTask(args)
		}else{
			break
		}
	}
	
}

func ReduceTask(reply ReplyingTaskArgs, reducef func(string, []string) string){
	
	intermediate_file_names := reply.Intermediate_file_names
	intermediate := []KeyValue{}
	for _,intermediate_file_name := range(intermediate_file_names){
		file, err := os.Open(intermediate_file_name)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-"+strconv.Itoa(reply.X)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
}


func MapTask(reply ReplyingTaskArgs, mapf func(string, string) []KeyValue){
	// intermediate := []mr.KeyValue{}
	filename := reply.File
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()										// closing the file here
	kva := mapf(filename, string(content))
	// intermediate = append(intermediate, kva...)
	
	kvas := partiton(kva, reply.NReduce) // to make intermediate files
	create_intermediate_files(reply.X, kvas)
}


func AskingTask(args SeekingTaskArgs)ReplyingTaskArgs{
	
	reply:= ReplyingTaskArgs{}
	ok := call("Coordinator.TaskManager",&args,&reply)

	if ok{
		fmt.Println(reply.File)
	}else{
		fmt.Println("Call Failed")
	}
	return reply

}

func partiton(kva []KeyValue, nReduce int)[][]KeyValue{
	kvas := make([][]KeyValue, nReduce)
	for _, kv := range(kva){
		idx := ihash(kv.Key)%nReduce
		kvas[idx] = append(kvas[idx], kv)
	}
	return kvas
}

/*map task number is X*/
func create_intermediate_files(X int, kvas [][]KeyValue){
	for i,kva := range(kvas){
		intermediate_file_name := "mr-" + strconv.Itoa(X) + "-"+ strconv.Itoa(i)
		intermediate_file,err := os.Create(intermediate_file_name)
		if err != nil {
			log.Fatalf("cannot open %v %s", intermediate_file, err)
		}
		enc := json.NewEncoder(intermediate_file)
		for _,kv := range(kva){
			enc.Encode(&kv)
		}
		store_intermediate_file(intermediate_file_name, i)
	}
}

func store_intermediate_file(file_name string, ReduceNum int){
	args := StoreIntermediate{file_name, ReduceNum}
	reply := ReplyingTaskArgs{}
	ok := call("Coordinator.StoreIntermediateFile", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("Intermediate File %v stored\n", file_name)
	} else {
		fmt.Printf("call failed!\n")
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
