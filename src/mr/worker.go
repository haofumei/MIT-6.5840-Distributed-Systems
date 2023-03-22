package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	reply, ok := requestTask()
	fmt.Printf("Get task: %s\n", reply.FileName)

	if ok && reply.TaskType == 0 { // map task
		intermediate := []KeyValue{}
		file, err := os.Open(reply.FileName)
		if err != nil {
			fmt.Printf("cannot open %s", reply.FileName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			fmt.Printf("cannot read %s", reply.FileName)
		}
		file.Close()
		kva := mapf(reply.FileName, string(content))
		intermediate = append(intermediate, kva...)
		createImd(intermediate, &reply)
		
		ok := submitTask(&reply)
		if ok {
			fmt.Printf("Submit task success: %s\n", reply.FileName)
		} else {
			fmt.Printf("Submit task fail: %s\n", reply.FileName)
		}
	}
	if ok && reply.TaskType == 1 { // reduce task
		intermediate := []KeyValue{}
		for i:=0; i<reply.NMap; i++ {
			filename := fmt.Sprintf("./intermediate/mr-%d-%d", i, reply.ID)
			fd, err := os.Open(filename)
			if err != nil {
				fmt.Printf("cannot open %s", filename)
			}

			dec := json.NewDecoder(fd)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
				  break
				}
				intermediate = append(intermediate, kv)
			}
		}
		
		sort.Sort(ByKey(intermediate))
	}


	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

// ask coordinator for new task
func requestTask() (TaskReply, bool) {
	args := TaskArgs{}
	reply := TaskReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if !ok {
		fmt.Printf("call AssignTask failed!\n")
	}
	return reply, ok
}

// call done when a task was finished
func submitTask(task *TaskReply) bool {
	args := TaskArgs{
		ID: task.ID,
		TaskType: task.TaskType,
	}
	reply := TaskReply{}
	ok := call("Coordinator.CheckoutTask", &args, &reply)
	if !ok {
		fmt.Printf("call CheckoutTask failed!\n")
	}
	return ok
}

// create intermediate files for a map task
func createImd(intermediate []KeyValue, reply *TaskReply) {
	// write to intermdediate file
	tmpfiles := make([]*os.File, reply.NReduce)
	for i := range tmpfiles {
		file, err := ioutil.TempFile("./tmp", fmt.Sprintf("mr-%d-", reply.ID))
		if err != nil {
			panic(err)
		}
		tmpfiles[i] = file
	}
	
	for _, kv := range intermediate {
		i := ihash(kv.Key) % reply.NReduce  // reduce partition index
		enc := json.NewEncoder(tmpfiles[i])
		err := enc.Encode(&kv)
		if err != nil {
			panic(err)
		}
	}

	for i, v := range tmpfiles {
		// close the tmp file
		if err := v.Close(); err != nil {
			panic(err)
		}
		// rename the tmp file to the final destination.
		oname := fmt.Sprintf("./intermediate/mr-%d-%d", reply.ID, i)
		if err := os.Rename(v.Name(), oname); err != nil {
			panic(err)
		}
		// clean the tmp file when done
		os.Remove(v.Name()) 
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
