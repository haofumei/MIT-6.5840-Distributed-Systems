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
	for {
		reply, ok := requestTask()

		if !ok { // jobs all done, exit
			os.Exit(0)
		}

		if ok && reply.TaskType == MapTask { 
			doMap(mapf, &reply)
		}

		if ok && reply.TaskType == ReduceTask { 
			doReduce(reducef, &reply)
		}
	}
}

func doMap(mapf func(string, string) []KeyValue, reply *TaskReply) {
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %s", reply.FileName)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %s", reply.FileName)
	}
	file.Close()

	kva := mapf(reply.FileName, string(content))
	createImd(kva, reply)

	ok := submitTask(reply)
	if !ok {
		log.Fatalf("Submit mtask fail: %d\n", reply.TaskID)
	}
}

func doReduce(reducef func(string, []string) string, reply *TaskReply) {
	intermediate := []KeyValue{}
	for i := 0; i < reply.NMap; i++ { // load all the intermediate files
		filename := fmt.Sprintf("mr-%d-%d", i, reply.TaskID)
		fd, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %s", filename)
		}

		dec := json.NewDecoder(fd)
		var kv KeyValue
		for dec.More() {
			err := dec.Decode(&kv)
			if err != nil {
				log.Fatalf("cannot decode %s", filename)
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))
	createOutput(intermediate, reply, reducef)

	ok := submitTask(reply)
	if !ok {
		log.Fatalf("Submit rtask fail: %d\n", reply.TaskID)
	}
}

// ask coordinator for new task
func requestTask() (TaskReply, bool) {
	args := TaskArgs{}
	reply := TaskReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)

	return reply, ok
}

// call done when a task was finished
func submitTask(task *TaskReply) bool {
	args := TaskArgs{
		TaskID: task.TaskID,
		TaskType: task.TaskType,
	}
	reply := TaskReply{}
	ok := call("Coordinator.CheckoutTask", &args, &reply)
	return ok
}

// create intermediate files for a map task
func createImd(intermediate []KeyValue, reply *TaskReply) {
	// write to intermdediate file
	tmpfiles := make([]*os.File, reply.NReduce)  // tmp file descriptors
	encs := make([]*json.Encoder, reply.NReduce) // json encoder descriptors

	for i := range tmpfiles {
		filename := fmt.Sprintf("mr-%d-", reply.TaskID)
		file, err := ioutil.TempFile("", filename)
		if err != nil {
			log.Fatalf("Can not create tmp file %s", filename)
		}
		tmpfiles[i] = file
		encs[i] = json.NewEncoder(file)
	}

	for _, kv := range intermediate {
		i := ihash(kv.Key) % reply.NReduce // reduce partition index
		err := encs[i].Encode(&kv)
		if err != nil {
			log.Fatalf("Can not encode %v", kv)
		}
	}

	for i, v := range tmpfiles {
		// close the tmp file
		if err := v.Close(); err != nil {
			log.Fatalf("Can not close tmp file")
		}
		// rename the tmp file to the final destination.
		oname := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
		if err := os.Rename(v.Name(), oname); err != nil {
			log.Fatalf("Can not rename file %s", oname)
		}
		// clean the tmp file when done
		os.Remove(v.Name())
	}
}

// create the output files for reduce tasks
func createOutput(intermediate []KeyValue, reply *TaskReply,
	reducef func(string, []string) string) {

	tmpfile := fmt.Sprintf("tmpout-%d", reply.TaskID)
	tfd, err := ioutil.TempFile("", tmpfile)
	if err != nil {
		log.Fatalf("Can not create tmp file %s", tmpfile)
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-X.
	//
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

		// this is the correct format for each line of Reduce output.
		_, err = fmt.Fprintf(tfd, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			log.Fatalf("Can not write to file")
		}

		i = j
	}

	// rename tmp to mr-out-Y
	oname := fmt.Sprintf("mr-out-%d", reply.TaskID)
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	if err := os.Rename(tfd.Name(), oname); err != nil {
		log.Fatalf("Can not rename file %s", oname)
	}
	// clean the tmp file when done
	tfd.Close()
	os.Remove(tfd.Name())
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

	//fmt.Println(err)
	return false
}
