package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	unassigned = 0
	assigned   = 1
	finished   = 2
	taskTimeout = 10 // refresh task hasn't done after 10s
	MapTask = 3
	ReduceTask = 4
)

type Task struct {
	id       int
	filename string
	state    int
}

type Coordinator struct {
	mTasks     []Task
	rTasks     []Task
	mavailable int // available map tasks
	ravailable int // available reduce tasks
	mdone      int // done map tasks
	rdone      int // done reduce tasks
	mu         sync.Mutex
}

type PleaseExit struct {
	message string
}

func (e PleaseExit) Error() string {
	return e.message
}

// Your code here -- RPC handlers for the worker to call.

// assign map task or reduce task to worker
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	if c.mavailable > 0 { // assign map task
		for i, t := range c.mTasks {
			if t.state == unassigned {
				reply.TaskID = t.id
				reply.FileName = t.filename
				reply.TaskType = MapTask
				reply.NReduce = len(c.rTasks)

				c.mavailable--
				c.mTasks[i].state = assigned
				c.mu.Unlock()
				go c.refresh(reply.TaskID, MapTask)
				return nil
			}
		}
	} else if c.ravailable > 0 {
		for i, t := range c.rTasks {
			if t.state == unassigned {
				reply.TaskID = t.id
				reply.TaskType = ReduceTask
				reply.NReduce = len(c.rTasks)
				reply.NMap = len(c.mTasks)

				c.ravailable--
				c.rTasks[i].state = assigned
				c.mu.Unlock()
				go c.refresh(reply.TaskID, ReduceTask)
				return nil
			}
		}
	}
	c.mu.Unlock()
	return PleaseExit{"Jobs done! Please exit"}
}

// assign map task or reduce task to worker
func (c *Coordinator) CheckoutTask(args *TaskArgs, reply *TaskReply) error {
	
	if args.TaskType == MapTask { 
		c.mu.Lock()
		if c.mTasks[args.TaskID].state != finished {
			c.mTasks[args.TaskID].state = finished
			c.mdone++
		}
		c.mu.Unlock()
	} else { // reduce task
		c.mu.Lock()
		if c.rTasks[args.TaskID].state != finished {
			c.rTasks[args.TaskID].state = finished
			c.rdone++
		}
		c.mu.Unlock()
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	if c.rdone == len(c.rTasks) {
		ret = true
		//fmt.Println("Jobs done!")
	}
	c.mu.Unlock()

	return ret
}

// refresh the assigned tasks to unassigned every 10s
func (c *Coordinator) refresh(taskID int, taskType int) {

	<-time.After(time.Second * taskTimeout)

	c.mu.Lock()
	if taskType == MapTask && c.mTasks[taskID].state == assigned {
		c.mTasks[taskID].state = unassigned
		c.mavailable++
	} else if taskType == ReduceTask && c.rTasks[taskID].state == assigned{
		c.rTasks[taskID].state = unassigned
		c.ravailable++
	}
	c.mu.Unlock()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Initiate coordinator
	c := Coordinator{
		mavailable: len(files),
		ravailable: nReduce,
		mdone:      0,
		rdone:      0,
		mTasks:     make([]Task, len(files)),
		rTasks:     make([]Task, nReduce),
	}

	for i, file := range files {
		c.mTasks[i].id = i
		c.mTasks[i].filename = file
		c.mTasks[i].state = unassigned
	}
	for i := 0; i < c.ravailable; i++ {
		c.rTasks[i].id = i
		c.rTasks[i].state = unassigned
	}
	c.server()
	return &c
}

// personal print Coordinator
func (c *Coordinator) String() string {
	c.mu.Lock()
	builder := strings.Builder{}
	builder.WriteString("Map task:\n")
	for _, v := range c.mTasks {
		builder.WriteString(
			fmt.Sprintf("id: %d, filename: %s, state: %d\n", v.id, v.filename, v.state))
	}
	builder.WriteString("Reduce task:\n")
	for _, v := range c.rTasks {
		builder.WriteString(
			fmt.Sprintf("id: %d, filename: %s, state: %d\n", v.id, v.filename, v.state))
	}
	c.mu.Unlock()
	return builder.String()
}
