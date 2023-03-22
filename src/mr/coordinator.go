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
)

type Task struct {
	id       int
	filename string
	state    int
	modified time.Time
}

type Coordinator struct {
	// Your definitions here.
	mTasks     []Task
	rTasks     []Task
	mavailable int // available map tasks
	ravailable int // available reduce tasks
	mdone      int // done map tasks
	rdone      int // done reduce tasks
	mu         sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// assign map task or reduce task to worker
func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	if c.mavailable > 0 { // assign map task
		for i, t := range c.mTasks {
			if t.state == unassigned {
				reply.ID = t.id
				reply.FileName = t.filename
				reply.TaskType = 0
				reply.NReduce = len(c.rTasks)

				c.mavailable--
				c.mTasks[i].state = assigned
				c.mTasks[i].modified = time.Now()
				c.mu.Unlock()
				return nil
			}
		}
	} else if c.ravailable > 0 {
		for i, t := range c.rTasks {
			if t.state == unassigned {
				reply.ID = t.id
				reply.TaskType = 1
				reply.NReduce = len(c.rTasks)
				reply.NMap = len(c.mTasks)

				c.ravailable--
				c.rTasks[i].state = assigned
				c.rTasks[i].modified = time.Now()
				c.mu.Unlock()
				return nil
			}
		}
	}
	return nil
}

// assign map task or reduce task to worker
func (c *Coordinator) CheckoutTask(args *TaskArgs, reply *TaskReply) error {
	
	if args.TaskType == 0 { // map task
		c.mu.Lock()
		c.mTasks[args.ID].state = finished
		c.mdone++
		c.mu.Unlock()
	} else { // reduce task
		c.mu.Lock()
		c.rTasks[args.ID].state = finished
		c.rdone++
		c.mu.Unlock()
	}
	fmt.Println(c)
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

	return ret
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
		c.mTasks[i].modified = time.Now()
	}
	for i := 0; i < c.ravailable; i++ {
		c.rTasks[i].id = i
		c.rTasks[i].state = unassigned
	}
	fmt.Println(&c)
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
			fmt.Sprintf("id: %d, filename: %s, state: %d, modified: %s\n", v.id, v.filename, v.state, v.modified))
	}
	builder.WriteString("Reduce task:\n")
	for _, v := range c.rTasks {
		builder.WriteString(
			fmt.Sprintf("id: %d, filename: %s, state: %d, modified: %s\n", v.id, v.filename, v.state, v.modified))
	}
	c.mu.Unlock()
	return builder.String()
}
