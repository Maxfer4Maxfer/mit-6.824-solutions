package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	completionTaskLimit = 10 * time.Second
	comeBackAfter       = 2 * time.Second
)

var errUnsupporteTaskType = errors.New("unsupported task type")

type task struct {
	id        TaskID
	done      bool
	file      string
	worker    string
	startTime time.Time
}

func (t *task) isExpired() bool {
	return time.Now().After(t.startTime.Add(completionTaskLimit))
}

func (t *task) isFree(log *log.Logger) bool {
	if !t.done && t.worker == "" {
		log.Printf("Found an unassigned task %d for file %s", t.id, t.file)

		return true
	}

	if !t.done && t.worker != "" && t.isExpired() {
		log.Printf("Found a stale %d work %s executed by %s",
			t.id, t.file, t.worker)

		return true
	}

	return false
}

type Coordinator struct {
	log              *log.Logger
	nReduce          int
	files            []string
	mapTasks         []*task
	nMapTasksDone    int
	reduceTasks      []*task
	nReduceTasksDone int
	everythingIsDone bool
	mu               sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) acceptTask(
	t *task, tt TaskType, args *CommitToWorkArgs, reply *CommitToWorkReply,
) {
	t.worker = args.Worker
	t.startTime = time.Now()
	reply.ID = t.id
	reply.TaskType = tt
	reply.File = t.file
	reply.NReduce = c.nReduce
}

func (c *Coordinator) CommitToWork(
	args *CommitToWorkArgs, reply *CommitToWorkReply,
) error {
	c.log.Printf("Take request from worker %s", args.Worker)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.nMapTasksDone != len(c.mapTasks) {
		for _, t := range c.mapTasks {
			if t.isFree(c.log) {
				c.acceptTask(t, MapTaskType, args, reply)

				return nil
			}
		}

		reply.ComeBackAfter = comeBackAfter

		return nil
	}

	if c.nReduceTasksDone != len(c.reduceTasks) {
		for _, t := range c.reduceTasks {
			if t.isFree(c.log) {
				c.acceptTask(t, ReduceTaskType, args, reply)

				return nil
			}
		}

		reply.ComeBackAfter = comeBackAfter

		return nil
	}

	c.log.Print("Everything is done. Nothing to do")

	reply.EverythingIsDone = true

	return nil
}

func (c *Coordinator) RegisterResult(
	args *RegisterResultArgs, reply *RegisterResultReply,
) error {
	c.log.Printf("Worker %s report that %s task %d is done",
		args.Worker, args.TaskType, args.ID)

	switch args.TaskType {
	case MapTaskType:
		i := args.ID

		c.mu.Lock()
		defer c.mu.Unlock()

		c.mapTasks[i].done = true
		c.nMapTasksDone++
	case ReduceTaskType:
		i := args.ID

		c.mu.Lock()
		defer c.mu.Unlock()

		c.reduceTasks[i].done = true
		c.nReduceTasksDone++
	default:
		err := fmt.Errorf("%w %v", errUnsupporteTaskType, args.TaskType)

		c.log.Printf("ERROR: %v", err)

		return err
	}

	if c.nReduceTasksDone == len(c.reduceTasks) {
		c.everythingIsDone = true
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go.
//
func (c *Coordinator) server() {
	if err := rpc.Register(c); err != nil {
		panic(err)
	}

	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)

	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	go func() {
		if err := http.Serve(l, nil); err != nil {
			log.Fatal("serve error:", err)
		}
	}()
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.everythingIsDone
}

func (c *Coordinator) createTasks() {
	c.log.Printf("Prepering %d map tasks", len(c.files))
	mts := make([]*task, len(c.files))

	for i := range c.files {
		var t task

		t.id = TaskID(i)
		t.file = c.files[i]

		mts[i] = &t
	}

	c.mapTasks = mts

	c.log.Printf("Prepering %d reduce tasks", c.nReduce)
	rts := make([]*task, c.nReduce)

	for i := 0; i < c.nReduce; i++ {
		var t task

		t.id = TaskID(i)

		rts[i] = &t
	}

	c.reduceTasks = rts
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log := log.New(os.Stdout, "", log.Lshortfile|log.Lmicroseconds)

	var c Coordinator

	c.log = log
	c.nReduce = nReduce
	c.files = files

	// Your code here.
	c.createTasks()

	c.server()

	return &c
}
