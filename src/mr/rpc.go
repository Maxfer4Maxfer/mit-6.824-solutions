package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

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

// Add your RPC definitions here.

type TaskType uint32

func (tt TaskType) String() string {
	switch tt {
	case MapTaskType:
		return "map"
	case ReduceTaskType:
		return "reduce"
	default:
		return "!unsupported_task!"
	}
}

const (
	MapTaskType TaskType = iota
	ReduceTaskType
)

type TaskID uint32

func (id TaskID) String() string {
	return strconv.Itoa(int(id))
}

type CommitToWorkArgs struct {
	Worker string
}

type CommitToWorkReply struct {
	ID               TaskID
	TaskType         TaskType
	File             string
	NReduce          int
	EverythingIsDone bool
	ComeBackAfter    time.Duration
}

type RegisterResultArgs struct {
	Worker   string
	ID       TaskID
	TaskType TaskType
}

type RegisterResultReply struct {
	EverythingIsDone bool
	ComeBackAfter    time.Duration
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())

	return s
}
