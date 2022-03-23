package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
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

type RegisterArgs struct {
}

type RegisterReply struct {
	WorkerId int
	M        int
	R        int
}

type AskArgs struct {
	WorkerId int
}

type AskReply struct {
	Over        bool //Job Over!
	TaskId      int
	TaskType    int    // 0:Map 1:Reduce
	MapFileName string // if map
}

type FinishMapArgs struct {
	WorkerId int
	TaskId   int
	//FileName string
}

type FinishMapReply struct {
}

type FinishReduceArgs struct {
	WorkerId int
	TaskId   int
	//FileName string
}

type FinishReduceReply struct {
	Over bool
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
