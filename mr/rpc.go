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

// ExampleArgs to show how to declare the arguments
// for an RPC.
type ExampleArgs struct {
	X int
}

// ExampleReply to show how to declare the reply
// for an RPC.
type ExampleReply struct {
	Y int
}

type RegisterReq struct {
}

type RegisterResp struct {
	WorkerId uint64
}

type AskJobReq struct {
	WorkerId uint64
}

type AskJobResp struct {
	Type   byte
	Id     uint64
	Source string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
