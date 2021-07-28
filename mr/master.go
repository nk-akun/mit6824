package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

// Master for MapReduce
type Master struct {
	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.

// Example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// server starts a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done is called periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// MakeMaster creates a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.server()
	return &m
}

/*
This repository contains a better version of the code from MIT's 6.824 intended for non-MIT students who just want to dive into the code.

The modifications include:

Restructuring the code to use Go modules and to remove errors. Some of these errors include duplicate methods on the same package, when really they should belong to different folders/packages since they should not be compiled separately anyway (see the mr folder).
Editing some scripts to account for the restructuring.
Removing files that required Athena to use. The presence of these files will cause errors/warnings in your Go code otherwise.
Removing golint warnings/errors. Most of these changes are cosmetic (changing x += 1 to x++, using camelCase instead of snake_case, adding documentation for exported functions/variables). The only non-cosmetic change is replacing calls to t.Fatalf from non-test goroutines in labrpc/test_test.go.

*/
