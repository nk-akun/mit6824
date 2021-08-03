package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync/atomic"
)

// Master for MapReduce
type Master struct {
	JobManager    *JobManager
	WorkerManager *WorkerManager
}

type JobManager struct {
	Counter uint64
	Jobs    chan *Job // 任务队列
}

type Job struct {
	Type   byte // map or reduce
	Id     uint64
	Source string
}

type WorkerManager struct {
	Counter uint64
	Wokers  []*Woker
}

type Woker struct {
	Id uint64
}

// Your code here -- RPC handlers for the worker to call.

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

func (m *Master) RegisterWork(args *RegisterReq, reply *RegisterResp) error {
	id := atomic.AddUint64(&m.WorkerManager.Counter, 1)
	reply.WorkerId = id
	m.WorkerManager.Wokers = append(m.WorkerManager.Wokers, &Woker{Id: id})
	return nil
}

func (m *Master) AllocateJob(args *AskJobReq, reply *AskJobResp) error {
	// workerId := args.WorkerId
	job := <-m.JobManager.Jobs
	reply.Id = job.Id
	reply.Source = job.Source
	reply.Type = job.Type
	return nil
}

// MakeMaster creates a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		JobManager: &JobManager{
			Counter: 0,
			Jobs:    make(chan *Job, len(files)),
		},
		WorkerManager: &WorkerManager{
			Counter: 0,
		},
	}

	for _, f := range files {
		job := &Job{
			Type:   0,
			Id:     atomic.AddUint64(&m.JobManager.Counter, 1),
			Source: f,
		}
		m.JobManager.Jobs <- job
	}

	m.server()
	return &m
}
