package mr

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync/atomic"
)

// Master for MapReduce
type Master struct {
	Mnum          int
	Rnum          int
	JobManager    *JobManager
	WorkerManager *WorkerManager
}

type JobManager struct {
	Counter      uint64
	Jobs         chan *Job // 任务队列
	RShuffleChan chan string
}

type Job struct {
	Type   byte // map or reduce
	Id     uint64
	RNum   int
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

func (m *Master) RegisterWorker(args *RegisterReq, reply *RegisterResp) error {
	id := atomic.AddUint64(&m.WorkerManager.Counter, 1)
	reply.WorkerId = id
	m.WorkerManager.Wokers = append(m.WorkerManager.Wokers, &Woker{Id: id})
	return nil
}

func (m *Master) AllocateJob(args *AskJobReq, reply *AskJobResp) error {
	// workerId := args.WorkerId
	job := <-m.JobManager.Jobs
	reply.Job = &Job{
		Type:   job.Type,
		Id:     job.Id,
		Source: job.Source,
	}
	return nil
}

func (m *Master) ReportJobResult(args *JobResultReq, reply *JobResultResp) {
	if args.Code != 0 {
		// TODO: 错误处理
		return
	}

	if args.Type == 0 {
		for _, s := range args.Source {
			m.JobManager.RShuffleChan <- s
		}
	} else if args.Type == 1 {
		// TODO:
	}
}

func (m *Master) ShuffleReduceJobs() {
	rdMap := make(map[string][]string, m.Rnum)
	for f := range m.JobManager.RShuffleChan {
		rdIdx := strings.Split(f, "_")[3]
		rdMap[rdIdx] = append(rdMap[rdIdx], f)
		if len(rdMap[rdIdx]) == m.Mnum {
			// TODO: 归并排序
			reduceFile := fmt.Sprintf("reduce_file_%s", rdIdx)
			mergeKeys(reduceFile, rdMap[rdIdx])
			m.JobManager.Jobs <- &Job{
				Type:   1,
				Id:     atomic.AddUint64(&m.JobManager.Counter, 1),
				RNum:   m.Rnum,
				Source: reduceFile,
			}
		}
	}
}

func mergeKeys(outFile string, inFiles []string) {
	wf, _ := os.Create(outFile)
	fps := make([]*os.File, len(inFiles))
	frs := make([]*bufio.Reader, len(inFiles))
	for i, f := range inFiles {
		fp, _ := os.Open(f)
		frs[i] = bufio.NewReader(fp)
		fps[i] = fp
	}

	defer func() {
		for _, f := range fps {
			f.Close()
		}
		wf.Close()
	}()

	// 多路归并
	queue := make(PriorityQueue, len(frs))
	for i, f := range frs {
		line, _, err := f.ReadLine()
		if err == io.EOF {
			continue
		}

		kvs := strings.Split(string(line), " ")
		queue[i] = &Item{
			filePos:  i,
			priority: kvs[0],
			value:    kvs[1],
			index:    i,
		}
	}
	heap.Init(&queue)

	for len(queue) > 0 {
		item := heap.Pop(&queue).(*Item)
		wf.WriteString(fmt.Sprintf("%s %s\n", item.priority, item.value))

		line, _, err := frs[item.filePos].ReadLine()
		if err == io.EOF {
			continue
		}

		kvs := strings.Split(string(line), " ")
		heap.Push(&queue, &Item{
			filePos:  item.filePos,
			priority: kvs[0],
			value:    kvs[1],
		})
	}
}

// MakeMaster creates a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		Mnum: len(files),
		Rnum: nReduce,
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
			RNum:   nReduce,
			Source: f,
		}
		m.JobManager.Jobs <- job
	}

	m.server()
	return &m
}
