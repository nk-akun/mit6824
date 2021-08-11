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
	"time"
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
	SuccNum      int64
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
	succNum := atomic.LoadInt64(&m.JobManager.SuccNum)
	return succNum == int64(m.Rnum)
}

func (m *Master) RegisterWorker(args *RegisterReq, reply *RegisterResp) error {
	id := atomic.AddUint64(&m.WorkerManager.Counter, 1)
	reply.WorkerId = id
	m.WorkerManager.Wokers = append(m.WorkerManager.Wokers, &Woker{Id: id})
	return nil
}

func (m *Master) AllocateJob(args *AskJobReq, reply *AskJobResp) error {
	// workerId := args.WorkerId
	ticker := time.NewTicker(10 * time.Second)
	select {
	case <-ticker.C:
		return fmt.Errorf("no job")
	case job := <-m.JobManager.Jobs:
		// TODO: 任务是否完成的监控
		reply.Job = &Job{
			Type:   job.Type,
			Id:     job.Id,
			Source: job.Source,
			RNum:   m.Rnum,
		}
		return nil
	}
}

func (m *Master) ReportJobResult(args *JobResultReq, reply *JobResultResp) error {
	if args.Code != 0 {
		// TODO: 错误处理
		return nil
	}

	if args.Type == 0 {
		for _, s := range args.Source {
			m.JobManager.RShuffleChan <- s
		}
	} else if args.Type == 1 {
		atomic.AddInt64(&m.JobManager.SuccNum, 1)
	}
	return nil
}

func (m *Master) ShuffleReduceJobs() {
	rdMap := make(map[string][]string, m.Rnum)
	// RShuffleChan采用无缓冲chan会hang死，好的解决方案?
	for f := range m.JobManager.RShuffleChan {
		rdIdx := strings.Split(f, "_")[3]
		rdMap[rdIdx] = append(rdMap[rdIdx], f)
		if len(rdMap[rdIdx]) == m.Mnum {
			reduceFile := fmt.Sprintf("./reduce_file_%s.out", rdIdx)
			mergeKeys(rdMap[rdIdx], reduceFile)
			m.JobManager.Jobs <- &Job{
				Type:   1,
				Id:     atomic.AddUint64(&m.JobManager.Counter, 1),
				RNum:   m.Rnum,
				Source: reduceFile,
			}
		}
	}
}

func mergeKeys(inFiles []string, outFile string) {
	wf, _ := os.Create(outFile)
	fps := make([]*os.File, len(inFiles))
	frs := make([]*bufio.Reader, len(inFiles))
	for i, f := range inFiles {
		fp, err := os.Open(f)
		if err != nil {
			fmt.Printf("error:%v\n", err)
		}
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
	for i, fr := range frs {
		line, _, err := fr.ReadLine()
		if err == io.EOF {
			continue
		}

		kvs := strings.Split(string(line), " ")
		queue[i] = &Item{
			filePos:  i,
			priority: kvs[0],
			value:    kvs[1], // 可能不止1呢？
			index:    i,
		}
	}
	heap.Init(&queue)

	// 可以优化为读取多个，减少io次数
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
			Counter:      0,
			Jobs:         make(chan *Job, len(files)),
			RShuffleChan: make(chan string, len(files)), // TODO: 是否可以优化
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

	go m.ShuffleReduceJobs()
	m.server()
	return &m
}
