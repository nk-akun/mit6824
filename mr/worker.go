package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
)

// KeyValue is the type of the slice contents returned by the Map functions.
type KeyValue struct {
	Key   string
	Value string
}

// ihash(key) % NReduce is used to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker is called by the MapReduce worker.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerId := RegisterWork()
	// TODO: 上报心跳

	for {
		job := AskJob(workerId)
		if job == nil {
			break
		}

		resultReq := ExecJob(mapf, reducef, job)
		resultResp := &JobResultResp{}
		succ := call("Master.ReportJobResult", resultReq, resultResp)
		if succ {
			fmt.Printf("上报成功\n")
		}
	}
	fmt.Printf("worker结束\n")
}

func RegisterWork() uint64 {
	req := &RegisterReq{}
	resp := &RegisterResp{}
	succ := call("Master.RegisterWorker", req, resp)
	if succ {
		return resp.WorkerId
	}
	return 0
}

func ExecJob(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, job *Job) *JobResultReq {

	var result *JobResultReq
	if job.Type == 0 {
		files, err := doMap(mapf, job)
		if err == nil {
			result = &JobResultReq{
				Code:   0,
				Type:   0,
				JobId:  job.Id,
				Source: files,
			}
		} else {
			result = &JobResultReq{
				Code:  1,
				Type:  0,
				JobId: job.Id,
			}
		}
	} else {
		files, err := doReduce(reducef, job)
		if err == nil {
			result = &JobResultReq{
				Code:   0,
				Type:   1,
				JobId:  job.Id,
				Source: files,
			}
		} else {
			result = &JobResultReq{
				Code:  1,
				Type:  1,
				JobId: job.Id,
			}
		}
	}

	return result
}

func AskJob(workId uint64) *Job {
	if workId == 0 {
		return nil
	}
	req := &AskJobReq{
		WorkerId: workId,
	}
	resp := &AskJobResp{}
	succ := call("Master.AllocateJob", req, resp)
	if succ {
		return resp.Job
	}
	return nil
}

func doMap(mapf func(string, string) []KeyValue, job *Job) ([]string, error) {
	fileName := job.Source
	f, _ := os.Open(fileName)
	defer f.Close()

	wrFiles := make([]string, job.RNum)
	for i := 0; i < job.RNum; i++ {
		file := fmt.Sprintf("temporary_map_file_%d_%d.out", i, job.Id)
		wrFiles[i] = file
	}

	wrCache := make(map[string][]KeyValue)

	content, _ := ioutil.ReadAll(f)
	kvas := mapf(fileName, string(content))
	for _, kv := range kvas {
		idx := ihash(kv.Key) % job.RNum
		file := wrFiles[idx]
		// TODO: 分批刷入文件
		wrCache[file] = append(wrCache[file], kv)
	}

	for i := range wrFiles {
		f, _ = os.Create(wrFiles[i])

		kvas := wrCache[wrFiles[i]]
		sort.Slice(kvas, func(i, j int) bool { return kvas[i].Key < kvas[j].Key })

		for _, kv := range kvas {
			f.WriteString(fmt.Sprintf("%s %s\n", kv.Key, kv.Value))
		}
		f.Close()
	}

	return wrFiles, nil
}

func doReduce(reducef func(string, []string) string, job *Job) ([]string, error) {
	f, _ := os.Open(job.Source)
	r := bufio.NewReader(f)
	defer f.Close()

	var (
		reduceRes  map[string]string
		values     []string
		currentKey string
	)

	reduceRes = map[string]string{}
	for {
		line, _, err := r.ReadLine()
		if err == io.EOF {
			reduceRes[currentKey] = reducef(currentKey, values)
			break
		}
		kv := strings.Split(string(line), " ")
		if currentKey == "" || kv[0] == currentKey {
			values = append(values, kv[1])
		} else {
			reduceRes[currentKey] = reducef(currentKey, values)
			values = values[:0]
		}
		currentKey = kv[0]
	}

	outFile := fmt.Sprintf("mr-out-%d.out", job.Id)
	f, _ = os.Open(outFile)
	defer f.Close()

	for k, v := range reduceRes {
		f.WriteString(fmt.Sprintf("%s %s\n", k, v))
	}
	return []string{outFile}, nil
}

// call sends an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":3234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
