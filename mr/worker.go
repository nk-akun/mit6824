package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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

	job := AskJob(workerId)
	resultReq := ExecJob(mapf, reducef, job)
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
		idx := ihash(kv.Key)
		file := wrFiles[idx]
		// TODO: 分批刷入文件
		wrCache[file] = append(wrCache[file], kv)
	}

	for i := range wrFiles {
		f, _ = os.Create(wrFiles[i])
		// TODO: 刷入前排序
		for _, kv := range wrCache[wrFiles[i]] {
			f.WriteString(fmt.Sprintf("%s %s\n", kv.Key, kv.Value))
		}
		f.Close()
	}

	return wrFiles, nil
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
