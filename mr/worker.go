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
	if job.Type == 0 {
		doMap(mapf, job)
	}
}

func RegisterWork() uint64 {
	req := &RegisterReq{}
	resp := &RegisterResp{}
	succ := call("Master.RegisterWork", req, resp)
	if succ {
		return resp.WorkerId
	}
	return 0
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

func doMap(mapf func(string, string) []KeyValue, job *Job) {
	fileName := job.Source
	f, _ := os.Open(fileName)
	defer f.Close()

	wrFiles := make([]string, job.RNum)
	for i := 0; i < job.RNum; i++ {
		file := fmt.Sprintf("temporary_file_%d_%d.map", i, job.Id)
		wrFiles[i] = file
	}

	content, _ := ioutil.ReadAll(f)
	kvas := mapf(fileName, string(content))
	for _, kv := range kvas {
		hash := ihash(kv.Key)
	}
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
