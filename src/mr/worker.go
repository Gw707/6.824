package mr

import (
	"fmt"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	//TODO 开启通信

	ch := make(chan Job, 5)
	for i := 0; i < 5; i++ {
		go mapWorker(mapf, ch)
	}

	for i := 0; i < 5; i++ {
		go reduceWorker(reducef, ch)
	}

	//结束通信
	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func mapWorker(mapf func(string, string) []KeyValue, ch chan Job) {

	for true {
		time.Sleep(200)
		job := Job{}
		job.WorkType = 1
		job.JobId = "-1"
		call("Master.Map", &job, &job)
		if job.State == 6 {
			break
		}
		sourceFile := job.SourceFile

		file, err := os.Open(sourceFile)
		if err != nil {
			job.Status = false
			job.Mes = "file open failed!!!"
		}
		var dat []byte
		file.Read(dat)
		content := string(dat)

		keyValues := mapf(sourceFile, content)

		job.Res = keyValues
		job.State = 3
		//TODO map工作完成返回
		//call...
		ch <- job
	}

}

func reduceWorker(reducef func(string, []string) string, ch chan Job) {
	for true {
		time.Sleep(200)
		job := <-ch
		job.WorkType = 2
		call("Master.Reduce", &job, &job)

		if job.State == 7 {
			break
		}

		mp := job.Mp
		for _, word := range job.Res {
			mp[word.Key] = append(mp[word.Key], word.Value)
		}
		file, _ := os.Open("mr-out-1")

		for key, value := range mp {
			cnt := reducef(key, value)
			str := key + " " + cnt
			file.Write([]byte(str))
		}
		job.State = 5

		//TODO reduce工作完成返回
		//call...
	}

}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
