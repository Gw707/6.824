package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

type Job struct {
	JobId    string
	WorkType uint8 //工作类型 1-map、2-reduce
	//map操作所需要的参数-IO  --> 返回数据切片
	SourceFile string

	//reduce操作所需要的参数-Compute  --> 进行单词统计
	//TargetFile string

	State uint8 //Job状态1-undo、2-mapping、3-mapped、4-reducing、5-reduced/finish、6-allMapped、7-allReduced

	//LimitThreads int //限制worker的数量 --> nReduce

	//map返回结果  reduce参数
	Res []KeyValue //map结果--数据切片  --> reduce参数

	Mp map[string][]string //reduce结果--单词统计

	Status bool
	Mes    string
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
