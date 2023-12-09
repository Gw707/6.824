package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	LimitThreads int

	jobCount int //生成jobId

	mapFiles []string //开始时的所有任务，分配给worker

	timeStamp []int64 //记录上次通信的时间

	jobList []Job //Job状态1-undo、2-map、3-mapped、4-reduce、5-reduced/finish、6-timeout==超时直接重试，无中间态

	lock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Map(args *Job, reply *Job) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	//分发map类型的job
	reply = args
	workType := args.WorkType
	if workType == 2 || reply.State > 1 {
		reply.Status = false
		reply.Mes = "Job mapped!!!"
		return nil
	}

	if reply.JobId == "-1" {
		reply.Status = false
		reply.Mes = "no Job to map!!!"
		flag := true
		for _, job := range m.jobList {
			if job.State < 3 {
				flag = false
			}
		}
		if flag {
			reply.State = 6
		}
		return nil
	}

	for _, job := range m.jobList {
		state := job.State
		if state == 1 {
			reply = &job //给worker分配任务，将状态置为 mapping - 2
			reply.State = 2
			reply.Status = true
			reply.Mes = "success"
			break
		}
	}

	m.autoFlush(reply)
	return nil
}

func (m *Master) Reduce(args *Job, reply *Job) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	//在收到Job后第一时间改变状态为 mapped - 3
	reply = args
	// reduce调用前将状态改为 reducing - 4
	//限制reduce的线程数量
	//通过设置队列来限制
	nThreads := m.LimitThreads

	curNumber := m.check()

	state := reply.State
	if state != 3 {
		reply.Status = false
		reply.Mes = "state doesn't match operation"
		return nil
	}

	if curNumber >= nThreads {
		reply.Status = false
		reply.Mes = "already in max running reduce"
		return nil
	}
	flag := true
	for _, job := range m.jobList {
		if job.State < 3 {
			flag = false
		}
	}
	if flag {
		reply.State = 7
		reply.Status = false
		reply.Mes = "all completed"
	}

	for _, job := range m.jobList {
		state := job.State
		if state == 3 {
			reply = &job //给worker分配任务
			reply.State = 4
			reply.Status = true
			reply.Mes = "success"
			break
		}
	}
	m.autoFlush(reply)
	return nil
}

func (m *Master) check() int {
	count := 0
	list := m.jobList
	for _, job := range list {
		if job.State == 4 {
			count++
		}
	}
	return count
}

func (m *Master) autoFlush(job *Job) {
	jobId, _ := strconv.Atoi(job.JobId)
	m.jobList[jobId] = *job
	m.timeStamp[jobId] = time.Now().UnixMilli()
	return
}

// start a thread that listens for RPCs from worker.go
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

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := true
	//map完成后检查reduce是否完成
	if len(m.mapFiles) <= len(m.jobList) {
		for _, job := range m.jobList {
			if job.State < 5 {
				ret = false
			}
		}
	} else {
		ret = false
	}

	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mapFiles = files
	m.jobList = make([]Job, len(files))
	m.LimitThreads = nReduce
	m.timeStamp = make([]int64, len(files))
	m.jobCount = 0

	//将job封装好
	for _, file := range files {
		job := Job{
			JobId:      string(m.jobCount),
			WorkType:   1,
			SourceFile: file,
			State:      1,
		}
		m.jobCount++
		m.jobList = append(m.jobList, job)
	}

	//开启线程定期检查job处理情况
	go m.checkTime()
	m.server()
	return &m
}

func (m *Master) checkTime() {
	for true {
		for index, stamp := range m.timeStamp {
			lastTime := time.UnixMilli(stamp)
			if lastTime.Add(2 * time.Second).Before(time.Now()) {
				//当前job已过期，需要更新状态，等待重新处理
				m.lock.Lock()
				job := m.jobList[index]
				if job.State == 1 {
					continue
				}
				job.State--
				m.autoFlush(&job)
				m.lock.Unlock()
			}
		}

		time.Sleep(1 * time.Second)
	}
}
