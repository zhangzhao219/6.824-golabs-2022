package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	MapTask    []MapTaskInformation    // Map任务列表
	ReduceTask []ReduceTaskInformation // Reduce任务列表
}

type MapTaskInformation struct {
	Id                   int    // 任务唯一编码
	State                int    // 0表示未开始，1表示正在进行，2表示已经完成
	NReduce              int    // 分成Reduce任务的数量
	OriginFileName       string // 原始文件名称
	IntermediateFileName string // Map任务完成后的文件名称（中间文件）
}

type ReduceTaskInformation struct {
	Id             int    // 任务唯一编码
	State          int    // 0表示未开始，1表示正在进行，2表示已经完成
	OriginFileName string // Reduce的初始文件名称（中间文件）
	OutputFileName string // Reduce任务完成后的最终文件名称
}

var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.

// 分配任务
func (c *Coordinator) AsssignTask(args *TaskInformation, reply *TaskInformation) error {
	isMapfinished := true
	//遍历所有的Map任务信息，将未开始的分配给这个节点
	for i, mapTask := range c.MapTask {
		if mapTask.State == 0 {
			isMapfinished = false
			reply.Id = mapTask.Id
			reply.TaskType = "map"
			reply.InputFileName = mapTask.OriginFileName
			reply.OutputFileName = mapTask.IntermediateFileName
			reply.NReduce = mapTask.NReduce
			mu.Lock()
			c.MapTask[i].State = 1
			mu.Unlock()
			return nil
		} else if mapTask.State == 1 {
			isMapfinished = false
		}
	}
	// 如果所有的Map任务都完成了，就遍历Reduce任务
	if isMapfinished {
		for i, reduceTask := range c.ReduceTask {
			if reduceTask.State == 0 {
				reply.Id = reduceTask.Id
				reply.TaskType = "reduce"
				reply.InputFileName = reduceTask.OriginFileName
				reply.OutputFileName = reduceTask.OutputFileName
				mu.Lock()
				c.ReduceTask[i].State = 1
				mu.Unlock()
				return nil
			}
		}

	}
	return nil
}

// 接收任务已经完成的信息
func (c *Coordinator) TaskFinish(args *TaskInformation, reply *TaskInformation) error {
	if args.TaskType == "map" {
		mu.Lock()
		c.MapTask[args.Id-1].State = 2
		mu.Unlock()
	} else if args.TaskType == "reduce" {
		mu.Lock()
		c.ReduceTask[args.Id-1].State = 2
		mu.Unlock()
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := true
	mu.Lock()
	// Your code here.
	for _, v := range c.ReduceTask {
		if v.State != 2 {
			ret = false
			break
		}
	}
	mu.Unlock()
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	mapTaskSlice := []MapTaskInformation{}

	for id, fileName := range files {
		mapTaskSlice = append(mapTaskSlice, MapTaskInformation{
			Id:                   id + 1,
			State:                0,
			NReduce:              nReduce,
			OriginFileName:       fileName,
			IntermediateFileName: "mr-" + strconv.Itoa(id+1) + "-",
		})
	}

	reduceTaskSlice := []ReduceTaskInformation{}

	for i := 0; i < nReduce; i++ {
		reduceTaskSlice = append(reduceTaskSlice, ReduceTaskInformation{
			Id:             i + 1,
			State:          0,
			OriginFileName: "mr-*-" + strconv.Itoa(i+1),
			OutputFileName: "mr-out-" + strconv.Itoa(i+1),
		})
	}

	c := Coordinator{
		MapTask:    mapTaskSlice,
		ReduceTask: reduceTaskSlice,
	}

	// Your code here.

	c.server()
	return &c
}
