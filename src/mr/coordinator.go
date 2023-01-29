package mr

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Coordinator存储的主要信息，包括Map和Reduce两部分任务的信息以及工作节点的信息
type Coordinator struct {
	UniqueIdSlice     []*list.Element // 通过任务Id找到任务信息的切片，相当于一个Map
	MapTaskNum        int             // map任务总数量
	ReduceTaskNum     int             // reduce任务总数量
	WorkerNum         int             // 目前正在工作的节点数量
	MapTask                           // Map任务信息链表
	ReduceTask                        // Reduce任务信息链表
	WorkerInformation                 // Worker的信息
}

// 存储Worker的信息
type WorkerInformation struct {
}

// Map任务信息链表，包括三个链表，分别表示未开始、正在进行和已经完成的任务
type MapTask struct {
	MapListReady    *list.List // 未开始的Map任务
	MapListRunning  *list.List // 正在进行的Map任务
	MapListComplete *list.List // 已经完成的Map任务
}

// Reduce任务信息链表，包括三个链表，分别表示未开始、正在进行和已经完成的任务
type ReduceTask struct {
	ReduceListReady    *list.List // 未开始的Reduce任务
	ReduceListRunning  *list.List // 正在进行的Reduce任务
	ReduceListComplete *list.List // 已经完成的Reduce任务
}

// Map任务具体信息
type MapTaskInformation struct {
	Id                   int      // 任务唯一编码
	OriginFileName       string   // 原始文件名称
	IntermediateFileName []string // Map任务完成后中间文件列表
}

// Reduce任务具体信息
type ReduceTaskInformation struct {
	Id                   int      // 任务唯一编码
	IntermediateFileName []string // Reduce的初始中间文件列表（从Map处获得）
	OutputFileName       string   // Reduce任务完成后的最终文件名称
}

type HeartBeat struct {
	WorkID int
	Time   int64
}

// 全局互斥锁
var mu sync.Mutex
var WorkerList []HeartBeat

// Coordinator接收心跳信号
func (c *Coordinator) WorkerAlive(args *WorkerArgs, reply *WorkerReply) error {
	mu.Lock()
	WorkerList[args.Id-1].Time = time.Now().Unix()
	fmt.Printf("接收到%d心跳信号\n", args.Id-1)
	mu.Unlock()
	return nil
}

// Worker告知Coordinator自己上线了
func (c *Coordinator) WorkerOnline(args *WorkerArgs, reply *WorkerReply) error {
	mu.Lock()
	if c.WorkerNum == -1 {
		c.WorkerNum = 0
	}
	c.WorkerNum += 1
	// 分配任务ID并记录时间
	WorkerList = append(WorkerList, HeartBeat{
		WorkID: -1,
		Time:   time.Now().Unix(),
	})
	reply.WorkerID = len(WorkerList)
	mu.Unlock()
	return nil
}

// Worker向Coordinator请求任务
func (c *Coordinator) AsssignTask(args *WorkerArgs, reply *WorkerReply) error {

	mu.Lock()

	// 首先查看map任务是否已经全部完成，如果全部完成了就去完成Reduce任务，如果也全部完成了就发送Worker可以退出的消息
	// 判断方式：通过完成链表的节点数量与初始化时侯计算的数量是否相同

	if c.MapListComplete.Len() != c.MapTaskNum {

		// 分配map任务

		if c.MapListReady.Len() == 0 {

			// 没有没开始的Map任务
			reply.TaskType = "waiting"

		} else {

			// 将一个未完成的任务从未开始的链表中取出，插入到正在进行的链表里面
			e := c.MapListReady.Front()
			c.MapListReady.Remove(e)
			c.MapListRunning.PushBack(e)

			// 构建返回消息，告知Worker这个任务的信息
			reply.TaskType = "map"
			value := e.Value.(MapTaskInformation)
			reply.Id = value.Id
			reply.MapInput = value.OriginFileName
			reply.MapOutput = value.IntermediateFileName
			WorkerList[args.Id-1].WorkID = value.Id
		}
	} else if c.ReduceListComplete.Len() != c.ReduceTaskNum {

		// 分配reduce任务

		if c.ReduceListReady.Len() == 0 {
			// 没有没开始的Reduce任务
			reply.TaskType = "waiting"

		} else {

			// 将一个未完成的任务从未开始的链表中取出，插入到正在进行的链表里面
			e := c.ReduceListReady.Front()
			c.ReduceListReady.Remove(e)
			c.ReduceListRunning.PushBack(e)

			// 构建返回消息，告知Worker这个任务的信息
			reply.TaskType = "reduce"
			value := e.Value.(ReduceTaskInformation)
			reply.Id = value.Id
			reply.ReduceInput = value.IntermediateFileName
			reply.ReduceOutput = value.OutputFileName
			WorkerList[args.Id-1].WorkID = value.Id
		}
	} else {

		//告知Worker已经没有任务了，可以退出了
		reply.TaskType = "finish"
	}

	mu.Unlock()

	return nil
}

// Worker告知Coordinator刚才分配的任务已经完成
func (c *Coordinator) TaskFinish(args *WorkerArgs, reply *WorkerReply) error {

	mu.Lock()

	// 将节点从正在进行的链表中取出，插入到已经完成的链表中
	if args.TaskType == "map" {

		// 操作节点
		e := c.UniqueIdSlice[args.Taskid]
		c.MapListRunning.Remove(e)
		c.MapListComplete.PushBack(e)

		// 如果是Map任务，需要将产生的nReduce个中间文件分配给Reduce节点
		for _, file := range e.Value.(MapTaskInformation).IntermediateFileName {

			// 计算是哪个Reduce节点
			reduceTaskNum, err := strconv.Atoi(strings.Split(file, "-")[2])
			if err != nil {
				log.Fatalf("cannot parseInt %v", file)
			}

			// 将产生的nReduce个中间文件分配给Reduce节点（需要重新构建节点）
			value := c.UniqueIdSlice[reduceTaskNum].Value
			tempSlice := append(value.(ReduceTaskInformation).IntermediateFileName, file)
			c.UniqueIdSlice[reduceTaskNum].Value = ReduceTaskInformation{
				Id:                   value.(ReduceTaskInformation).Id,
				IntermediateFileName: tempSlice,
				OutputFileName:       value.(ReduceTaskInformation).OutputFileName,
			}
		}
	} else if args.TaskType == "reduce" {

		// 操作节点
		e := c.ReduceListRunning.Remove(c.UniqueIdSlice[args.Taskid])
		c.ReduceListComplete.PushBack(e)
	}
	mu.Unlock()
	return nil
}

// Worker告知Coordinator自己退出了
func (c *Coordinator) WorkerFinish(args *WorkerArgs, reply *WorkerReply) error {

	mu.Lock()

	// 退出时将Coordinator内部存储的Worker数量-1
	c.WorkerNum -= 1

	mu.Unlock()

	return nil
}

// main/mrcoordinator.go每隔一秒调用Done()进行检查，如果返回true，认为全部的任务都已经完成，退出主程序
func (c *Coordinator) Done() bool {
	ret := false
	mu.Lock()
	if c.WorkerNum == 0 {
		ret = true
	}
	mu.Unlock()
	return ret
}

// 开启监听Worker的线程
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

// 创建一个 Coordinator
// 输入：输入文件名称和Reduce任务执行的节点数量
// 输出：Coordinator指针
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	// 全局唯一任务id，通过切片的方式存储，以下标作为索引
	uniqueIdSlice := make([]*list.Element, 0)
	uniqueId := 0

	// map的三个双向链表
	mapListReady := list.New()
	mapListRunning := list.New()
	mapListComplete := list.New()

	// reduce的三个双向链表
	reduceListReady := list.New()
	reduceListRunning := list.New()
	reduceListComplete := list.New()

	// 遍历输入文件，构建Map结构体并插入到双向链表中
	for _, fileName := range files {
		// 构建输出文件名称
		tempSlice := make([]string, 0)
		for i := len(files); i < nReduce+len(files); i++ {
			tempSlice = append(tempSlice, "mr-"+strconv.Itoa(uniqueId)+"-"+strconv.Itoa(i)+"-")
		}
		uniqueIdSlice = append(uniqueIdSlice, mapListReady.PushBack(MapTaskInformation{
			Id:                   uniqueId,
			OriginFileName:       fileName,
			IntermediateFileName: tempSlice,
		}))
		uniqueId += 1
	}

	// fmt.Println(uniqueIdSlice[0].Value)

	// 获得map任务的数量
	mapTaskNum := uniqueId

	// Reduce节点数量，构建Reduce结构体并插入到双向链表中
	for i := 0; i < nReduce; i++ {
		uniqueIdSlice = append(uniqueIdSlice, reduceListReady.PushBack(ReduceTaskInformation{
			Id:                   uniqueId,
			IntermediateFileName: make([]string, 0),
			OutputFileName:       "mr-out-" + strconv.Itoa(uniqueId),
		}))
		uniqueId += 1
	}

	// 构建 Coordinator
	c := Coordinator{
		WorkerNum:     -1,
		UniqueIdSlice: uniqueIdSlice,
		MapTaskNum:    mapTaskNum,
		ReduceTaskNum: nReduce,
		MapTask: MapTask{
			MapListReady:    mapListReady,
			MapListRunning:  mapListRunning,
			MapListComplete: mapListComplete,
		},
		ReduceTask: ReduceTask{
			ReduceListReady:    reduceListReady,
			ReduceListRunning:  reduceListRunning,
			ReduceListComplete: reduceListComplete,
		},
	}
	// Worker信息存储
	WorkerList = make([]HeartBeat, 0)
	// 每间隔10秒进行验证
	go func() {
		for {
			time.Sleep(10 * time.Second)
			mu.Lock()
			for i := 0; i < len(WorkerList); i++ {
				if WorkerList[i].WorkID != -1 && time.Now().Unix()-WorkerList[i].Time > 10 {
					fmt.Printf("%d心跳信号过期\n", i)
					e2 := *(c.UniqueIdSlice[WorkerList[i].WorkID])

					// 这里不太懂为什么要这样写
					if WorkerList[i].WorkID < c.MapTaskNum {
						c.MapListRunning.Remove(&e2)
						c.MapListReady.PushBack(e2.Value)
					} else {
						c.ReduceListRunning.Remove(&e2)
						c.ReduceListReady.PushBack(e2.Value)
					}

					c.WorkerNum -= 1
					WorkerList[i].WorkID = -1
				}
			}
			mu.Unlock()
		}
	}()

	// 启动监听Worker的线程
	c.server()

	return &c
}
