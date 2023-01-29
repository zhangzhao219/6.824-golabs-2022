package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// 实现排序接口
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go 调用的函数
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// 1. 告知Coordinator自己已经上线
	args := WorkerArgs{TaskType: "None"}
	reply := WorkerReply{TaskType: "None"}
	call("Coordinator.WorkerOnline", &args, &reply)
	WorkerID := reply.WorkerID

	// 心跳信号
	go func() {
		for {
			args := WorkerArgs{TaskType: "None"}
			args.Id = WorkerID
			reply := WorkerReply{TaskType: "None"}
			time.Sleep(time.Second * 5)
			call("Coordinator.WorkerAlive", &args, &reply)
		}
	}()

	// 无限循环向Coordinator请求任务
	for {
		// 2. 向Coordinator请求任务
		args = WorkerArgs{TaskType: "None"}
		args.Id = WorkerID
		reply = WorkerReply{TaskType: "None"}
		ok := call("Coordinator.AsssignTask", &args, &reply)

		if ok {

			fmt.Println("Call Success!")

			if reply.TaskType == "map" {

				fmt.Printf("Map Task!\n")

				// 读取文件，调用map函数进行处理
				intermediate := []KeyValue{}
				file, err := os.Open(reply.MapInput)
				if err != nil {
					log.Fatalf("cannot open %v", reply.MapInput)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", reply.MapInput)
				}
				file.Close()
				kva := mapf(reply.MapInput, string(content))
				intermediate = append(intermediate, kva...)

				// 循环创建NReduce个文件准备保存
				encoderList := make([]*json.Encoder, 0)
				for _, fileName := range reply.MapOutput {
					tempFile, err := os.Create(fileName)
					if err != nil {
						log.Fatalf("cannot create %v", fileName)
					}
					defer tempFile.Close()
					encoderList = append(encoderList, json.NewEncoder(tempFile))
				}
				// 将map后的结果存入文件中（最费时间）
				for i, v := range intermediate {
					encoderList[ihash(v.Key)%len(reply.MapOutput)].Encode(&intermediate[i])
				}

				// 3. 向Coordinator返回自己的Map任务已经完成
				args.TaskType = "map"
				args.Id = WorkerID
				args.Taskid = reply.Id
				call("Coordinator.TaskFinish", &args, &reply)

			} else if reply.TaskType == "reduce" {

				fmt.Printf("Reduce Task!\n")

				// 创建输出文件
				ofile, _ := os.Create(reply.ReduceOutput)

				// 遍历输入文件，汇总Map产生的所有结果
				kva := make([]KeyValue, 0)
				for _, filename := range reply.ReduceInput {
					// fmt.Println(filename)
					file, err := os.Open(filename)
					if err != nil {
						log.Fatalf("cannot open %v", filename)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}
				}

				// 排序
				sort.Sort(ByKey(kva))

				// 在已经排好序的键值对上进行统计，并写入到文件中
				i := 0
				for i < len(kva) {
					j := i + 1
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, kva[k].Value)
					}
					output := reducef(kva[i].Key, values)

					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

					i = j
				}

				// 4. 向Coordinator返回自己的Reduce任务已经完成
				args.Taskid = reply.Id
				args.Id = WorkerID
				args.TaskType = "reduce"
				call("Coordinator.TaskFinish", &args, &reply)

			} else if reply.TaskType == "finish" {

				// 5. 向Coordinator返回自己退出的消息
				call("Coordinator.WorkerFinish", &args, &reply)
				fmt.Printf("Bye!\n")
				return
			}
		} else {
			fmt.Printf("Call failed!\n")
		}

		// 间隔1秒请求一次
		time.Sleep(time.Second)
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
