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
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := TaskInformation{}
		reply := TaskInformation{TaskType: "None"}
		ok := call("Coordinator.AsssignTask", &args, &reply)
		if ok {
			fmt.Println("Call Success!")
			if reply.TaskType == "map" {
				fmt.Printf("Map Task!\n")
				intermediate := []KeyValue{}
				file, err := os.Open(reply.InputFileName)
				if err != nil {
					log.Fatalf("cannot open %v", reply.InputFileName)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", reply.InputFileName)
				}
				file.Close()
				kva := mapf(reply.InputFileName, string(content))
				intermediate = append(intermediate, kva...)

				// 循环创建NReduce个文件准备保存
				encoderList := make([]*json.Encoder, 0)
				for i := 0; i < reply.NReduce; i++ {
					fileName := reply.OutputFileName + strconv.FormatInt(int64(i+1), 10)
					tempFile, err := os.Create(fileName)
					if err != nil {
						log.Fatalf("cannot create %v", fileName)
					}
					defer tempFile.Close()
					encoderList = append(encoderList, json.NewEncoder(tempFile))
				}
				for i, v := range intermediate {
					encoderList[ihash(v.Key)%reply.NReduce].Encode(&intermediate[i])
				}
				args = reply
				call("Coordinator.TaskFinish", &args, &reply)

			} else if reply.TaskType == "reduce" {

				ofile, _ := os.Create(reply.OutputFileName)

				fmt.Printf("Reduce Task!\n")
				kva := make([]KeyValue, 0)
				for p := 1; p <= 8; p++ {
					filename := strings.Replace(reply.InputFileName, "*", strconv.FormatInt(int64(p), 10), 1)
					fmt.Println(filename)
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
				//
				// call Reduce on each distinct key in intermediate[],
				// and print the result to mr-out-0.
				//
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

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

					i = j
				}
				args = reply
				call("Coordinator.TaskFinish", &args, &reply)
			}
		} else {
			fmt.Printf("Call failed!\n")
		}
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
