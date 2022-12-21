package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Worker向Coordinator传递的信息
type WorkerArgs struct {
	TimeStamp int64  // Worker的唯一时间戳
	Taskid    int    // 任务全局唯一ID
	TaskType  string // 任务类型
}

// Coordinator向Worker传递的信息
type WorkerReply struct {
	Id           int      // 任务id
	TaskType     string   // 任务类型
	MapInput     string   // Map任务的输入
	MapOutput    []string // Map任务的输出
	ReduceInput  []string // Reduce任务的输入
	ReduceOutput string   // Reduce任务的输出
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
