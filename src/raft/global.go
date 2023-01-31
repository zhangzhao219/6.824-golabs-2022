package raft

import (
	"sync"

	"6.824/labrpc"
)

// 定义Peer的状态
type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{} // 用户执行的命令
	Term    int         // 用户执行命令时候的任期号
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 在所有peer上面的持久性的状态
	// 在对RPC进行响应之后要在稳定存储上更新
	currentTerm int // this peer 看到的最新的任期号
	votedFor    int // 在当前任期获得选票的Candidate的id（如果没有则为-1）

	log []LogEntry // 日志信息

	// 在所有peer上面的变化的状态
	commitIndex int // 已知的已经被提交的日志条目的最大索引值
	lastApplied int // 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）

	// 在Leader上面的变化的状态
	// 每一次选举后都要重新进行初始化
	nextIndex  []int // 对于每⼀个服务器，需要发送给他的下⼀个日志条目的索引值（初始化为Leader最后索引值加1）
	matchIndex []int // 对于每⼀个服务器，已经复制给他的日志的最高索引值

	// 与时间相关的变量
	electTimeout     int64 // 选举超时时间
	randomTimeout    int64 // 随机时间
	heartBeatTimeout int64 // 心跳周期

	// 当前状态
	state        State // 当前Peer所处的状态（Leader、Candidate或Follower）
	majorityVote int   // 成为Leader需要获得的最少票数
	lastReceive  int64 // 最后一次接收到Leader的心跳信号的时间
}
