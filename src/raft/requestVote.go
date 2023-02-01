package raft

import (
	"sync"
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // Candidate的任期号
	CandidateId  int // Candidate的 Id
	LastLogIndex int // Candidate最后一条日志条目的索引
	LastLogTerm  int // Candidate最后一条日志条目的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前的任期，接收到了之后Candidate可以更新自己
	VoteGranted bool // 是否给这个Candidate投票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// RPC 请求不一定在什么时候应用，因此必须加锁
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d]: received vote request from [%d]", rf.me, args.CandidateId)

	reply.VoteGranted = false

	// 如果参数的任期号还没有我的大，不投票，直接默认值返回即可
	if args.Term < rf.currentTerm {
		// 响应中包含当前自己的任期号
		reply.Term = rf.currentTerm
		return
	}
	// 如果参数的任期号比我的大，则我在这个任期内就只能是它的Follower，则更改我的任期号，而且在这个任期内我要投票给它
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}
	reply.Term = rf.currentTerm // 注意这里任期号已经变化了，因此要重新赋值
	DPrintf("[%d]: status: term [%d], state [%s], vote for [%d]", rf.me, rf.currentTerm, rf.state, rf.votedFor)
	// 是否没投票或者投给的是这个candidate
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// candidate的log是否至少和接受者的log一样新
		// 1. 我的log长度为0，那我肯定投票给他了
		// 2. candidate的最后的log的任期比我的最后的log的任期大
		// 3. candidate的最后的log的任期和我的最后的log的任期相同，但是它的日志长度比我长
		if len(rf.log) == 0 || (args.LastLogTerm > rf.log[len(rf.log)-1].Term) ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1) {
			rf.votedFor = args.CandidateId
			rf.lastReceive = time.Now().Unix() // 更新时间，上面操作相当于与可能的Leader通信过了
			reply.VoteGranted = true
			DPrintf("[%d]: voted to [%d]", rf.me, args.CandidateId)
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 向每一个Peer请求投票
func (rf *Raft) requestVoteToPeer(index int, args *RequestVoteArgs, votesSum *int, votesGet *int, cond *sync.Cond) {

	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(index, args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer cond.Broadcast()
	*votesSum += 1
	if !ok {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.toFollower(reply.Term)
		// } else if reply.VoteGranted && reply.Term == rf.currentTerm {
	} else if reply.VoteGranted {
		*votesGet += 1
	}
}
