package raft

import "time"

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int        // Leader的任期号
	LeaderId     int        // Follower可以通过这个LeaderId重定向客户端
	PrevLogIndex int        // 新的日志条目紧随之前的索引值
	PrevLogTerm  int        // PrevLogIndex日志条目的任期
	Entries      []LogEntry // 存储的日志条目，如果是心跳包则为空
	LeaderCommit int        // Leader的提交索引
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int  // 当前的任期，接收到了之后Leader可以更新自己
	Success bool // Follower包含了匹配上 prevLogIndex 和 prevLogTerm 的日志时为真
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	// RPC 请求不一定在什么时候应用，因此必须加锁
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 更新至少为当前的任期
	reply.Term = rf.currentTerm
	reply.Success = false
	// 如果Leader的任期还没有我的大，则直接拒绝请求
	if args.Term < rf.currentTerm {
		return
	}
	// 如果Leader的任期比我的大，则我转为这个任期的Follower
	if args.Term >= rf.currentTerm || rf.state == Candidate {
		rf.toFollower(args.Term)
	}
	// 如果Leader的任期和我的相同，则操作日志
	// Todo：日志操作
	rf.lastReceive = time.Now().Unix()
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 向指定的Peer增加日志条目或者发送心跳包
func (rf *Raft) appendEntriesToPeer(index int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	if ok := rf.sendAppendEntries(index, args, &reply); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// Todo：log相关
		// 如果响应的任期比Leader更大了，说明Leader需要退位成Follower了
		if reply.Term > rf.currentTerm {
			rf.toFollower(reply.Term)
		}
	}
}
