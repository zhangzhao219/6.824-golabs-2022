package raft

import (
	"log"
	"sync/atomic"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// 转为Leader
func (rf *Raft) toLeader() {
	DPrintf("[%d]: convert from [%s] to [%s], term [%d]", rf.me, rf.state, Leader, rf.currentTerm)
	rf.state = Leader
	rf.lastReceive = time.Now().Unix()
	// 选举为Leader后重新对所有的peer进行初始化
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = -1
	}
}

// 转为Follower
func (rf *Raft) toFollower(newTerm int) {
	DPrintf("[%d]: convert from [%s] to [%s]", rf.me, rf.state, Follower)
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.lastReceive = time.Now().Unix()
}

// 转为Candidate
func (rf *Raft) toCandidate() {
	DPrintf("[%d]: convert from [%s] to [%s]", rf.me, rf.state, Candidate)
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.lastReceive = time.Now().Unix()
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = false
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}
	return term, isleader
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	if !rf.killed() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == Leader {
			isLeader = true
			// 只有是Leader才可以接收日志信息
			// 添加日志信息
			rf.log = append(rf.log, LogEntry{
				Term:    rf.currentTerm,
				Command: command,
			})
			index = len(rf.log) - 1
			term = rf.currentTerm
			rf.matchIndex[rf.me] = index    // 已经复制给他的日志的最高索引值
			rf.nextIndex[rf.me] = index + 1 // 需要发送给他的下⼀个日志条目的索引值
		}
		// 论文与代码起始位置索引不同
		index += 1
	}

	return index, term, isLeader
}

func (rf *Raft) sendApplyMsg() {
	rf.moreApply = true
	rf.applyCond.Broadcast()
}

func (rf *Raft) appMsgApplier() {
	for {
		rf.mu.Lock()
		// 等待这个字段为真才可以继续
		for !rf.moreApply {
			rf.applyCond.Wait()
		}
		rf.moreApply = false

		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		entries := rf.log
		rf.mu.Unlock()
		// 发送已经提交但是还没有返回的日志字段
		for i := lastApplied + 1; i <= commitIndex; i++ {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entries[i].Command,
				CommandIndex: i + 1,
			}
			DPrintf("[%d]: apply index %d - 1", rf.me, msg.CommandIndex)
			rf.applyCh <- msg
			// 及时加锁更新，否则可能会变化
			rf.mu.Lock()
			rf.lastApplied = i
			rf.mu.Unlock()
		}
	}
}
