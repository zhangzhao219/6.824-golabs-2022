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
	// rf.lastReceive = time.Now().Unix()
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
	// rf.lastReceive = time.Now().Unix()
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
