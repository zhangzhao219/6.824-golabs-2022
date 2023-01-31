package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.824-golabs-2022/src/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) leaderElection() {

	lastElectTime := time.Now().Unix()

	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		time.Sleep(time.Duration(rf.electTimeout+rand.Int63n(rf.randomTimeout)) * time.Millisecond)

		rf.mu.Lock()
		// lastStartTime := startTime

		// 如果上一次循环到这里的实时时间比上一次接收到心跳包的时间还大，说明在睡眠时间内一直没有接收到心跳包，则认为超时
		if lastElectTime > rf.lastReceive {
			//DPrintf("[%d]: current state is [%s].", rf.me, rf.state)
			if rf.state != Leader {
				DPrintf("[%d]: is not leader, start election.", rf.me)
				rf.tryLeader()
			}
		}
		lastElectTime = time.Now().Unix() // 更新“上一次”的时间
		rf.mu.Unlock()
	}
}

func (rf *Raft) tryLeader() {
	rf.toCandidate()

	votesSum := 1                // 总共的票的数量
	votesGet := 1                // 收到的票数，自己首先给自己投票
	cond := sync.NewCond(&rf.mu) // 条件变量，控制投票结果的返回
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.requestVoteToPeer(i, &args, &votesSum, &votesGet, cond)
		}
	}
	// 等待票数统计完毕并判断是否能成为Leader
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		for votesGet < rf.majorityVote && votesSum < len(rf.peers) && rf.state == Candidate {
			cond.Wait()
		}
		if votesGet >= rf.majorityVote && rf.state == Candidate {
			rf.toLeader()
			// 发送心跳包
			go rf.logReplication()
		}
	}()
}

// Leader定时发送更新log的请求，同时也作为心跳包
func (rf *Raft) logReplication() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == Leader {
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go rf.appendEntriesToPeer(i, &args)
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(rf.heartBeatTimeout) * time.Millisecond)
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.toFollower(0)

	rf.electTimeout = 200     // 初始化选举超时时间
	rf.heartBeatTimeout = 100 // 初始化心跳间隔时间
	rf.randomTimeout = 100    // 设置随机时间的最大范围

	// 初始化成为Leader需要得到的票数
	if len(rf.peers)%2 == 0 {
		rf.majorityVote = len(rf.peers)/2 + 1
	} else {
		rf.majorityVote = (len(rf.peers) + 1) / 2
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.leaderElection()

	return rf
}
