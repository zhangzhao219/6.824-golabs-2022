package raft

import (
	"sort"
	"time"
)

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

	// 快速回退
	XTerm  int
	XIndex int
	XLen   int
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

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex >= len(rf.log) || (args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.XLen = len(rf.log)
		if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.log) {
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			for i := args.PrevLogIndex; i >= 0; i-- {
				if rf.log[i].Term == reply.XTerm {
					reply.XIndex = i
				} else {
					break
				}
			}
		}
		return
	}
	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	misMatchIndex := -1
	for i := range args.Entries {
		if args.PrevLogIndex+1+i >= len(rf.log) || rf.log[args.PrevLogIndex+1+i].Term != args.Entries[i].Term {
			misMatchIndex = i
			break
		}
	}
	// Append any new entries not already in the log
	if misMatchIndex != -1 {
		rf.log = append(rf.log[:args.PrevLogIndex+1+misMatchIndex], args.Entries[misMatchIndex:]...)
	}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		newEntryIndex := len(rf.log) - 1
		if args.LeaderCommit >= newEntryIndex {
			rf.commitIndex = newEntryIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		DPrintf("[%d]: commit index [%d]", rf.me, rf.commitIndex)
		rf.sendApplyMsg()
	}

	rf.lastReceive = time.Now().Unix()
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 向指定的Peer增加日志条目或者发送心跳包
func (rf *Raft) appendEntriesToPeer(index int) {
	// 找到日志的同步位置
	prevLogIndex := rf.nextIndex[index] - 1
	prevLogTerm := -1
	if prevLogIndex != -1 {
		prevLogTerm = rf.log[prevLogIndex].Term
	}
	// 找到要发送的日志
	var entries []LogEntry
	if len(rf.log)-1 >= rf.nextIndex[index] {
		entries = rf.log[rf.nextIndex[index]:]
	}
	// 补充结构体
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
	}
	reply := AppendEntriesReply{}
	if ok := rf.sendAppendEntries(index, &args, &reply); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 如果响应的任期比Leader更大了，说明Leader需要退位成Follower了
		if reply.Term > rf.currentTerm {
			rf.toFollower(reply.Term)
			return
		}
		// 如果响应的任期相同，且服务器自己的状态还没有改变
		if reply.Term == rf.currentTerm && rf.currentTerm == args.Term {
			// 如果响应成功则说明follower包含了匹配prevLogIndex和prevLogTerm的日志项
			// 如果响应失败需要回退重试
			if reply.Success {

				// 更新服务器的状态
				rf.nextIndex[index] = prevLogIndex + len(entries) + 1
				rf.matchIndex[index] = prevLogIndex + len(entries)

				// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
				// set commitIndex = N
				matches := make([]int, len(rf.peers))
				copy(matches, rf.matchIndex)
				sort.Ints(matches)

				for i := rf.majorityVote - 1; i >= 0 && matches[i] > rf.commitIndex; i-- {
					if rf.log[matches[i]].Term == rf.currentTerm {
						rf.commitIndex = matches[i]
						DPrintf("[%d]: commit index [%d]", rf.me, rf.commitIndex)
						rf.sendApplyMsg()
						break
					}
				}
			} else {
				// In Test (2C): Figure 8 (unreliable), the AppendEntry RPCs are reordered
				// So rf.nextIndex[index]-- would be wrong
				rf.nextIndex[index] = prevLogIndex
				// 如果接下来要尝试匹配的prevLogIndex比follower当前所拥有的的日志长度（XLen）还要大，那么显然直接从XLen尝试匹配即可。
				if rf.nextIndex[index]-1 >= reply.XLen {
					rf.nextIndex[index] = reply.XLen
				} else {
					// 如果接下来要尝试匹配的prevLogIndex在XLen以内，因为我们已经知道了follower的日志从XIndex到当前prevLogIndex的日志项的term都是XTerm，那么我们可以直接在leader侧遍历匹配一遍，而无需多次往返RPC通信
					for i := rf.nextIndex[index] - 1; i >= reply.XIndex; i-- {
						if rf.log[i].Term != reply.XTerm {
							rf.nextIndex[index] -= 1
						} else {
							break
						}
					}
				}
			}
		}
	}
}
