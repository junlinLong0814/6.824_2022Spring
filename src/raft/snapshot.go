package raft

type SnapShotArgs struct {
	Term              int    //leader's term
	LeaderId          int    //leader's id
	LastIncludedIndex int    //the snapshot replaces all entries up through and incluing this index
	LastIncludedTerm  int    //term of LastIncludedIndex
	Data              []byte //raw bytes of the sanpshot chunk,starting at offset

	/*if use chunk*/
	Done   bool //true if this is the last chunk
	Offset int  //byte offset where chunk is posistioned in the sanpshot file

}

type SnapShotReply struct {
	Term int //currentTerm, for leader to update itself
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.logs[0].Index {
		//avoid idx out of range
		return
	}

	indexInLog := rf.getIndexByAbsoluteIndex(index)
	newLogLen := len(rf.logs) - indexInLog
	newLog := make([]LogEntry, newLogLen)
	copy(newLog, rf.logs[indexInLog:])
	//save the index log and set it dummy
	//because it can find the snapshot.lastidx
	newLog[0].Command = nil
	rf.logs = newLog

	data := serialization(rf)
	rf.persister.SaveStateAndSnapshot(data, snapshot)

}

func (rf *Raft) CallSendInstallSnapshot(peer int, term int, leader int, snapShot []byte, lastIndex int, lastTerm int) {
	args := SnapShotArgs{
		LeaderId:          leader,
		Term:              term,
		Data:              snapShot,
		LastIncludedIndex: lastIndex,
		LastIncludedTerm:  lastTerm,
	}
	SnapInfo("[%d] send [%d] a snapshot{LastIncludedIndex:%d,LastIncludedTerm:%d,Term:%d}\n",
		leader, peer, lastIndex, lastTerm, term)
	reply := SnapShotReply{}

	ok := rf.sendInstallSnapshot(peer, &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			//peer's term newer than mine
			//i should change to be a follower
			rf.changeState(Follower, reply.Term, -1)
			rf.resetElectionTimer()
			return
		}

		if rf.currentTerm > reply.Term {
			return
		}

		if args.Term != rf.currentTerm {
			//out of date reply
			return
		}

		//update peer's nextIndex and matchIndex
		if rf.matchIndex[peer] < lastIndex {
			rf.matchIndex[peer] = lastIndex
			rf.nextIndex[peer] = lastIndex + 1
		}
	}
}

func (rf *Raft) InstallSnapshot(args *SnapShotArgs, reply *SnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	SnapInfo("[%d] recv [%d] snapshot{LastIncludedIndex:%d,LastIncludedTerm:%d,Term:%d}\n",
		rf.me, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm, args.Term)
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		return
	}

	//compare leader's snapshot and my snapshot
	if rf.logs[0].Index >= args.LastIncludedIndex {
		//my sanpshot longer than leaders'
		//ignore leader's rpc
		SnapInfo("[%d] ignore [%d] snapshot\n")
		return
	} else {
		rf.commitQueue = append(rf.commitQueue, ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
			Snapshot:      args.Data,
		})

		lastLogIndex := rf.logs[len(rf.logs)-1].Index
		//apply the snapshot to service
		if lastLogIndex <= args.LastIncludedIndex {
			//all logs out of date
			newLogs := make([]LogEntry, 0)
			newLogs = append(newLogs, LogEntry{
				Command: nil,
				Index:   args.LastIncludedIndex,
				Term:    args.LastIncludedTerm,
			})
			rf.logs = newLogs
			SnapInfo("[%d] oldCommitIndex:[%d],new[%d]\n", rf.me, rf.commitIndex, args.LastIncludedIndex)
			rf.commitIndex = args.LastIncludedIndex
		} else {
			newLogs := make([]LogEntry, lastLogIndex-args.LastIncludedIndex+1)
			copy(newLogs, rf.logs[args.LastIncludedIndex-rf.logs[0].Index:])
			newLogs[0].Command = nil
			rf.logs = newLogs

			//maybe new leader's snapshot idx less than my commitidx
			//if true,can't update commitIndex
			if args.LastIncludedIndex > rf.commitIndex {
				SnapInfo("[%d] oldCommitIndex:[%d],new[%d]\n", rf.me, rf.commitIndex, args.LastIncludedIndex)
				rf.commitIndex = args.LastIncludedIndex
			} else {
				SnapInfo("[%d] oldCommitIndex:[%d],new not change\n", rf.me, rf.commitIndex)
				//3b Bug
				// if there is some entry between lastIncludeEntry and origin commitIndex
				// need to recommit them
				// it not commit them, the kvserver's latestRes will lose those result
				for i := args.LastIncludedIndex + 1; i <= rf.commitIndex; i++ {
					rf.commitQueue = append(rf.commitQueue, ApplyMsg{
						CommandValid: true,
						CommandIndex: i,
						Command:      rf.logs[rf.getIndexByAbsoluteIndex(i)].Command,
					})
				}
			}
		}
		data := serialization(rf)
		rf.persister.SaveStateAndSnapshot(data, args.Data)
		rf.cv.Broadcast()
	}

	if args.Term > rf.currentTerm || rf.myState != Follower {
		rf.changeState(Follower, args.Term, -1)
	}
	rf.resetElectionTimer()

}

func (rf *Raft) sendInstallSnapshot(peer int, args *SnapShotArgs, reply *SnapShotReply) bool {
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).

	return true
}
