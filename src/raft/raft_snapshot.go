package raft

import (
	"bytes"

	"6.824/labgob"
)

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(
	lastIncludedTerm int, lastIncludedIndex int, snapshot []byte,
) bool {
	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	log := extendLoggerWithTopic(rf.logger, snapshotLogTopic)
	log = extendLoggerWithCorrelationID(log, CorrelationID())
	log.Printf("Snapshot")

	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("Snapshot for index %d {CT:%d, LI:%d len(log):%d OF:%d}",
		index, rf.currentTerm, rf.log.LastIndex(), len(rf.log.log), rf.log.offset)

	if index <= rf.log.lastIncludedIndex {
		log.Printf("Snapshot already applied index:%d <= LII:%d",
			index, rf.log.lastIncludedIndex)

		return
	}

	rf.log.lastIncludedIndex = index
	rf.log.lastIncludedTerm = rf.log.Term(index)

	rf.log.LeftShrink(index)

	log.Printf("Log lshrink {LI:%d len(log):%d OF:%d}",
		rf.log.LastIndex(), len(rf.log.log), rf.log.offset)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log.log)
	e.Encode(rf.log.offset)

	state := w.Bytes()

	rf.setCommitIndex(log, index)

	rf.persister.SaveStateAndSnapshot(state, snapshot)
}

func (rf *Raft) syncSnapshot(correlationID string, peerID int) {
	log := extendLoggerWithTopic(rf.logger, installSnapshotLogTopic)
	log = extendLoggerWithCorrelationID(log, correlationID)

	reply := &InstallSnapshotReply{}

	for {
		rf.mu.Lock()
		if !rf.heartbeats.IsSendingInProgress() {
			break
		}

		args := &InstallSnapshotArgs{
			CorrelationID:     correlationID,
			Term:              rf.currentTerm,
			LeaderID:          rf.me,
			LastIncludedIndex: rf.log.lastIncludedIndex,
			LastIncludedTerm:  rf.log.lastIncludedTerm,
			Offset:            0,
			Data:              rf.persister.ReadSnapshot(),
			Done:              true,
		}
		rf.mu.Unlock()

		log.Printf("-> S%d {T:%d LII:%d LIT:%d OF:%d len(data):%d Done:%v}",
			peerID, args.Term, args.LastIncludedIndex, args.LastIncludedTerm,
			args.Offset, len(args.Data), args.Done)

		if ok := rf.sendInstallSnapshot(peerID, args, reply); !ok {
			log.Printf("WRN fail InstallSnapshot call to %d peer", peerID)

			continue
		}

		log.Printf("<- S%d {T:%d}", peerID, reply.Term)

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			log.Printf("S%d has a higher term %d > %d",
				peerID, reply.Term, rf.currentTerm)

			if rf.heartbeats.IsSendingInProgress() {
				rf.heartbeats.StopSending()
				rf.leaderElection.ResetTicker()
			} else {
				rf.leaderElection.StopLeaderElection()
			}

			return
		}

		if rf.nextIndex[peerID] < args.LastIncludedIndex+1 {
			rf.nextIndex[peerID] = args.LastIncludedIndex + 1
		}

		rf.updateMatchIndex(correlationID, peerID, args.LastIncludedIndex)

		return
	}
}

func (rf *Raft) InstallSnapshot(
	args *InstallSnapshotArgs, reply *InstallSnapshotReply,
) {
	log := extendLoggerWithTopic(rf.logger, installSnapshotLogTopic)
	log = extendLoggerWithCorrelationID(log, args.CorrelationID)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("<- S%d {T:%d LII:%d LIT:%d OF:%d len(data):%d Done:%v}",
		args.LeaderID, args.Term, args.LastIncludedIndex, args.LastIncludedTerm,
		args.Offset, len(args.Data), args.Done)

	// 1. Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		log.Printf("Request from previous term %d < %d",
			args.Term, rf.currentTerm)

		return
	}

	if args.LastIncludedIndex <= rf.log.lastIncludedIndex {
		log.Printf("Nothing new in snapshot LII %d < %d",
			args.LastIncludedIndex, rf.log.lastIncludedIndex)

		return
	}

	// 2. Create new snapshot file if first chunk (offset is 0)
	// 3. Write data into snapshot file at given offset
	// 4. Reply and wait for more data chunks if done is false
	// 5. Save snapshot file, discard any existing or partial snapshot
	// with a smaller index

	// 6. If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply
	// 7. Discard the entire log

	rf.log.lastIncludedIndex = args.LastIncludedIndex
	rf.log.lastIncludedTerm = args.LastIncludedTerm

	rf.log.LeftShrink(args.LastIncludedIndex)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log.log)
	e.Encode(rf.log.offset)

	state := w.Bytes()

	rf.persister.SaveStateAndSnapshot(state, args.Data)

	// 8. Reset state machine using snapshot contents (and load
	// snapshot’s cluster configuration)
	log.Printf("-> applyCh {LII:%d LIT:%d len(data):%d}",
		args.LastIncludedIndex, args.LastIncludedTerm, len(args.Data))

	rf.setCommitIndex(log, args.LastIncludedIndex)

	rf.bufApplyCh <- ApplyMsg{
		CommandValid:  false,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotValid: true,
		Snapshot:      args.Data,
	}

	log.Printf("-> applyCh done")
}
