package raft

import "log"

func (rf *Raft) AppendEntries(
	args *AppendEntriesArgs, reply *AppendEntriesReply,
) {
	log := rf.appendEntriesCreateLogger(args)

	log.Printf("<- S%d {T:%d PLI:%d PLT:%d LC:%d len(Entries):%d}",
		args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm,
		args.LeaderCommit, len(args.Entries))

	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("Current state: {T:%d LC:%d LI:%d}",
		rf.currentTerm, rf.commitIndex(), rf.log.LastIndex())

	reply.Term = rf.currentTerm

	rf.processIncomingTerm(args.CorrelationID, log, args.LeaderID, args.Term)

	if ok := rf.appendEntriesCheckArgs(log, args, reply); !ok {
		return
	}

	reply.Success = true

	rf.leaderElection.ResetTicker()

	rf.appendEntriesProcessIncomingEntries(log, args)
	rf.appendEntriesProcessLeaderCommit(log, args)
}

func (rf *Raft) appendEntriesCreateLogger(args *AppendEntriesArgs) *log.Logger {
	var log *log.Logger

	if len(args.Entries) == 0 {
		log = extendLoggerWithTopic(rf.logger, heartbeatingLogTopic)
	} else {
		log = extendLoggerWithTopic(rf.logger, appendEntriesLogTopic)
	}

	log = extendLoggerWithCorrelationID(log, args.CorrelationID)

	return log
}

func (rf *Raft) appendEntriesCheckArgs(
	log *log.Logger, args *AppendEntriesArgs, reply *AppendEntriesReply,
) bool {
	switch {
	case args.Term < rf.currentTerm:
		log.Printf("Incoming term smaller %d < %d", args.Term, rf.currentTerm)

		reply.Success = false

		return false
	case rf.log.LastIndex() < args.PrevLogIndex:
		log.Printf("log is smaller %d < %d", rf.log.LastIndex(), args.PrevLogIndex)

		rf.leaderElection.ResetTicker()

		reply.ConflictIndex = rf.log.LastIndex()
		reply.ConflictTerm = -1
		reply.Success = false

		return false
	case rf.Log(args.PrevLogIndex).Term != args.PrevLogTerm:
		log.Printf("Log discrepancy %d != %d",
			rf.Log(args.PrevLogIndex).Term, args.PrevLogTerm)

		rf.leaderElection.ResetTicker()

		reply.ConflictTerm = rf.Log(args.PrevLogIndex).Term

		for i := args.PrevLogIndex - 1; i > 0; i-- {
			if rf.Log(i).Term == reply.ConflictTerm {
				continue
			}

			reply.ConflictIndex = i + 1

			break
		}

		reply.Success = false

		return false
	}

	return true
}

func (rf *Raft) appendEntriesProcessIncomingEntries(
	log *log.Logger, args *AppendEntriesArgs,
) {
	if len(args.Entries) == 0 {
		return
	}

	index := 0
	add := false

	// shrink local log
	for i := range args.Entries {
		if args.PrevLogIndex+(i+1) > rf.log.LastIndex() {
			index = i
			add = true

			log.Printf("Take entries from %d", index)

			break
		}

		if rf.Log(args.PrevLogIndex+(i+1)).Term != args.Entries[i].Term {
			log.Printf("Shrink local log [:%d] LI: %d",
				args.PrevLogIndex+(i+1), rf.log.LastIndex())

			rf.log.RightShrink(args.PrevLogIndex + (i + 1))

			add = true

			break
		}
	}

	if add {
		log.Printf("Append [%d:]", index)
		rf.log.Append(args.Entries[index:]...)
		rf.persist(args.CorrelationID)
	} else {
		log.Printf("No new entries")
	}
}

func (rf *Raft) appendEntriesProcessLeaderCommit(
	log *log.Logger, args *AppendEntriesArgs,
) {
	if args.LeaderCommit <= rf.commitIndex() {
		return
	}

	log.Printf("Incoming CommitIndex is higher %d > %d",
		args.LeaderCommit, rf.commitIndex())

	min := args.LeaderCommit

	if len(args.Entries) != 0 && args.LeaderCommit > rf.log.LastIndex() {
		min = rf.log.LastIndex()
	}

	rf.setCommitIndex(log, min)
}

type syncProcessReplyReturn int

const (
	syncProcessReplyReturnSucess syncProcessReplyReturn = iota
	syncProcessReplyReturnFailed
	syncProcessReplyReturnRetry
)

func (rf *Raft) syncProcessReply(
	log *log.Logger,
	peerID int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply,
	index int,
) syncProcessReplyReturn {
	log.Printf("<- S%d {T:%d S:%v CI:%d CT:%d}",
		peerID, reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch {
	case args.Term < rf.currentTerm:
		log.Printf("ApplyEntries reply from the previous term %d < %d",
			args.Term, rf.currentTerm)

		return syncProcessReplyReturnFailed
	case reply.Term > rf.currentTerm:
		log.Printf("S%d has a higher term %d > %d",
			peerID, reply.Term, rf.currentTerm)

		if rf.heartbeats.IsSendingInProgress() {
			rf.heartbeats.StopSending()
			rf.leaderElection.ResetTicker()
		} else {
			rf.leaderElection.StopLeaderElection()
		}

		rf.votedFor = -1
		rf.currentTerm = reply.Term

		rf.persist(args.CorrelationID)

		return syncProcessReplyReturnFailed
	// shorter follower's log
	case !reply.Success && reply.ConflictTerm == -1:
		if reply.ConflictIndex == 0 {
			rf.nextIndex[peerID] = 1
		} else {
			rf.nextIndex[peerID] = reply.ConflictIndex
		}

		log.Printf("nextIndex[%d] = %d", peerID, reply.ConflictIndex)

		return syncProcessReplyReturnRetry
	case !reply.Success && reply.ConflictTerm != -1:
		found := false

		for i := args.PrevLogIndex - 1; i > 0; i-- {
			if rf.Log(i).Term == reply.ConflictTerm {
				rf.nextIndex[peerID] = i + 1
				found = true

				break
			}

			if rf.Log(i).Term < reply.ConflictTerm {
				break
			}
		}

		if !found {
			rf.nextIndex[peerID] = reply.ConflictIndex
		}

		log.Printf("nextIndex[%d] = %d", peerID, rf.nextIndex[peerID])

		return syncProcessReplyReturnRetry
	default:
		if rf.nextIndex[peerID] < index+1 {
			rf.nextIndex[peerID] = index + 1
		}

		rf.updateMatchIndex(peerID, index)

		return syncProcessReplyReturnSucess
	}
}

func (rf *Raft) sync(
	log *log.Logger,
	peerID int,
	index int,
	args *AppendEntriesArgs,
) bool {
	reply := &AppendEntriesReply{}

	for {
		rf.mu.Lock()

		if !rf.heartbeats.IsSendingInProgress() {
			rf.mu.Unlock()

			return false
		}

		if rf.nextIndex[peerID] > index+1 {
			log.Printf("S%d already updated by someone else", peerID)
			rf.mu.Unlock()

			return true
		}

		args.PrevLogIndex = rf.nextIndex[peerID] - 1
		args.PrevLogTerm = rf.Log(args.PrevLogIndex).Term
		args.Entries = rf.log.Frame(rf.nextIndex[peerID], index+1)

		rf.mu.Unlock()

		log.Printf("-> S%d {T:%d PLI:%d PLT:%d LC:%d len(Entries):%d}",
			peerID, args.Term, args.PrevLogIndex, args.PrevLogTerm,
			args.LeaderCommit, len(args.Entries))

		if ok := rf.sendAppendEntries(peerID, args, reply); !ok {
			log.Printf("WRN fail AppendEntries call to %d peer", peerID)

			continue
		}

		a := rf.syncProcessReply(log, peerID, args, reply, index)
		switch a {
		case syncProcessReplyReturnRetry:
			continue
		case syncProcessReplyReturnFailed:
			return false
		case syncProcessReplyReturnSucess:
			return true
		}
	}
}
