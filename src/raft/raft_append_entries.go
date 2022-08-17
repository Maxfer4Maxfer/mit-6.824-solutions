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

	log.Printf("Current state: {T:%d LC:%d}", rf.currentTerm, rf.commitIndex())

	reply.Term = rf.currentTerm

	rf.processIncomingTerm(args.CorrelationID, log, args.LeaderID, args.Term)

	pass := rf.appendEntriesCheckArgs(log, args)
	if !pass {
		reply.Success = false

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
	log *log.Logger, args *AppendEntriesArgs,
) bool {
	switch {
	case args.Term < rf.currentTerm:
		log.Printf("Incoming term smaller %d < %d", args.Term, rf.currentTerm)

		return false
	case len(rf.log)-1 < args.PrevLogIndex:
		log.Printf("Smaller log size %d < %d", len(rf.log)-1, args.PrevLogIndex)

		rf.leaderElection.ResetTicker()

		return false
	case rf.log[args.PrevLogIndex].Term != args.PrevLogTerm:
		log.Printf("Log discrepancy %d != %d",
			rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)

		rf.leaderElection.ResetTicker()

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
		if args.PrevLogIndex+(i+1) > len(rf.log)-1 {
			index = i
			add = true

			log.Printf("Take entries from %d", index)

			break
		}

		if rf.log[args.PrevLogIndex+(i+1)].Term != args.Entries[i].Term {
			log.Printf("Shrink local log [:%d] len(rf.log): %d",
				args.PrevLogIndex+(i+1), len(rf.log))

			rf.log = rf.log[:args.PrevLogIndex+(i+1)]
			add = true

			break
		}
	}

	if add {
		log.Printf("Append [%d:]", index)
		rf.log = append(rf.log, args.Entries[index:]...)
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

	if len(args.Entries) != 0 && args.LeaderCommit > len(rf.log)-1 {
		min = len(rf.log) - 1
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
	log.Printf("<- S%d {T:%d S:%v}", peerID, reply.Term, reply.Success)

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

	case !reply.Success:
		if rf.nextIndex[peerID] >= args.PrevLogIndex+1 {
			rf.nextIndex[peerID]--
		}

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
		if !rf.heartbeats.IsSendingInProgress() {
			return false
		}

		rf.mu.Lock()

		if rf.nextIndex[peerID] > index+1 {
			log.Printf("S%d already updated by someone else", peerID)
			rf.mu.Unlock()

			return true
		}

		args.PrevLogIndex = rf.nextIndex[peerID] - 1
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		args.Entries = append(
			[]LogEntry(nil), rf.log[rf.nextIndex[peerID]:index+1]...)

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
