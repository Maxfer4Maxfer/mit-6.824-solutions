package raft

import (
	"context"
	"log"
)

func (rf *Raft) AppendEntries(
	args *AppendEntriesArgs, reply *AppendEntriesReply,
) {
	log := rf.appendEntriesCreateLogger(args)
	ctx := AddCorrelationID(context.Background(), args.CorrelationID)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("<- %s {T:%d PLI:%d PLT:%d LC:%d len(Entries):%d}",
		rf.peerName(args.LeaderID), args.Term, args.PrevLogIndex, args.PrevLogTerm,
		args.LeaderCommit, len(args.Entries))

	log.Printf("Current state: {T:%d LC:%d LI:%d LII:%d len(log):%d}",
		rf.currentTerm, rf.commitIndex(), rf.log.LastIndex(),
		rf.log.lastIncludedIndex, len(rf.log.log))

	reply.Term = rf.currentTerm

	rf.processIncomingTerm(ctx, log, args.LeaderID, args.Term)
	rf.appendEntriesProcessArgs(ctx, log, args, reply)
}

func (rf *Raft) appendEntriesCreateLogger(args *AppendEntriesArgs) *log.Logger {
	var log *log.Logger

	if len(args.Entries) == 0 {
		log = ExtendLoggerWithTopic(rf.logger, LoggerTopicHeartbeating)
	} else {
		log = ExtendLoggerWithTopic(rf.logger, LoggerTopicAppendEntries)
	}

	log = ExtendLoggerWithCorrelationID(log, args.CorrelationID)

	return log
}

func (rf *Raft) appendEntriesProcessArgs(
	ctx context.Context, log *log.Logger,
	args *AppendEntriesArgs, reply *AppendEntriesReply,
) {
	switch {
	case args.Term < rf.currentTerm:
		log.Printf("Incoming term smaller %d < %d", args.Term, rf.currentTerm)

		reply.Success = false
	case rf.log.LastIndex() < args.PrevLogIndex:
		log.Printf("log is smaller %d < %d", rf.log.LastIndex(), args.PrevLogIndex)

		reply.Success = false

		rf.leaderElection.ResetTicker()

		reply.ConflictIndex = rf.log.LastIndex()
		reply.ConflictTerm = -1
	case args.PrevLogIndex+len(args.Entries) <= rf.commitIndex():
		log.Printf("Up-to-date PLL:%d + len(Entrins):%d <= CI:%d",
			args.PrevLogIndex, len(args.Entries), rf.commitIndex())

		reply.Success = true

		rf.leaderElection.ResetTicker()
		rf.appendEntriesProcessLeaderCommit(log, args)
	case args.PrevLogIndex < rf.log.lastIncludedIndex:
		log.Printf("Incomming entries in local snapshot PLI:%d < LII:%d",
			args.PrevLogIndex, rf.log.lastIncludedIndex)

		reply.Success = false
		reply.ConflictIndex = rf.log.lastIncludedIndex
		reply.ConflictTerm = rf.log.lastIncludedTerm
	case rf.log.Term(args.PrevLogIndex) != args.PrevLogTerm:
		log.Printf("Log discrepancy %d != %d",
			rf.log.Term(args.PrevLogIndex), args.PrevLogTerm)

		reply.Success = false

		rf.leaderElection.ResetTicker()

		reply.ConflictTerm = rf.log.Term(args.PrevLogIndex)

		for i := args.PrevLogIndex - 1; i > rf.log.lastIncludedIndex; i-- {
			if rf.log.Term(i) == reply.ConflictTerm {
				continue
			}

			reply.ConflictIndex = i + 1

			break
		}

		if reply.ConflictIndex == 0 {
			reply.ConflictIndex = rf.log.lastIncludedIndex + 1
		}
	default:
		reply.Success = true

		rf.leaderElection.ResetTicker()
		rf.appendEntriesProcessIncomingEntries(ctx, log, args)
		rf.appendEntriesProcessLeaderCommit(log, args)
	}
}

func (rf *Raft) appendEntriesProcessIncomingEntries(
	ctx context.Context, log *log.Logger, args *AppendEntriesArgs,
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

		if rf.log.Term(args.PrevLogIndex+(i+1)) != args.Entries[i].Term {
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
		rf.persist(ctx)
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
	ctx context.Context,
	log *log.Logger,
	peerID int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply,
	index int,
) syncProcessReplyReturn {
	log.Printf("<- %s {T:%d S:%v CI:%d CT:%d}", rf.peerName(peerID),
		reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm)

	rf.mu.Lock()

	switch {
	case args.Term < rf.currentTerm:
		log.Printf("ApplyEntries reply from the previous term %d < %d",
			args.Term, rf.currentTerm)
		rf.mu.Unlock()

		return syncProcessReplyReturnFailed
	case reply.Term > rf.currentTerm:
		log.Printf("%s has a higher term %d > %d",
			rf.peerName(peerID), reply.Term, rf.currentTerm)

		if rf.heartbeats.IsSendingInProgress() {
			rf.heartbeats.StopSending()
			rf.leaderElection.ResetTicker()
		} else {
			rf.leaderElection.StopLeaderElection()
		}

		rf.votedFor = -1
		rf.currentTerm = reply.Term

		rf.persist(ctx)

		rf.mu.Unlock()

		return syncProcessReplyReturnFailed
	// shorter follower's log
	case !reply.Success && reply.ConflictTerm == -1:
		if reply.ConflictIndex < rf.log.FirstIndex() {
			rf.nextIndex[peerID] = rf.log.FirstIndex()
			rf.mu.Unlock()

			rf.syncSnapshot(ctx, peerID)

			return syncProcessReplyReturnRetry
		}

		if reply.ConflictIndex == 0 {
			rf.nextIndex[peerID] = 1
		} else {
			rf.nextIndex[peerID] = reply.ConflictIndex
		}

		log.Printf("nextIndex[%d] = %d", peerID, reply.ConflictIndex)

		rf.mu.Unlock()

		return syncProcessReplyReturnRetry
	case !reply.Success && reply.ConflictTerm != -1:
		found := false

		for i := args.PrevLogIndex - 1; i > 0; i-- {
			if i < rf.log.FirstIndex() {
				rf.mu.Unlock()
				rf.syncSnapshot(ctx, peerID)

				return syncProcessReplyReturnRetry
			}

			if rf.log.Term(i) == reply.ConflictTerm {
				rf.nextIndex[peerID] = i + 1
				found = true

				break
			}

			if rf.log.Term(i) < reply.ConflictTerm {
				break
			}
		}

		if !found {
			rf.nextIndex[peerID] = reply.ConflictIndex
		}

		log.Printf("nextIndex[%d] = %d", peerID, rf.nextIndex[peerID])

		rf.mu.Unlock()

		return syncProcessReplyReturnRetry
	default:
		if rf.nextIndex[peerID] < index+1 {
			rf.nextIndex[peerID] = index + 1
		}

		rf.updateMatchIndex(ctx, peerID, index)

		rf.mu.Unlock()

		return syncProcessReplyReturnSucess
	}
}

func (rf *Raft) sync(
	ctx context.Context,
	log *log.Logger,
	peerID int,
	index int,
	args *AppendEntriesArgs,
) bool {
	for {
		rf.mu.Lock()

		if !rf.heartbeats.IsSendingInProgress() {
			rf.mu.Unlock()

			return false
		}

		if args.Term != rf.currentTerm {
			log.Printf("Sync from the past %d != %d", args.Term, rf.currentTerm)
			rf.mu.Unlock()

			return false
		}

		if rf.nextIndex[peerID] > index+1 {
			log.Printf("%s already updated by someone else", rf.peerName(peerID))
			rf.mu.Unlock()

			return true
		}

		if rf.nextIndex[peerID]-1 < rf.log.lastIncludedIndex {
			rf.mu.Unlock()
			rf.syncSnapshot(ctx, peerID)

			continue
		}

		args.PrevLogIndex = rf.nextIndex[peerID] - 1
		args.PrevLogTerm = rf.log.Term(args.PrevLogIndex)
		args.Entries = rf.log.Frame(rf.nextIndex[peerID], index+1)

		rf.mu.Unlock()

		log.Printf("-> %s {T:%d PLI:%d PLT:%d LC:%d len(Entries):%d}",
			rf.peerName(peerID), args.Term, args.PrevLogIndex, args.PrevLogTerm,
			args.LeaderCommit, len(args.Entries))

		reply := &AppendEntriesReply{}

		if ok := rf.sendAppendEntries(peerID, args, reply); !ok {
			log.Printf("WRN fail AppendEntries call to %d peer", peerID)

			continue
		}

		a := rf.syncProcessReply(ctx, log, peerID, args, reply, index)
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
