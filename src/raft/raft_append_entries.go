package raft

import "log"

func (rf *Raft) AppendEntries(
	args *AppendEntriesArgs, reply *AppendEntriesReply,
) {
	log := rf.appendEntriesCreateLogger(args)

	log.Printf("AppendEntries from S%d %+v", args.LeaderID, args)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	rf.processIncomingTerm(log, args.LeaderID, args.Term)

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

			log.Printf("take entries from %d", index)

			break
		}

		if rf.log[args.PrevLogIndex+(i+1)].Term != args.Entries[i].Term {
			log.Printf("shrink local log [:%d] len(rf.log): %d",
				args.PrevLogIndex+(i+1), len(rf.log))

			rf.log = rf.log[:args.PrevLogIndex+(i+1)]
			add = true

			break
		}
	}

	if add {
		log.Printf("append [%d:]", index)
		rf.log = append(rf.log, args.Entries[index:]...)
	} else {
		log.Printf("no new entries")
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

	rf.setCommitIndex(min)
}
