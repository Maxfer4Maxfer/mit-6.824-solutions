package raft

import (
	"log"
	"sync"
)

type startReplyAction int

const (
	startReplyActionRetry startReplyAction = iota
	startReplyActionAnotherFailedExit
	startReplyActionAnotherPossitiveExit
)

func (rf *Raft) startSyncWithPeerProcessReply(
	log *log.Logger,
	peerID int,
	originalTerm int,
	reply *AppendEntriesReply,
	index int,
) startReplyAction {
	log.Printf("<- S%d AppendEntiresReply %+v", peerID, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if originalTerm < rf.currentTerm {
		log.Printf("ApplyEntries reply from the previous term %d < %d",
			originalTerm, rf.currentTerm)

		return startReplyActionRetry
	}

	if ok := rf.processIncomingTerm(log, peerID, reply.Term); !ok {
		return startReplyActionAnotherFailedExit
	}

	if reply.Success {
		if rf.nextIndex[peerID] < index+1 {
			rf.nextIndex[peerID] = index + 1
		}

		if rf.matchIndex[peerID] < index {
			rf.updateMatchIndex(peerID, index)
		}
	} else {
		rf.nextIndex[peerID]--

		return startReplyActionRetry
	}

	return startReplyActionAnotherPossitiveExit
}

func (rf *Raft) startSyncWithPeer(
	log *log.Logger,
	wg *sync.WaitGroup,
	peerID int,
	resultCh chan struct{},
	args *AppendEntriesArgs,
	index int,
) {
	defer wg.Done()

	reply := &AppendEntriesReply{}

	for {
		if !rf.heartbeats.IsSendingInProgress() {
			return
		}

		rf.mu.Lock()

		if rf.nextIndex[peerID] > index+1 {
			log.Printf("S%d already updated by someoune else", peerID)
			resultCh <- struct{}{}
			rf.mu.Unlock()

			return
		}

		args.PrevLogIndex = rf.nextIndex[peerID] - 1
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		args.Entries = append(
			[]LogEntry(nil), rf.log[rf.nextIndex[peerID]:index+1]...)

		rf.mu.Unlock()

		log.Printf("-> S%d {Term:%d PrevLogIndex:%d "+
			"PrevLogTerm:%d len(Entries):%d LeaderCommit:%d}",
			peerID, args.Term, args.PrevLogIndex, args.PrevLogTerm,
			len(args.Entries), args.LeaderCommit)

		if ok := rf.sendAppendEntries(peerID, args, reply); !ok {
			log.Printf("WRN fail AppendEntries call to %d peer", peerID)

			continue
		}

		a := rf.startSyncWithPeerProcessReply(log, peerID, args.Term, reply, index)
		switch a {
		case startReplyActionRetry:
			continue
		case startReplyActionAnotherFailedExit:
			return
		case startReplyActionAnotherPossitiveExit:
			resultCh <- struct{}{}

			return
		}
	}
}

func (rf *Raft) startProcessAnswers(index int, resultCh chan struct{}) {
	done := false
	cAnswers := 1 // already count itself

	for range resultCh {
		if done {
			continue
		}

		cAnswers++

		log.Printf("(%d/%d) answers", cAnswers, len(rf.peers))

		if cAnswers == len(rf.peers)/2+1 {
			log.Printf("increasing comminIndex")

			rf.setCommitIndex(index)

			done = true
		}
	}
}

func (rf *Raft) startCreateLogger(correlationID string) *log.Logger {
	log := extendLoggerWithTopic(rf.logger, startLogTopic)

	log = extendLoggerWithCorrelationID(log, correlationID)

	return log
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
	correlationID := CorrelationID()
	log := rf.startCreateLogger(correlationID)

	log.Printf("Start call %v", command)

	if !rf.heartbeats.IsSendingInProgress() {
		log.Printf("Not a leader")

		rf.mu.Lock()
		ct := rf.currentTerm
		rf.mu.Unlock()

		return -1, ct, false
	}

	rf.mu.Lock()
	args := &AppendEntriesArgs{
		CorrelationID: correlationID,
		Term:          rf.currentTerm,
		LeaderID:      rf.me,
		LeaderCommit:  rf.commitIndex(),
	}

	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
	index := len(rf.log) - 1
	rf.mu.Unlock()

	log.Printf("append a local log")
	log.Printf("append to index: %d", index)

	wg := &sync.WaitGroup{}
	resultCh := make(chan struct{}) // true - ok false - cancel everything

	for peerID := range rf.peers {
		if peerID == rf.me {
			continue
		}

		wg.Add(1)

		go rf.startSyncWithPeer(log, wg, peerID, resultCh, args.DeepCopy(), index)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	go rf.startProcessAnswers(index, resultCh)

	return index, args.Term, true
}
