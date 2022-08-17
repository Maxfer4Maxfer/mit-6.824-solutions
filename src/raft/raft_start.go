package raft

import (
	"log"
	"sync"
)

func (rf *Raft) startProcessAnswers(
	log *log.Logger, index int, resultCh chan struct{},
) {
	done := false
	cAnswers := 1 // already count itself

	for range resultCh {
		if done {
			continue
		}

		cAnswers++

		log.Printf("(%d/%d) answers", cAnswers, len(rf.peers))

		if cAnswers == len(rf.peers)/2+1 {
			log.Printf("try increase commitIndex to %d", index)

			rf.mu.Lock()
			differentTerm := rf.log[index].Term != rf.currentTerm
			rf.mu.Unlock()

			if differentTerm {
				log.Printf("Previous term detected")
				done = true

				continue
			}

			rf.setCommitIndex(log, index)

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

	rf.mu.Lock()
	if !rf.heartbeats.IsSendingInProgress() {
		log.Printf("Not a leader")

		ct := rf.currentTerm
		rf.mu.Unlock()

		return -1, ct, false
	}

	args := &AppendEntriesArgs{
		CorrelationID: correlationID,
		Term:          rf.currentTerm,
		LeaderID:      rf.me,
		LeaderCommit:  rf.commitIndex(),
	}

	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
	rf.persist(correlationID)
	index := len(rf.log) - 1
	rf.mu.Unlock()

	log.Printf("append to index: %d", index)

	wg := &sync.WaitGroup{}
	resultCh := make(chan struct{}) // true - ok false - cancel everything

	wg.Add(len(rf.peers) - 1)

	for peerID := range rf.peers {
		if peerID == rf.me {
			continue
		}

		go func(peerID int) {
			defer wg.Done()

			ok := rf.sync(log, peerID, index, args.DeepCopy())
			if ok {
				resultCh <- struct{}{}
			}
		}(peerID)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	go rf.startProcessAnswers(log, index, resultCh)

	return index, args.Term, true
}
