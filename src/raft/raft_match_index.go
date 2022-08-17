package raft

import "sync"

func (rf *Raft) updateMatchIndex(peerID int, index int) {
	rf.matchIndex[peerID] = index
	rf.matchIndexCond.Broadcast()
}

func (rf *Raft) matchIndexHistogram() (map[int]int, int) {
	cMatchIndex := make(map[int]int) // [idx]count
	max := 0

	rf.mu.Lock()

	for _, idx := range rf.matchIndex {
		cMatchIndex[idx]++

		if idx > max {
			max = idx
		}
	}

	rf.mu.Unlock()

	hMatchIndex := make(map[int]int, max) // [idx]count

	for idx := 0; idx <= max; idx++ {
		count := 0

		for i, c := range cMatchIndex {
			if i >= idx {
				count += c
			}
		}

		hMatchIndex[idx] = count
	}

	return hMatchIndex, max
}

func (rf *Raft) matchIndexProcessing() {
	log := extendLoggerWithTopic(rf.logger, matchIndexLogTopic)

	rf.matchIndexCond = sync.NewCond(&sync.Mutex{})
	rf.matchIndexCond.L.Lock()

	go func() {
		for {
			rf.matchIndexCond.Wait()

			if !rf.heartbeats.IsSendingInProgress() {
				continue
			}

			hMatchIndex, max := rf.matchIndexHistogram()

			rf.mu.Lock()
			for N := max; N > rf.commitIndex(); N-- {
				if rf.log[N].Term != rf.commitIndex() {
					continue
				}

				if hMatchIndex[N] < len(rf.peers)/2+1 {
					continue
				}

				rf.setCommitIndex(log, N)

				break
			}
			rf.mu.Unlock()
		}
	}()
}
