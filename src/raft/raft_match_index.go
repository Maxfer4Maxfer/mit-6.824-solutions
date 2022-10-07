package raft

import (
	"context"
	"sync"
)

func (rf *Raft) updateMatchIndex(ctx context.Context, peerID int, index int) {
	if rf.matchIndex[peerID] < index {
		log := ExtendLogger(ctx, rf.logger, LoggerTopicMatchIndex)

		log.Printf("Update matchIndex for %s %d -> %d",
			rf.peerName(peerID), rf.matchIndex[peerID], index)

		rf.matchIndex[peerID] = index
		rf.matchIndexCond.Broadcast()
	}
}

func (rf *Raft) matchIndexHistogram() (map[int]int, int) {
	cMatchIndex := make(map[int]int) // [idx]count
	max := 0

	for _, idx := range rf.matchIndex {
		cMatchIndex[idx]++

		if idx > max {
			max = idx
		}
	}

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
	log := ExtendLoggerWithTopic(rf.logger, LoggerTopicMatchIndex)

	rf.matchIndexCond = sync.NewCond(&sync.Mutex{})
	rf.matchIndexCond.L.Lock()

	go func() {
		for {
			rf.matchIndexCond.Wait()

			rf.mu.Lock()

			rf.matchIndex[rf.me] = rf.log.LastIndex()

			if !rf.heartbeats.IsSendingInProgress() {
				rf.mu.Unlock()

				continue
			}

			hMatchIndex, max := rf.matchIndexHistogram()

			for N := max; N > rf.commitIndex(); N-- {
				if N <= rf.log.lastIncludedIndex {
					log.Printf("Index %d included in last snapshot", N)

					continue
				}

				if hMatchIndex[N] < len(rf.peers)/2+1 {
					log.Printf("Index %d not on server majority h[%d]:%d < %d",
						N, N, hMatchIndex[N], len(rf.peers)/2+1)

					continue
				}

				log.Printf("Found not committed index on server majority: %d", N)

				rf.setCommitIndex(log, N)

				break
			}
			rf.mu.Unlock()
		}
	}()
}
