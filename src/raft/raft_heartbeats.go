package raft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type heartbeatsEngine struct {
	log                *log.Logger
	run                int32 // 1 - run; 0 - stop running
	cond               *sync.Cond
	broadcastTime      time.Duration
	sendHeartbeatsFunc func()
}

func initHeartbeatsEngine(
	logger *log.Logger,
	broadcastTime time.Duration,
	sendHeartbeatsFunc func(),
) *heartbeatsEngine {
	hbe := &heartbeatsEngine{
		log:                extendLoggerWithPrefix(logger, heartbeatingLogTopic),
		run:                0,
		cond:               sync.NewCond(&sync.Mutex{}),
		broadcastTime:      broadcastTime,
		sendHeartbeatsFunc: sendHeartbeatsFunc,
	}

	go hbe.processing()

	return hbe
}

func (hbe *heartbeatsEngine) StartSending() {
	hbe.log.Print("Start sending hearbeats")

	atomic.StoreInt32(&hbe.run, 1)
	hbe.cond.Broadcast()
}

func (hbe *heartbeatsEngine) StopSending() {
	if atomic.LoadInt32(&hbe.run) == 1 {
		hbe.log.Print("Stop sending heartbeats")
		atomic.StoreInt32(&hbe.run, 0)
	}
}

func (hbe *heartbeatsEngine) IsSendingInProgress() bool {
	if atomic.LoadInt32(&hbe.run) == 1 {
		return true
	}

	return false
}

func (hbe *heartbeatsEngine) processing() {
	hbe.log.Print("Start heartbeats engine")

	hbe.cond.L.Lock()

	for {
		switch {
		case atomic.LoadInt32(&hbe.run) == 0:
			hbe.cond.Wait()
		default:
			hbe.sendHeartbeatsFunc()

			time.Sleep(broadcastTime)
		}
	}
}

func (rf *Raft) sendHeartbeats() {
	log := extendLoggerWithPrefix(rf.logger, heartbeatingLogTopic)

	rf.mu.Lock()
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: len(rf.log) - 1,
		PrevLogTerm:  rf.log[len(rf.log)-1].Term,
		LeaderCommit: rf.commitIndex(),
	}
	rf.mu.Unlock()

	log.Printf("Hearbeats for term %d", args.Term)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(peerID int) {
			reply := &AppendEntriesReply{}

			log.Printf("Hearbeats to S%d (term %d) %+v", peerID, args.Term, *args)

			if ok := rf.sendAppendEntries(peerID, args.DeepCopy(), reply); !ok {
				log.Printf("Fail hearbeating to %d peer (term %d)",
					peerID, args.Term)
				return
			}

			rf.mu.Lock()
			rf.processIncomingTerm(log, peerID, reply.Term)
			rf.mu.Unlock()

			// rf.mu.Lock()
			// if !reply.Success {
			// 	rf.nextIndex[peerID]--
			// }
			// rf.mu.Unlock()
		}(i)
	}
}
