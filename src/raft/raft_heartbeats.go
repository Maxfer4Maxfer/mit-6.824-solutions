package raft

import (
	"context"
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
	sendHeartbeatsFunc func(cID CorrelationID)
}

func initHeartbeatsEngine(
	logger *log.Logger,
	broadcastTime time.Duration,
	sendHeartbeatsFunc func(cID CorrelationID),
) *heartbeatsEngine {
	hbe := &heartbeatsEngine{
		log:                ExtendLoggerWithTopic(logger, heartbeatingLogTopic),
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
		hbe.cond.Broadcast()
	}
}

func (hbe *heartbeatsEngine) IsSendingInProgress() bool {
	return atomic.LoadInt32(&hbe.run) == 1
}

func (hbe *heartbeatsEngine) processing() {
	hbe.log.Print("Start heartbeats engine")

	ticker := func(ctx context.Context) {
		ticker := time.NewTicker(broadcastTime)
		defer ticker.Stop()

		for ; true; <-ticker.C {
			select {
			case <-ctx.Done():
				return
			default:
				correlationID := NewCorrelationID()
				log := ExtendLoggerWithCorrelationID(hbe.log, correlationID)

				log.Printf("Hearbeats timer is triggered")

				go hbe.sendHeartbeatsFunc(correlationID)
			}
		}
	}

	hbe.cond.L.Lock()

	var cancelf context.CancelFunc

	for {
		switch {
		case atomic.LoadInt32(&hbe.run) == 0:
			if cancelf != nil {
				cancelf()
			}

			hbe.cond.Wait()
		case atomic.LoadInt32(&hbe.run) == 1:
			ctx, cancel := context.WithCancel(context.Background())

			go ticker(ctx)

			cancelf = cancel

			hbe.cond.Wait()
		}
	}
}

func (rf *Raft) sendHeartbeats(cID CorrelationID) {
	ctx := AddCorrelationID(context.Background(), cID)
	log := ExtendLogger(ctx, rf.logger, heartbeatingLogTopic)

	rf.mu.Lock()
	args := &AppendEntriesArgs{
		CorrelationID: cID,
		Term:          rf.currentTerm,
		LeaderID:      rf.me,
		LeaderCommit:  rf.commitIndex(),
	}

	index := rf.log.LastIndex()
	rf.mu.Unlock()

	for peerID := range rf.peers {
		if peerID == rf.me {
			continue
		}

		go rf.sync(ctx, log, peerID, index, args.DeepCopy())
	}
}
