package raft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type heartbeatsEngine struct {
	run                int32 // 1 - run; 0 - stop running
	cond               *sync.Cond
	log                *log.Logger
	broadcastTime      time.Duration
	sendHeartbeatsFunc func()
}

func initHeartbeatsEngine(
	logger *log.Logger,
	broadcastTime time.Duration,
	sendHeartbeatsFunc func(),
) *heartbeatsEngine {
	hbe := &heartbeatsEngine{
		run:                0,
		cond:               sync.NewCond(&sync.Mutex{}),
		broadcastTime:      broadcastTime,
		sendHeartbeatsFunc: sendHeartbeatsFunc,
		log:                extendLoggerWithPrefix(logger, heartbeatingLogTopic),
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
	atomic.StoreInt32(&hbe.run, 0)
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
			hbe.log.Print("Stop sending heartbeats")
			hbe.cond.Wait()
		default:
			hbe.sendHeartbeatsFunc()

			time.Sleep(broadcastTime)
		}
	}
}
