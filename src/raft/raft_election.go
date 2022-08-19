package raft

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"
)

type leaderElectionEngine struct {
	log                *log.Logger
	mu                 *sync.Mutex
	stopTickerCh       chan struct{}
	resetTickerCh      chan struct{}
	electionCancelFunc context.CancelFunc
	leaderElectionFunc func(ctx context.Context, correlationID string)
}

func initLeaderElectionEngine(
	logger *log.Logger,
	leaderElectionFunc func(context.Context, string),
) *leaderElectionEngine {
	lee := &leaderElectionEngine{
		log:                extendLoggerWithTopic(logger, leaderElectionLogTopic),
		mu:                 &sync.Mutex{},
		resetTickerCh:      make(chan struct{}),
		stopTickerCh:       make(chan struct{}),
		leaderElectionFunc: leaderElectionFunc,
	}

	go lee.ticker(logger)

	return lee
}

func (lee *leaderElectionEngine) ResetTicker() {
	lee.resetTickerCh <- struct{}{}
}

func (lee *leaderElectionEngine) StopTicker() {
	lee.stopTickerCh <- struct{}{}
}

func (lee *leaderElectionEngine) StopLeaderElection() {
	lee.mu.Lock()
	defer lee.mu.Unlock()

	if lee.electionCancelFunc != nil {
		lee.electionCancelFunc()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (lee *leaderElectionEngine) ticker(logger *log.Logger) {
	log := extendLoggerWithTopic(logger, tickerLogTopic)

	max := int64(electionTimeoutUpperBoundary)
	min := int64(electionTimeoutLowerBoundary)
	et := time.Duration(rand.Int63n(max-min) + min)
	electionTimeoutCh := make(chan time.Duration)

	ticker := time.NewTicker(et)

	go func() {
		for range ticker.C {
			correlationID := CorrelationID()
			llog := extendLoggerWithCorrelationID(log, correlationID)

			llog.Printf("Did not see a leader to much")

			lee.StopLeaderElection()

			ctx, cancel := context.WithCancel(context.Background())

			lee.mu.Lock()
			lee.electionCancelFunc = cancel
			lee.mu.Unlock()

			go lee.leaderElectionFunc(ctx, correlationID)

			electionTimeoutCh <- time.Duration(rand.Int63n(max-min) + min)
		}
	}()

	for {
		select {
		case <-lee.stopTickerCh:
			ticker.Stop()
			log.Printf("Stop the election ticker")
		case <-lee.resetTickerCh:
			ticker.Reset(et)
			log.Printf("Reset the election ticker %v", et)
		case et = <-electionTimeoutCh:
			log.Printf("Set a new election timeout %v", et)
			ticker.Reset(et)
		}
	}
}

func (rf *Raft) leaderElectionSendRequestVote(
	log *log.Logger, wg *sync.WaitGroup, args *RequestVoteArgs,
	peerID int, resultCh chan int,
) {
	reply := &RequestVoteReply{}

	defer wg.Done()

	log.Printf("-> S%d T:%d", peerID, args.Term)

	if ok := rf.sendRequestVote(peerID, args, reply); !ok {
		log.Printf("Fail RequestVote call to S%d peer T:%d", peerID, args.Term)

		return
	}

	log.Printf("<- S%d ET:%d {T:%d VG:%v}",
		peerID, args.Term, reply.Term, reply.VoteGranted)

	rf.mu.Lock()
	ok := rf.processIncomingTerm(args.CorrelationID, log, peerID, reply.Term)
	rf.mu.Unlock()

	if ok && reply.VoteGranted {
		log.Printf("Win: S%d voted for us", peerID)

		resultCh <- 1
	} else {
		log.Printf("Loose: S%d voted against us", peerID)

		resultCh <- 0
	}
}

func (rf *Raft) leaderElectionSendRequestVotes(
	log *log.Logger, args *RequestVoteArgs,
) chan int {
	resultCh := make(chan int) // 0 - votedFor; 1 - votedAgeinst
	wg := &sync.WaitGroup{}

	wg.Add(len(rf.peers) - 1)

	for peerID := range rf.peers {
		if peerID == rf.me {
			continue
		}

		go rf.leaderElectionSendRequestVote(log, wg, args, peerID, resultCh)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	return resultCh
}

func isLeaderElectionActive(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	default:
		return true
	}
}

func (rf *Raft) leaderElectionVotesCalculation(
	log *log.Logger,
	nVotedFor int,
	nVoted int,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("(%d/%d) voted, %d for, %d against",
		nVoted, len(rf.peers), nVotedFor, nVoted-nVotedFor)

	if nVotedFor > len(rf.peers)/2 || nVoted == len(rf.peers) {
		if nVotedFor > len(rf.peers)/2 {
			log.Printf("New leader T:%d", rf.currentTerm)

			for i := range rf.peers {
				log.Printf("Set nextIndex for S%d to %d", i, rf.log.LastIndex()+1)
				rf.nextIndex[i] = rf.log.LastIndex() + 1
				rf.matchIndex[i] = 0
			}

			rf.heartbeats.StartSending()
			rf.leaderElection.StopTicker()
		} else {
			log.Print("Not luck, going to try the next time")
		}

		rf.leaderElection.StopLeaderElection()
	}
}

func (rf *Raft) leaderElectionStart(ctx context.Context, correlationID string) {
	log := extendLoggerWithTopic(rf.logger, leaderElectionLogTopic)
	log = extendLoggerWithCorrelationID(log, correlationID)

	log.Printf("Starting new Leader election cycle")

	rf.mu.Lock()

	if !isLeaderElectionActive(ctx) {
		rf.mu.Unlock()

		return
	}

	rf.currentTerm++
	rf.votedFor = rf.me

	rf.persist(correlationID)

	log.Printf("Become a condidate and increase term to %d", rf.currentTerm)

	args := &RequestVoteArgs{
		CorrelationID: correlationID,
		Term:          rf.currentTerm,
		CandidateID:   rf.me,
		LastLogIndex:  rf.log.LastIndex(),
		LastLogTerm:   rf.Log(rf.log.LastIndex()).Term,
	}

	rf.mu.Unlock()

	nVoted := 1    // already voted for themselves
	nVotedFor := 1 // already voted for themselves

	resultCh := rf.leaderElectionSendRequestVotes(log, args.DeepCopy())

	for result := range resultCh {
		if !isLeaderElectionActive(ctx) {
			log.Printf("Skip vote, election canceled T:%d", args.Term)

			return
		}

		nVotedFor += result
		nVoted++

		rf.leaderElectionVotesCalculation(log, nVotedFor, nVoted)
	}
}

func (rf *Raft) RequestVote(
	args *RequestVoteArgs, reply *RequestVoteReply,
) {
	log := extendLoggerWithTopic(rf.logger, leaderElectionLogTopic)
	log = extendLoggerWithCorrelationID(log, args.CorrelationID)

	log.Printf("<- S%d {T:%d, LLI:%d, LLT:%d}",
		args.CandidateID, args.Term, args.LastLogIndex, args.LastLogTerm)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("Current State: {T:%d, LLI:%d, LLT:%d}",
		rf.currentTerm, rf.log.LastIndex(), rf.Log(rf.log.LastIndex()).Term)

	rf.processIncomingTerm(args.CorrelationID, log, args.CandidateID, args.Term)

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	switch {
	case args.Term < rf.currentTerm:
		log.Printf("Incoming term smaller %d < %d", args.Term, rf.currentTerm)
	case rf.votedFor != -1:
		log.Printf("Already voted for %d", rf.votedFor)
	case rf.votedFor == args.CandidateID:
		log.Printf("Already voted for incoming candidate %d", rf.votedFor)
	// candidate’s log is at least as up-to-date as receiver’s log
	case rf.Log(rf.log.LastIndex()).Term > args.LastLogTerm:
		log.Printf("Has more up-to-date log term %d > %d",
			rf.Log(rf.log.LastIndex()).Term, args.LastLogTerm)
	case rf.Log(rf.log.LastIndex()).Term == args.LastLogTerm &&
		rf.log.LastIndex() > args.LastLogIndex:
		log.Printf("Has longer log %d > %d", rf.log.LastIndex(), args.LastLogIndex)
	default:
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.persist(args.CorrelationID)
		rf.leaderElection.ResetTicker()
		log.Printf("Voted for %d ", args.CandidateID)
	}
}
