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
	leaderElectionFunc func(context.Context)
}

func initLeaderElectionEngine(
	logger *log.Logger,
	leaderElectionFunc func(context.Context),
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
			log.Printf("Did not see a leader to much")

			lee.StopLeaderElection()

			ctx, cancel := context.WithCancel(context.Background())

			lee.mu.Lock()
			lee.electionCancelFunc = cancel
			lee.mu.Unlock()

			go lee.leaderElectionFunc(ctx)

			electionTimeoutCh <- time.Duration(rand.Int63n(max-min) + min)
		}
	}()

	for {
		select {
		case <-lee.stopTickerCh:
			ticker.Stop()
			log.Printf("Stop the election ticker")
		case <-lee.resetTickerCh:
			lee.stopLeaderElection()
			ticker.Reset(et)
			log.Printf("Reset the election ticker %v", et)
		case et = <-electionTimeoutCh:
			log.Printf("Set a new election timeout %v", et)
			ticker.Reset(et)
		}
	}
}

func (rf *Raft) askForVoting(
	ctx context.Context, log *log.Logger,
	peerID int, args *RequestVoteArgs, resCh chan int,
) {
	reply := &RequestVoteReply{}

	defer wg.Done()

	log.Printf("-> S%d T:%d", peerID, args.Term)

	if ok := rf.sendRequestVote(peerID, args, reply); !ok {
		log.Printf("Fail RequestVote call to S%d peer (term: %d)",
			peerID, args.Term)

		return
	}

	log.Printf("RequestVote reply from S%d (term: %d)", peerID, args.Term)

	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		log.Printf("RequestVote reply from the previous term %d < %d",
			args.Term, rf.currentTerm)

		rf.mu.Unlock()

		return
	}

	ok := rf.processIncomingTerm(log, peerID, reply.Term)
	rf.mu.Unlock()

	select {
	case <-ctx.Done():
		log.Printf("Skip S%d vote, election canceled (term %d)", peerID, args.Term)

		return
	default:
		if ok && reply.VoteGranted {
			log.Printf("Win: S%d voted for us", peerID)

			res = 1
		} else {
			log.Printf("Loose: S%d voted against us", peerID)

			res = 0
		}

		resCh <- res
	}
}

func (rf *Raft) leaderElectionStart(ctx context.Context) {
	correlationID := CorrelationID()
	log := extendLoggerWithTopic(rf.logger, leaderElectionLogTopic)
	log = extendLoggerWithCorrelationID(log, correlationID)

	log.Printf("Starting new Leader election cycle")

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if electionTerm < rf.currentTerm {
		log.Printf("Current term is higher since election started %d < %d",
			electionTerm, rf.currentTerm)

		rf.leaderElection.StopLeaderElection()

		return
	}

	log.Printf("(%d/%d) voted, %d for, %d against",
		nVoted, len(rf.peers), nVotedFor, nVoted-nVotedFor)

	if nVotedFor > len(rf.peers)/2 || nVoted == len(rf.peers) {
		if nVotedFor > len(rf.peers)/2 {
			log.Printf("I am a new leader for term %d", electionTerm)

			for i := range rf.peers {
				log.Printf("Set nextIndex for S%d to %d", i, len(rf.log))
				rf.nextIndex[i] = len(rf.log)
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

func (rf *Raft) leaderElectionStart(ctx context.Context) {
	log := extendLoggerWithTopic(rf.logger, leaderElectionLogTopic)
	log = extendLoggerWithCorrelationID(log, CorrelationID())

	log.Printf("Starting new Leader election cycle")

	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me

	log.Printf("Become a condidate and increase term to %d", rf.currentTerm)

	args := &RequestVoteArgs{
		CorrelationID: correlationID,
		Term:          rf.currentTerm,
		CandidateID:   rf.me,
		LastLogIndex:  len(rf.log) - 1,
		LastLogTerm:   rf.log[len(rf.log)-1].Term,
	}
}

func (rf *Raft) startLeaderElection(ctx context.Context) {
	log := extendLoggerWithTopic(rf.logger, leaderElectionLogTopic)

	log.Printf("Starting new Leader election cycle")

	args := rf.prepareForVote(log)
	nVoted := 1             // already voted for themselves
	nVotedFor := 1          // already voted for themselves
	resCh := make(chan int) // 0 - votedFor; 1 - votedAgeinst

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go rf.askForVoting(ctx, log, i, args.DeepCopy(), resCh)
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("Stop election for term %d", args.Term)

			return
		case res := <-resCh:
			nVotedFor += res
			nVoted++

			log.Printf("(%d/%d) voted, %d for, %d against",
				nVoted, len(rf.peers), nVotedFor, nVoted-nVotedFor)

			if nVotedFor > len(rf.peers)/2 || nVoted == len(rf.peers) {
				if nVotedFor > len(rf.peers)/2 {
					log.Printf("I am a new leader for term %d", args.Term)

					rf.mu.Lock()
					nextIndex := len(rf.log)

					for i := range rf.nextIndex {
						log.Printf("Set nextIndex for S%d to %d", i, nextIndex)
						rf.nextIndex[i] = nextIndex
					}
					rf.mu.Unlock()

					rf.heartbeats.StartSending()
					rf.leaderElection.StopTicker()
				} else {
					log.Print("Not luck, going to try the next time")
				}

				return
			}
		}
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
		rf.currentTerm, len(rf.log)-1, rf.log[len(rf.log)-1].Term)

	rf.processIncomingTerm(log, args.CandidateID, args.Term)

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
	case rf.log[len(rf.log)-1].Term > args.LastLogTerm:
		log.Printf("Has more up-to-date log term %d > %d",
			rf.log[len(rf.log)-1].Term, args.LastLogTerm)
	case rf.log[len(rf.log)-1].Term == args.LastLogTerm &&
		len(rf.log)-1 > args.LastLogIndex:
		log.Printf("Has longer log %d > %d", len(rf.log)-1, args.LastLogIndex)
	default:
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.leaderElection.ResetTicker()
		log.Printf("Voted for %d ", args.CandidateID)
	}
}