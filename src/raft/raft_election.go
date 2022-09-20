package raft

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"
)

func genElectionTimeout() time.Duration {
	max := int64(electionTimeoutUpperBoundary)
	min := int64(electionTimeoutLowerBoundary)

	return time.Duration(rand.Int63n(max-min) + min)
}

type leaderElectionEngine struct {
	log                *log.Logger
	mu                 *sync.Mutex
	stopTickerCh       chan struct{}
	resetTickerCh      chan struct{}
	electionCancelFunc context.CancelFunc
	leaderElectionFunc func(ctx context.Context)
	electionTimeout    time.Duration
	ticker             *time.Ticker
}

func initLeaderElectionEngine(
	logger *log.Logger,
	leaderElectionFunc func(context.Context),
) *leaderElectionEngine {

	lee := &leaderElectionEngine{
		log:                ExtendLoggerWithTopic(logger, LoggerTopicLeaderElection),
		mu:                 &sync.Mutex{},
		resetTickerCh:      make(chan struct{}),
		stopTickerCh:       make(chan struct{}),
		leaderElectionFunc: leaderElectionFunc,
		electionTimeout:    genElectionTimeout(),
	}

	lee.ticker = time.NewTicker(lee.electionTimeout)

	go lee.proccess(logger)

	return lee
}

func (lee *leaderElectionEngine) ResetTicker() {
	lee.mu.Lock()
	defer lee.mu.Unlock()

	lee.ticker.Reset(lee.electionTimeout)
	lee.log.Printf("Reset the election ticker %v", lee.electionTimeout)
}

func (lee *leaderElectionEngine) StopTicker() {
	lee.mu.Lock()
	defer lee.mu.Unlock()

	lee.ticker.Stop()
	lee.log.Printf("Stop the election ticker")
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
func (lee *leaderElectionEngine) proccess(logger *log.Logger) {
	for range lee.ticker.C {
		correlationID := NewCorrelationID()
		llog := ExtendLoggerWithCorrelationID(lee.log, correlationID)

		llog.Printf("Did not see a leader to much")

		lee.StopLeaderElection()

		ctx, cancel := context.WithCancel(context.Background())
		ctx = AddCorrelationID(ctx, correlationID)

		lee.mu.Lock()
		lee.electionCancelFunc = cancel
		lee.electionTimeout = genElectionTimeout()
		lee.ticker.Reset(lee.electionTimeout)
		llog.Printf("Set a new election timeout %v", lee.electionTimeout)
		lee.mu.Unlock()

		go lee.leaderElectionFunc(ctx)

	}
}

func (rf *Raft) leaderElectionSendRequestVote(
	ctx context.Context, wg *sync.WaitGroup, args *RequestVoteArgs,
	peerID int, resultCh chan int,
) {
	log := ExtendLogger(ctx, rf.logger, LoggerTopicLeaderElection)
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
	ok := rf.processIncomingTerm(ctx, log, peerID, reply.Term)
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
	ctx context.Context, args *RequestVoteArgs,
) chan int {
	resultCh := make(chan int) // 0 - votedFor; 1 - votedAgeinst
	wg := &sync.WaitGroup{}

	wg.Add(len(rf.peers) - 1)

	for peerID := range rf.peers {
		if peerID == rf.me {
			continue
		}

		go rf.leaderElectionSendRequestVote(ctx, wg, args, peerID, resultCh)
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
	ctx context.Context, term int, resultCh chan int,
) {
	log := ExtendLogger(ctx, rf.logger, LoggerTopicLeaderElection)

	nVoted := 1    // already voted for themselves
	nVotedFor := 1 // already voted for themselves

	for result := range resultCh {
		nVotedFor += result
		nVoted++

		rf.mu.Lock()

		if !isLeaderElectionActive(ctx) {
			log.Printf("Skip vote, election canceled T:%d", term)
			rf.mu.Unlock()

			continue
		}

		log.Printf("(%d/%d) voted, %d for, %d against",
			nVoted, len(rf.peers), nVotedFor, nVoted-nVotedFor)

		if nVotedFor > len(rf.peers)/2 || nVoted == len(rf.peers) {
			if nVotedFor > len(rf.peers)/2 {
				log.Printf("New leader T:%d", rf.currentTerm)

				for i := range rf.peers {
					log.Printf("Set nextIndex for S%d to %d",
						i, rf.log.LastIndex()+1)

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

		rf.mu.Unlock()
	}
}

func (rf *Raft) leaderElectionStart(ctx context.Context) {
	log := ExtendLogger(ctx, rf.logger, LoggerTopicLeaderElection)

	log.Printf("Starting new Leader election cycle")

	rf.mu.Lock()

	if !isLeaderElectionActive(ctx) {
		rf.mu.Unlock()

		return
	}

	rf.currentTerm++
	rf.votedFor = rf.me

	rf.persist(ctx)

	log.Printf("Become a condidate and increase term to %d", rf.currentTerm)

	args := &RequestVoteArgs{
		CorrelationID: GetCorrelationID(ctx),
		Term:          rf.currentTerm,
		CandidateID:   rf.me,
		LastLogIndex:  rf.log.LastIndex(),
		LastLogTerm:   rf.log.Term(rf.log.LastIndex()),
	}

	rf.mu.Unlock()

	resultCh := rf.leaderElectionSendRequestVotes(ctx, args.DeepCopy())

	rf.leaderElectionVotesCalculation(ctx, args.Term, resultCh)
}

func (rf *Raft) RequestVote(
	args *RequestVoteArgs, reply *RequestVoteReply,
) {
	ctx := AddCorrelationID(context.Background(), args.CorrelationID)
	log := ExtendLogger(ctx, rf.logger, LoggerTopicLeaderElection)

	log.Printf("<- S%d {T:%d, LLI:%d, LLT:%d}",
		args.CandidateID, args.Term, args.LastLogIndex, args.LastLogTerm)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("Current State: {T:%d, LI:%d, LLT:%d}",
		rf.currentTerm, rf.log.LastIndex(), rf.log.Term(rf.log.LastIndex()))

	rf.processIncomingTerm(ctx, log, args.CandidateID, args.Term)

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
	case rf.log.Term(rf.log.LastIndex()) > args.LastLogTerm:
		log.Printf("Has more up-to-date log term %d > %d",
			rf.log.Term(rf.log.LastIndex()), args.LastLogTerm)
	case rf.log.Term(rf.log.LastIndex()) == args.LastLogTerm &&
		rf.log.LastIndex() > args.LastLogIndex:
		log.Printf("Has longer log %d > %d", rf.log.LastIndex(), args.LastLogIndex)
	default:
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.persist(ctx)
		rf.leaderElection.ResetTicker()
		log.Printf("Voted for %d ", args.CandidateID)
	}
}
