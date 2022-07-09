package raft

import (
	"context"
	crand "crypto/rand"
	"log"
	"math/big"
	"math/rand"
	"time"
)

func (rf *Raft) askForVoting(
	ctx context.Context, log *log.Logger,
	peer int, args *RequestVoteArgs, resCh chan bool,
) {
	reply := &RequestVoteReply{}

	log.Printf("RequestVote send to S%d", peer)

	if ok := rf.sendRequestVote(peer, args, reply); !ok {
		log.Printf("WRN fail RequestVote call to S%d peer", peer)
	}

	log.Printf("RequestVote reply from S%d", peer)

	rf.mu.Lock()
	rf.processIncomingTerm(log, args.CandidateID, args.Term)
	rf.mu.Unlock()

	var res bool

	if reply.VoteGranted {
		log.Printf("Win: S%d voted for us", peer)

		res = true
	} else {
		log.Printf("Loose: S%d voted against us", peer)

		res = false
	}

	select {
	case <-ctx.Done():
		log.Printf("Stop ask S%d for voining", peer)

		return
	default:
		resCh <- res
	}
}

func (rf *Raft) prepareForVote(log *log.Logger) *RequestVoteArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm++
	rf.votedFor = rf.me

	log.Printf("Become a condidate and increase term to %d", rf.currentTerm)

	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
}

func (rf *Raft) startLeaderElection() {
	ctx, cancel := context.WithCancel(context.Background())

	rf.mu.Lock()
	rf.electionCancelFunc = cancel
	rf.mu.Unlock()

	rf.leaderElection(ctx)
}

func (rf *Raft) stopLeaderElection() {
	if rf.electionCancelFunc != nil {
		rf.electionCancelFunc()
	}
}

func (rf *Raft) leaderElection(ctx context.Context) {
	log := extendLoggerWithPrefix(rf.logger, leaderElectionLogTopic)

	log.Printf("Starting new Leader election cycle")

	args := rf.prepareForVote(log)
	nVoted := 1              // already voted for themselves
	nVotedFor := 1           // already voted for themselves
	resCh := make(chan bool) // true - votedFor; false - votedAgeinst

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go rf.askForVoting(ctx, log, i, args.DeepCopy(), resCh)
	}

	for {
		select {
		case <-ctx.Done():
			log.Print("Stop election (term sig)")
			return
		case res := <-resCh:
			nVoted++

			if res {
				nVotedFor++
			}

			log.Printf("(%d/%d) voted, %d for, %d against",
				nVoted, len(rf.peers), nVotedFor, nVoted-nVotedFor)

			if nVotedFor > len(rf.peers)/2 || nVoted == len(rf.peers) {
				if nVotedFor > len(rf.peers)/2 {
					log.Printf("I am a new leader for term %d", args.Term)
					rf.heartbeats.StartSending()
				} else {
					log.Print("Not luck, going to try the next time")
					rf.mu.Lock()
					rf.votedFor = -1
					rf.mu.Unlock()
				}

				return
			}
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	log := extendLoggerWithPrefix(rf.logger, tickerLogTopic)

	makeSeed := func() int64 {
		max := big.NewInt(int64(1) << 62)
		bigx, _ := crand.Int(crand.Reader, max)
		x := bigx.Int64()

		return x
	}

	rand.Seed(makeSeed())

	max := int64(electionTimeoutUpperBoundary)
	min := int64(electionTimeoutLowerBoundary)
	et := time.Duration(1)

	ticker := time.NewTicker(1)

	for {
		select {
		case <-rf.ctx.Done():
			log.Printf("Exit leader election routin")
			return
		case <-ticker.C:
			if rf.heartbeats.IsSendingInProgress() {
				continue
			}

			log.Printf("Did not see a leader to much")

			ticker.Stop()
			rf.startLeaderElection()

			if !rf.heartbeats.IsSendingInProgress() {
				et = time.Duration(rand.Int63n(max-min) + min)
				ticker.Reset(et)

				log.Printf("Set a new election timeout %v", et)
			}
		case <-rf.resetElectionTimeoutCh:
			ticker.Reset(et)
		}
	}

	// for !rf.killed() {
	// 	// (2A) Your code here to check if a leader election should
	// 	// be started and to randomize sleeping time using
	// 	// time.Sleep().
	// 	isLeader := rf.heartbeats.IsSendingInProgress()
	// 	rf.mu.Lock()
	// 	longAlone := rf.lastHeartbeat.Add(rf.electionTimeout).Before(time.Now())
	// 	rf.mu.Unlock()

	// 	if !isLeader && longAlone {
	// 		log.Printf("Did not see a leader to much")

	// 		rf.startLeaderElection()
	// 	}

	// 	max := int64(electionTimeoutUpperBoundary)
	// 	min := int64(electionTimeoutLowerBoundary)
	// 	et := time.Duration(rand.Int63n(max-min) + min)

	// 	log.Printf("Set a new election timeout %v", et)

	// 	rf.mu.Lock()
	// 	rf.electionTimeout = et
	// 	rf.mu.Unlock()

	// 	time.Sleep(et)
	// }
}

func (rf *Raft) RequestVote(
	args *RequestVoteArgs, reply *RequestVoteReply,
) {
	log := extendLoggerWithPrefix(rf.logger, leaderElectionLogTopic)

	// Your code here (2A, 2B).
	log.Printf("RequestVote call from S%d", args.CandidateID)

	rf.mu.Lock()
	defer rf.mu.Unlock()

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
		log.Printf("Has more up-to-date logs %d > %d", rf.log[len(rf.log)-1].Term, args.LastLogTerm)
	case rf.log[len(rf.log)-1].Term == args.LastLogTerm &&
		len(rf.log)-1 > args.LastLogIndex:
		log.Printf("Has longer log %d > %d", len(rf.log)-1, args.LastLogIndex)
	// case len(rf.log)-1 < args.LastLogIndex:
	// 	log.Printf("Has smaller log size %d != %d", len(rf.log)-1, args.LastLogIndex)
	// case rf.log[args.LastLogIndex].Term != args.LastLogTerm:
	// 	log.Printf("Has log discrepancy %d != %d",
	// 		rf.log[args.LastLogIndex].Term, args.LastLogTerm)
	default:
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		log.Printf("Voted for %d ", args.CandidateID)
	}
}
