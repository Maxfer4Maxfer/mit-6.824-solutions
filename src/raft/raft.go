package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)

//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	electionTimeoutUpperBoundary = 300 * time.Millisecond
	electionTimeoutLowerBoundary = 150 * time.Millisecond
	broadcastTime                = 10 * time.Millisecond
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	Command      interface{}
	CommandValid bool
	CommandIndex int

	// For 2D:
	SnapshotTerm  int
	SnapshotIndex int
	SnapshotValid bool
	Snapshot      []byte
}

type state int32

const (
	followerState state = iota
	candidateState
	leaderState
)

func (s state) String() string {
	switch s {
	case followerState:
		return "FOLLOWER"
	case candidateState:
		return "CANDIDATE"
	case leaderState:
		return "LEADER"
	default:
		return "NONE"
	}
}

type LogEntry struct {
	Term    int
	Command string
}

func (le *LogEntry) DeepCopy() *LogEntry {
	return &LogEntry{
		Term:    le.Term,
		Command: le.Command,
	}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	logger    *log.Logger
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state           state
	electionTimeout time.Duration
	lastHeartbeat   time.Time

	heartbeatCancelFunc context.CancelFunc

	// latest term server has seen
	// (initialized to 0 on first boot, increases monotonically)
	currentTerm int

	// candidateId that received vote in current term (or -1 if none)
	votedFor int

	// log entries; each entry contains command for state machine,
	// and term when entry was received by leader (first index is 1)
	log []LogEntry
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var (
		term     int
		isLeader bool
	)

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm

	if rf.state == leaderState {
		isLeader = true
	}

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	return
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}

func (rf *Raft) RequestVote(
	args *RequestVoteArgs, reply *RequestVoteReply,
) {
	log := extendLoggerWithPrefix(rf.logger, leaderElectionLogTopic)

	// Your code here (2A, 2B).
	log.Printf("RequestVote call from S%d", args.CandidateID)

	rf.processIncomingTerm(log, args.CandidateID, args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	switch {
	case args.Term < rf.currentTerm:
		log.Printf("Incoming term smaller %d < %d", args.Term, rf.currentTerm)
	case rf.votedFor != -1:
		log.Printf("Already voted for %d ", rf.votedFor)
	case rf.votedFor == args.CandidateID:
		log.Printf("Already voted for incoming candidate %d ", rf.votedFor)
	// candidate’s log is at least as up-to-date as receiver’s log
	case len(rf.log) < args.LastLogIndex:
		log.Printf("Has smaller log %d != %d ", len(rf.log), args.LastLogIndex)
	case rf.log[args.LastLogIndex].Term != args.LastLogTerm:
		log.Printf("has log discrepancy %d != %d  ",
			rf.log[args.LastLogIndex].Term, args.LastLogTerm)
	default:
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		log.Printf("Voted for %d ", args.CandidateID)
	}
}

func (rf *Raft) AppendEntries(
	args *AppendEntriesArgs, reply *AppendEntriesReply,
) {
	log := extendLoggerWithPrefix(rf.logger, appendEntriesLogTopic)

	log.Printf("AppendEntries from S%d", args.LeaderID)

	rf.processIncomingTerm(log, args.LeaderID, args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		log.Printf("Incoming term smaller %d < %d", args.Term, rf.currentTerm)

		return
	}

	rf.lastHeartbeat = time.Now()
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	extendLoggerWithPrefix(rf.logger, commonLogTopic)
	log.Printf("Resived the KILL signal")

	if rf.heartbeatCancelFunc != nil {
		rf.heartbeatCancelFunc()
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)

	return z == 1
}

func (rf *Raft) sendHeartbeats() {
}

func (rf *Raft) stopHeartbeats() {
}

func (rf *Raft) heartbeating() {
	log := extendLoggerWithPrefix(rf.logger, heartbeatingLogTopic)
	log.Print("Start sending heartbets to my peers")

	ctx, cancelFunc := context.WithCancel(context.Background())

	rf.mu.Lock()
	rf.heartbeatCancelFunc = cancelFunc
	rf.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			log.Print("Stop hearbeating")

			rf.mu.Lock()
			rf.heartbeatCancelFunc = nil
			rf.mu.Unlock()

			return
		default:
			log.Print("Send hearbeat")

			rf.mu.Lock()
			args := &AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderID: rf.me,
			}
			rf.mu.Unlock()

			for i := range rf.peers {
				if i == rf.me {
					continue
				}

				go func(i int) {
					reply := &AppendEntriesReply{}

					if ok := rf.sendAppendEntries(i, args.DeepCopy(), reply); !ok {
						log.Printf("WRN fail AppendEntries call to %d peer", i)
					}

					if rf.processIncomingTerm(log, i, reply.Term) {
						if rf.heartbeatCancelFunc != nil {
							rf.heartbeatCancelFunc()
						}
					}
				}(i)
			}

			time.Sleep(broadcastTime)
		}
	}
}

func (rf *Raft) processIncomingTerm(log *log.Logger, peerID int, term int) bool {
	rf.mu.Lock()

	if term > rf.currentTerm {
		log.Printf(
			"S%d has a higher term %d > %d",
			peerID, term, rf.currentTerm,
		)

		rf.mu.Unlock()

		rf.becomeFollower(term)

		return true
	}

	rf.mu.Unlock()

	return false
}

func (rf *Raft) becomeFollower(term int) {
	log := extendLoggerWithPrefix(rf.logger, becomeFollowerLogTopic)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != followerState {
		log.Printf("state %s -> %s", rf.state, followerState)
		rf.state = followerState
	}

	rf.votedFor = -1
	rf.currentTerm = term

	if rf.heartbeatCancelFunc != nil {
		rf.heartbeatCancelFunc()
	}
}

func (rf *Raft) interactWith(
	ctx context.Context, log *log.Logger,
	peer int, args *RequestVoteArgs, resCh chan bool,
) {
	reply := &RequestVoteReply{}

	if ok := rf.sendRequestVote(peer, args, reply); !ok {
		log.Printf("WRN fail RequestVote call to S%d peer", peer)
	}

	rf.processIncomingTerm(log, args.CandidateID, args.Term)

	if reply.Term > rf.currentTerm {
		log.Printf("Stopping election, S%d has higher term %d > %d",
			args.CandidateID, reply.Term, rf.currentTerm)

		return
	}

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
		return
	default:
		resCh <- res
	}
}

func (rf *Raft) prepareForVote(log *log.Logger) *RequestVoteArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm++
	rf.state = candidateState
	rf.votedFor = rf.me

	log.Printf("Become a condidate and increase term to %d", rf.currentTerm)

	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
}

func (rf *Raft) leaderElection() {
	log := extendLoggerWithPrefix(rf.logger, leaderElectionLogTopic)

	log.Printf("Starting new Leader election cycle")

	args := rf.prepareForVote(log)
	nVoted := 1              // already voted for themselves
	nVotedFor := 1           // already voted for themselves
	resCh := make(chan bool) // true - votedFor; false - votedAgeinst

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go rf.interactWith(ctx, log, i, args.DeepCopy(), resCh)

	}

	for res := range resCh {
		nVoted++

		if res {
			nVotedFor++
		}

		log.Printf("(%d/%d) voted, %d for, %d against",
			nVoted, len(rf.peers), nVotedFor, nVoted-nVotedFor)

		if nVotedFor > len(rf.peers)/2 || nVoted == len(rf.peers)-1 {
			if nVotedFor > len(rf.peers)/2 {
				log.Print("I am a new leader")
				rf.mu.Lock()
				rf.state = leaderState
				rf.mu.Unlock()
				rf.heartbeating()
			} else {
				log.Print("Not luck, going to try the next time")
				rf.mu.Lock()
				rf.votedFor = -1
				rf.state = followerState
				rf.mu.Unlock()
			}

			return
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

	for !rf.killed() {
		// (2A) Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		isLeader := rf.state == leaderState
		leaderForgotUs := rf.lastHeartbeat.Add(rf.electionTimeout).Before(time.Now())
		rf.mu.Unlock()

		if !isLeader && leaderForgotUs {
			log.Printf("Did not see a leader to much")

			rf.leaderElection()
		}

		max := int64(electionTimeoutUpperBoundary)
		min := int64(electionTimeoutLowerBoundary)
		et := time.Duration(rand.Int63n(max-min) + min)

		log.Printf("Set a new election timeout %v", et)

		rf.mu.Lock()
		rf.electionTimeout = et
		rf.mu.Unlock()

		time.Sleep(et)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(
	peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg,
) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.state = followerState
	rf.log = []LogEntry{{0, ""}}

	rf.logger = log.New(
		os.Stdout,
		fmt.Sprintf("S%d ", me),
		log.Lshortfile|log.Lmicroseconds,
	)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
