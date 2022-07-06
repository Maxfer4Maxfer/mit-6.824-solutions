package raft

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

import (
	"context"
	"fmt"
	"log"
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
	// The tester requires that the leader send heartbeat RPCs
	// no more than ten times per second.
	broadcastTime = 110 * time.Millisecond
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

	electionTimeout    time.Duration
	electionCancelFunc context.CancelFunc

	heartbeats    *heartbeatsEngine
	lastHeartbeat time.Time

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
	isLeader = rf.heartbeats.IsSendingInProgress()

	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(
	lastIncludedTerm int, lastIncludedIndex int, snapshot []byte,
) bool {
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
		log.Printf("Already voted for %d", rf.votedFor)
	case rf.votedFor == args.CandidateID:
		log.Printf("Already voted for incoming candidate %d", rf.votedFor)
	// candidate’s log is at least as up-to-date as receiver’s log
	case len(rf.log)-1 < args.LastLogIndex:
		log.Printf("Has smaller log size %d != %d", len(rf.log)-1, args.LastLogIndex)
	case rf.log[args.LastLogIndex].Term != args.LastLogTerm:
		log.Printf("has log discrepancy %d != %d",
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if !rf.heartbeats.IsSendingInProgress() {
		return -1, -1, false
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	extendLoggerWithPrefix(rf.logger, commonLogTopic)
	log.Printf("Received the KILL signal")

	rf.heartbeats.StopSending()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)

	return z == 1
}

func (rf *Raft) sendHeartbeats() {
	log := extendLoggerWithPrefix(rf.logger, heartbeatingLogTopic)

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

			rf.processIncomingTerm(log, i, reply.Term)
		}(i)
	}
}

func (rf *Raft) processIncomingTerm(log *log.Logger, peerID int, term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if term > rf.currentTerm {
		log.Printf("S%d has a higher term %d > %d", peerID, term, rf.currentTerm)

		rf.heartbeats.StopSending()
		rf.stopLeaderElection()

		rf.votedFor = -1
		rf.currentTerm = term
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(
	peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg,
) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.logger = log.New(
		os.Stdout,
		fmt.Sprintf("S%d ", me),
		log.Lshortfile|log.Lmicroseconds,
	)

	rf.heartbeats = initHeartbeatsEngine(
		rf.logger, broadcastTime, rf.sendHeartbeats,
	)
	rf.votedFor = -1
	rf.log = []LogEntry{{0, ""}} // need for first log index will be 1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
