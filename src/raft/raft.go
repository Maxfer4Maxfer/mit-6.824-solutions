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
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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
	Command interface{}
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

	// applyCh is a channel on which the
	// tester or service expects Raft to send ApplyMsg messages.
	applyCh chan ApplyMsg

	leaderElection *leaderElectionEngine
	heartbeats     *heartbeatsEngine

	// latest term server has seen
	// (initialized to 0 on first boot, increases monotonically)
	currentTerm int

	// candidateId that received vote in current term (or -1 if none)
	votedFor int

	// log entries; each entry contains command for state machine,
	// and term when entry was received by leader (first index is 1)
	log []LogEntry

	// index of highest log entry known to be committed
	// initialized to 0, intcreases monotonically
	// available only via getter and setter methods
	vCommitIndex    int64
	commitIndexCond *sync.Cond

	// index of highest log entry applied to state machine
	// initialized to 0, intcreases monotonically
	lastApplied int

	// for each server, index of the next log entry to send to that server
	// (initialized to leader last log index + 1)
	nextIndex []int

	// index of highest log entry applied to state machine
	// (initialized to 0, increases monotonically)
	matchIndex     []int
	matchIndexCond *sync.Cond
}

func (rf *Raft) commitIndex() int {
	return int(atomic.LoadInt64(&rf.vCommitIndex))
}

func (rf *Raft) setCommitIndex(ci int) {
	if ci > rf.commitIndex() {
		atomic.StoreInt64(&rf.vCommitIndex, int64(ci))
		rf.commitIndexCond.Broadcast()
	}
}

func (rf *Raft) applyLogProcessing() {
	log := extendLoggerWithTopic(rf.logger, applyLogTopic)

	rf.commitIndexCond = sync.NewCond(&sync.Mutex{})

	rf.commitIndexCond.L.Lock()

	go func() {
		for {
			rf.commitIndexCond.Wait()

			rf.mu.Lock()
			for i := rf.lastApplied + 1; i <= rf.commitIndex(); i++ {
				log.Printf("Apply index: %d, term: %d, command %v",
					i, rf.log[i].Term, rf.log[i].Command)
				rf.lastApplied++
				rf.applyCh <- ApplyMsg{
					Command:      rf.log[i].Command,
					CommandValid: true,
					CommandIndex: rf.lastApplied,
				}
			}
			rf.mu.Unlock()
		}
	}()
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
func (rf *Raft) persist(correlationID string) {
	log := extendLoggerWithTopic(rf.logger, persisterLogTopic)
	log = extendLoggerWithCorrelationID(log, correlationID)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	log.Printf("save CT:%d VF:%d len(log):%d",
		rf.currentTerm, rf.votedFor, len(rf.log))

	rf.persister.SaveRaftState(w.Bytes())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	logger := extendLoggerWithTopic(rf.logger, persisterLogTopic)

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var (
		currentTerm int
		votedFor    int
		log         []LogEntry
	)

	if d.Decode(&currentTerm) != nil {
		panic("currentTerm not found")
	}

	if d.Decode(&votedFor) != nil {
		panic("votedFor not found")
	}

	if d.Decode(&log) != nil {
		panic("log not found")
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log

	logger.Printf("load CT:%d VF:%d len(log):%d", currentTerm, votedFor, len(log))
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
	extendLoggerWithTopic(rf.logger, commonLogTopic)
	log.Printf("Received the KILL signal")

	rf.heartbeats.StopSending()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)

	return z == 1
}

func (rf *Raft) processIncomingTerm(
	correlationID string, log *log.Logger, peerID int, term int,
) bool {
	if term > rf.currentTerm {
		log.Printf("S%d has a higher term %d > %d", peerID, term, rf.currentTerm)

		if rf.heartbeats.IsSendingInProgress() {
			rf.heartbeats.StopSending()
			rf.leaderElection.ResetTicker()
		} else {
			rf.leaderElection.StopLeaderElection()
		}

		rf.votedFor = -1
		rf.currentTerm = term

		rf.persist(correlationID)

		return false
	}

	return true
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
	rand.Seed(MakeSeed())

	rf := &Raft{
		peers:      peers,
		persister:  persister,
		me:         me,
		votedFor:   -1,
		applyCh:    applyCh,
		log:        []LogEntry{{0, ""}}, // need for first log index will be 1
		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.logger = log.New(
		os.Stdout,
		fmt.Sprintf("S%d ", me),
		log.Lshortfile|log.Lmicroseconds,
	)

	rf.heartbeats = initHeartbeatsEngine(
		rf.logger, broadcastTime, rf.sendHeartbeats,
	)
	rf.leaderElection = initLeaderElectionEngine(
		rf.logger, rf.leaderElectionStart,
	)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.matchIndexProcessing()
	rf.applyLogProcessing()

	return rf
}
