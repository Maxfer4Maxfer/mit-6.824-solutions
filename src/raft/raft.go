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
	"math"
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
	broadcastTime = 80 * time.Millisecond
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
	cancel    context.CancelFunc  // set by Kill()
	ctx       context.Context     // for liveness check

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// applyCh is a channel on which the
	// tester or service expects Raft to send ApplyMsg messages.
	applyCh chan ApplyMsg

	resetElectionTimeoutCh chan struct{}
	electionCancelFunc     context.CancelFunc

	heartbeats *heartbeatsEngine

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
	vCommitIndex    uint64
	commitIndexCond *sync.Cond

	// index of highest log entry applied to state machine
	// initialized to 0, intcreases monotonically
	lastApplied uint64
}

func (rf *Raft) commitIndex() uint64 {
	return atomic.LoadUint64(&rf.vCommitIndex)
}

func (rf *Raft) setCommitIndex(ci uint64) {
	if ci > rf.commitIndex() {
		atomic.StoreUint64(&rf.vCommitIndex, ci)
		rf.commitIndexCond.Broadcast()
	}
}

func (rf *Raft) resetElectionTimeout() {
	select {
	case rf.resetElectionTimeoutCh <- struct{}{}:
	default:
	}
}

func (rf *Raft) applyLog() {
	log := extendLoggerWithPrefix(rf.logger, applyLogTopic)

	rf.commitIndexCond = sync.NewCond(&sync.Mutex{})

	rf.commitIndexCond.L.Lock()

	go func() {
		for {
			rf.commitIndexCond.Wait()

			rf.mu.Lock()
			for i := rf.lastApplied + 1; i <= rf.commitIndex(); i++ {
				log.Printf("Apply %d index, %d term", i, rf.log[i].Term)
				rf.lastApplied++
				rf.applyCh <- ApplyMsg{
					Command:      rf.log[i].Command,
					CommandValid: true,
					CommandIndex: int(rf.lastApplied),
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

func (rf *Raft) AppendEntries(
	args *AppendEntriesArgs, reply *AppendEntriesReply,
) {
	var log *log.Logger

	if len(args.Entries) == 0 {
		log = extendLoggerWithPrefix(rf.logger, heartbeatingLogTopic)
	} else {
		log = extendLoggerWithPrefix(rf.logger, appendEntriesLogTopic)
	}

	log.Printf("AppendEntries from S%d", args.LeaderID)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.processIncomingTerm(log, args.LeaderID, args.Term)

	reply.Success = false
	reply.Term = rf.currentTerm

	switch {
	case args.Term < rf.currentTerm:
		log.Printf("Incoming term smaller %d < %d", args.Term, rf.currentTerm)
	case uint64(len(rf.log)-1) < args.PrevLogIndex:
		log.Printf("Has smaller log size %d != %d", len(rf.log)-1, args.PrevLogIndex)
	case rf.log[args.PrevLogIndex].Term != args.PrevLogTerm:
		log.Printf("has log discrepancy %d != %d",
			rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		rf.log = rf.log[:args.PrevLogIndex]

		fallthrough
	default:
		reply.Success = true
		rf.resetElectionTimeout()
		rf.log = append(rf.log, args.Entries...)

		if args.LeaderCommit > rf.commitIndex() {
			log.Printf("Incoming CommitIndex is higher %d > %d",
				args.LeaderCommit, rf.commitIndex())

			min := args.LeaderCommit

			if len(args.Entries) != 0 &&
				args.LeaderCommit > uint64(len(rf.log)-1) {
				min = uint64(len(rf.log) - 1)
			}

			rf.setCommitIndex(min)
		}
	}
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
	// Your code here (2B).
	log := extendLoggerWithPrefix(rf.logger, startLogTopic)

	log.Printf("Start call %v", command)

	if !rf.heartbeats.IsSendingInProgress() {
		log.Printf("Not a leader")
		rf.mu.Lock()
		defer rf.mu.Unlock()
		return -1, rf.currentTerm, false
	}

	var (
		doneCh      = make(chan int)
		cAnswers    = float64(1) // already count itself
		le          = LogEntry{rf.currentTerm, command}
		ctx, cancel = context.WithCancel(context.Background())
	)

	rf.mu.Lock()
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: uint64(len(rf.log) - 1),
		PrevLogTerm:  rf.log[len(rf.log)-1].Term,
		LeaderCommit: rf.commitIndex(),
		Entries:      []LogEntry{le},
	}

	rf.log = append(rf.log, le)

	rf.mu.Unlock()

	log.Printf("append a local log")

	go func() {
		for _ = range doneCh {
			cAnswers++
			log.Printf("(%.0f/%d) answers", cAnswers, len(rf.peers))

			if cAnswers == math.Ceil(float64(len(rf.peers))/2) {
				log.Printf("increasing comminIndex")

				rf.setCommitIndex(args.PrevLogIndex + 1)

				cancel()
			}
		}
	}()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(peerID int) {
			reply := &AppendEntriesReply{}

			log.Printf("AppendEntires sent to %d", peerID)

			for {
				if ok := rf.sendAppendEntries(peerID, args.DeepCopy(), reply); !ok {
					log.Printf("WRN fail AppendEntries call to %d peer", peerID)

					continue
				}

				break
			}

			log.Printf("AppendEntires reply from %d", peerID)

			rf.mu.Lock()
			rf.processIncomingTerm(log, peerID, reply.Term)
			rf.mu.Unlock()

			select {
			case <-ctx.Done():
				return
			default:
				doneCh <- peerID
			}
		}(i)
	}

	return int(args.PrevLogIndex + 1), args.Term, true
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
	rf.stopLeaderElection()
	rf.cancel()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)

	return z == 1
}

func (rf *Raft) sendHeartbeats() {
	log := extendLoggerWithPrefix(rf.logger, heartbeatingLogTopic)

	rf.mu.Lock()
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: uint64(len(rf.log) - 1),
		PrevLogTerm:  rf.log[len(rf.log)-1].Term,
		LeaderCommit: rf.commitIndex(),
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

			rf.mu.Lock()
			rf.processIncomingTerm(log, i, reply.Term)
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) processIncomingTerm(log *log.Logger, peerID int, term int) {
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
	rf.ctx, rf.cancel = context.WithCancel(context.Background())
	rf.resetElectionTimeoutCh = make(chan struct{})

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

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	rf.applyLog()

	return rf
}
