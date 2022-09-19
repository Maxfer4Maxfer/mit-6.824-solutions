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
	"context"
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
	broadcastTime     = 110 * time.Millisecond
	capacityOfApplyCh = 1000
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
	applyCh    chan ApplyMsg
	bufApplyCh chan ApplyMsg

	leaderElection *leaderElectionEngine
	heartbeats     *heartbeatsEngine

	// latest term server has seen
	// (initialized to 0 on first boot, increases monotonically)
	currentTerm int

	// candidateId that received vote in current term (or -1 if none)
	votedFor int

	// Raft log
	log *RLog

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

// Log is a shurtcut for rf.log.Log(i).
func (rf *Raft) Log(idx int) LogEntry {
	return rf.log.Log(idx)
}

func (rf *Raft) commitIndex() int {
	return int(atomic.LoadInt64(&rf.vCommitIndex))
}

func (rf *Raft) setCommitIndex(log *log.Logger, ci int) {
	if ci > rf.commitIndex() {
		log.Printf("Increase commitIndex to %d", ci)

		atomic.StoreInt64(&rf.vCommitIndex, int64(ci))
		rf.commitIndexCond.Broadcast()
	} else {
		log.Printf("commitIndex already increased %d > %d", rf.commitIndex(), ci)
	}
}

func (rf *Raft) applyLogProcessing() {
	log := ExtendLoggerWithTopic(rf.logger, LoggerTopicApply)

	rf.commitIndexCond = sync.NewCond(&sync.Mutex{})

	rf.commitIndexCond.L.Lock()

	go func() {
		for msg := range rf.bufApplyCh {
			// always apply incomming Snapshot
			switch {
			case msg.CommandValid && msg.CommandIndex <= rf.lastApplied:
				continue
			case !msg.CommandValid && msg.SnapshotIndex <= rf.lastApplied:
				continue
			}

			log.Printf("apply CI:%d", msg.CommandIndex)

			rf.applyCh <- msg

			switch {
			case msg.CommandValid:
				rf.lastApplied = msg.CommandIndex
			case !msg.CommandValid:
				rf.lastApplied = msg.SnapshotIndex
			}

			log.Printf("CI:%d applied", msg.CommandIndex)
		}
	}()

	go func() {
		lastProcessed := 0
		for {
			if rf.lastApplied() == rf.commitIndex() {
				rf.commitIndexCond.Wait()
			}

			log.Printf("Apply %d -> %d", rf.lastApplied()+1, rf.commitIndex())

			for idx := rf.lastApplied() + 1; idx <= rf.commitIndex(); idx++ {
				rf.mu.Lock()
				if idx <= rf.log.lastIncludedIndex {
					rf.mu.Unlock()

					continue
				}

				command := rf.Log(idx).Command

				log.Printf("-> applyCh {I:%d T:%d command:%+v}",
					idx, rf.log.Term(idx), command)

				rf.mu.Unlock()

				applied := false

				for {
					if applied {
						break
					}

					select {
					case rf.bufApplyCh <- ApplyMsg{
						Command:      command,
						CommandValid: true,
						CommandIndex: idx,
					}:
						applied = true
					default:
						log.Printf("WRN encrease capacity of buffered applyCh: %d",
							capacityOfApplyCh)
					}
				}
			}
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
func (rf *Raft) persist(ctx context.Context) {
	rf.persistWithSnapshot(ctx, nil)
}

func (rf *Raft) persistWithSnapshot(ctx context.Context, snapshot []byte) {
	log := ExtendLoggerWithTopic(rf.logger, LoggerTopicPersister)
	log = ExtendLoggerWithCorrelationID(log, GetCorrelationID(ctx))

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log.log)
	e.Encode(rf.log.offset)
	e.Encode(rf.log.lastIncludedIndex)
	e.Encode(rf.log.lastIncludedTerm)

	state := w.Bytes()

	log.Printf("save CT:%d VF:%d LII:%d LIT:%d OF:%d len(log):%d len(state):%d",
		rf.currentTerm, rf.votedFor,
		rf.log.lastIncludedIndex, rf.log.lastIncludedTerm,
		rf.log.offset, len(rf.log.log), len(state))

	if len(snapshot) == 0 {
		rf.persister.SaveRaftState(state)
	} else {
		rf.persister.SaveStateAndSnapshot(state, snapshot)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	logger := ExtendLoggerWithTopic(rf.logger, LoggerTopicPersister)

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if d.Decode(&rf.currentTerm) != nil {
		panic("currentTerm not found")
	}

	if d.Decode(&rf.votedFor) != nil {
		panic("votedFor not found")
	}

	if d.Decode(&rf.log.log) != nil {
		panic("log not found")
	}

	if d.Decode(&rf.log.offset) != nil {
		panic("offset not found")
	}

	if d.Decode(&rf.log.lastIncludedIndex) != nil {
		panic("lastIncludedIndex not found")
	}

	if d.Decode(&rf.log.lastIncludedTerm) != nil {
		panic("lastIncludedTerm not found")
	}

	logger.Printf("load CT:%d VF:%d LII:%d LTI:%d OF:%d len(log):%d",
		rf.currentTerm, rf.votedFor, rf.log.lastIncludedIndex,
		rf.log.lastIncludedTerm, rf.log.offset, len(rf.log.log))
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here, if desired.
	log := ExtendLoggerWithTopic(rf.logger, LoggerTopicCommon)
	log.Printf("The KILL signal")

	log.Printf("State:"+
		"{L:%v LA:%d CT:%d VF:%d CI:%d len(log):%d LII:%d LTI:%d NI:%d MI:%d}",
		rf.heartbeats.IsSendingInProgress(), rf.lastApplied,
		rf.currentTerm, rf.votedFor, rf.commitIndex(),
		len(rf.log.log), rf.log.lastIncludedIndex, rf.log.lastIncludedTerm,
		rf.nextIndex, rf.matchIndex,
	)

	rf.heartbeats.StopSending()
	rf.leaderElection.StopLeaderElection()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)

	return z == 1
}

func (rf *Raft) processIncomingTerm(
	ctx context.Context, log *log.Logger, peerID int, term int,
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

		rf.persist(ctx)

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
		bufApplyCh: make(chan ApplyMsg, capacityOfApplyCh),
		log:        newRLog(),
		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
	}

	// need for first log index will be 1
	rf.log.Append(LogEntry{0, ""})

	// Your initialization code here (2A, 2B, 2C).
	rf.logger = log.New(
		os.Stdout,
		fmt.Sprintf("R%d ", me),
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

	rf.logger.Printf("Started")

	return rf
}
