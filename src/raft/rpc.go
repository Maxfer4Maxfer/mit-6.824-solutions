package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

func (r *RequestVoteArgs) DeepCopy() *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         r.Term,
		CandidateID:  r.CandidateID,
		LastLogIndex: r.LastLogIndex,
		LastLogTerm:  r.LastLogTerm,
	}
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(
	server int, args *RequestVoteArgs, reply *RequestVoteReply,
) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	return ok
}

type AppendEntriesArgs struct {
	Term         int // leader’s term
	LeaderID     int // so follower can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // term of prevLogIndex entry
	// log entries to store (empty for heartbeat;
	// may send more than one for efficiency)
	Entries      []LogEntry
	LeaderCommit int // leader’s commitIndex
}

func (a *AppendEntriesArgs) DeepCopy() *AppendEntriesArgs {
	entries := make([]LogEntry, len(a.Entries))

	for i := range a.Entries {
		e := a.Entries[i].DeepCopy()

		entries[i] = *e
	}

	return &AppendEntriesArgs{
		Term:         a.Term,
		LeaderID:     a.LeaderID,
		PrevLogIndex: a.PrevLogIndex,
		PrevLogTerm:  a.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: a.LeaderCommit,
	}
}

type AppendEntriesReply struct {
	Term int // currentTerm, for leader to update itself
	// true if follower contained entry matching prevLogIndex and prevLogTerm
	Success bool
}

func (rf *Raft) sendAppendEntries(
	server int, args *AppendEntriesArgs, reply *AppendEntriesReply,
) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}
