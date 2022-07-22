package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// CorrelationsID is used to trace requests across multiple raft instance.
	CorrelationID string

	// candidate’s term
	Term int

	// candidate requesting vote
	CandidateID int

	// index of candidate’s last log entry
	LastLogIndex int

	// term of candidate’s last log entry
	LastLogTerm int
}

func (r *RequestVoteArgs) DeepCopy() *RequestVoteArgs {
	return &RequestVoteArgs{
		CorrelationID: r.CorrelationID,
		Term:          r.Term,
		CandidateID:   r.CandidateID,
		LastLogIndex:  r.LastLogIndex,
		LastLogTerm:   r.LastLogTerm,
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
	// CorrelationsID is used to trace requests across multiple raft instance.
	CorrelationID string

	// leader’s term
	Term int

	// so follower can redirect clients
	LeaderID int

	// index of log entry immediately preceding new ones
	PrevLogIndex int

	// term of prevLogIndex entry
	PrevLogTerm int

	// log entries to store (empty for heartbeat;
	// may send more than one for efficiency)
	Entries []LogEntry

	// leader’s commitIndex
	LeaderCommit int
}

func (a *AppendEntriesArgs) DeepCopy() *AppendEntriesArgs {
	entries := make([]LogEntry, len(a.Entries))

	for i := range a.Entries {
		e := a.Entries[i].DeepCopy()

		entries[i] = *e
	}

	return &AppendEntriesArgs{
		CorrelationID: a.CorrelationID,
		Term:          a.Term,
		LeaderID:      a.LeaderID,
		PrevLogIndex:  a.PrevLogIndex,
		PrevLogTerm:   a.PrevLogTerm,
		Entries:       entries,
		LeaderCommit:  a.LeaderCommit,
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
