package kvraft

import "6.824/raft"

type Err string

const (
	OK             Err = "OK"
	ErrInternal    Err = "ErrInternal"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrWrongOpType Err = "ErrWrongOpType"
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CorrelationID raft.CorrelationID
	RequestID     string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CorrelationID raft.CorrelationID
	RequestID     string
}

type GetReply struct {
	Err   Err
	Value string
}
