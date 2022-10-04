package shardkv

import (
	"6.824/raft"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

type Err string

const (
	OK                   Err = "OK"
	ErrInternal          Err = "ErrInternal"
	ErrNoKey             Err = "ErrNoKey"
	ErrWrongGroup        Err = "ErrWrongGroup"
	ErrWrongLeader       Err = "ErrWrongLeader"
	ErrWrongOpType       Err = "ErrWrongOpType"
	ErrWrongConfigNumber Err = "ErrWrongConfigNumber"
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
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
	Key           string
	CorrelationID raft.CorrelationID
	RequestID     string
}

type GetReply struct {
	Err   Err
	Value string
}

type ChangeConfigArgs struct {
	CorrelationID raft.CorrelationID
	RequestID     string
	ConfigNum     int
}

type ChangeConfigReply struct {
	Err Err
}

type TransferArgs struct {
	ConfigNum     int
	KeyValues     map[string]string
	DRequests     map[int]int64
	GID           int
	CorrelationID raft.CorrelationID
	RequestID     string
}

type TransferReply struct {
	Err Err
}
