package shardctrler

import "6.824/raft"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Each configuration describes a set of replica groups
// and an assignment of shards to replica groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (c *Config) DeepCopy() Config {

	var shards [NShards]int

	for i := range c.Shards {
		shards[i] = c.Shards[i]
	}

	groups := make(map[int][]string, len(c.Shards))

	for gid, servers := range c.Groups {
		ss := make([]string, len(servers))
		copy(ss, servers)
		groups[gid] = ss
	}

	return Config{
		Num:    c.Num,
		Shards: shards,
		Groups: groups,
	}
}

// Config {
// Num: 1,
//          0 1 2 3 4 5 6 7 8 9
// Shards: [1 1 1 1 1 2 2 2 2 0]
// Groups: {1:[s0, s1, s3], 2:[s2, s4, s5]}
// }

type Err string

const (
	OK             Err = "OK"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrNoKey       Err = "ErrNoKey"
)

type JoinArgs struct {
	Servers       map[int][]string // new GID -> servers mappings
	CorrelationID raft.CorrelationID
	RequestID     string
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs          []int
	CorrelationID raft.CorrelationID
	RequestID     string
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard         int
	GID           int
	CorrelationID raft.CorrelationID
	RequestID     string
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num           int // desired config number
	CorrelationID raft.CorrelationID
	RequestID     string
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
