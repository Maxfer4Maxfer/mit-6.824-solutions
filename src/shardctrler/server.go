package shardctrler

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const cleanupStaleSessionCheckPeriod = 500 * time.Millisecond

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	log *log.Logger

	configs []Config // indexed by config num

	// last done request groupded by cleck IDs
	dRequest map[int]int64

	// active sessins waited for responce from applyCh
	sessions map[string]Session
}

type OpType int

const (
	OpTypeQuery OpType = iota
	OpTypeJoin
	OpTypeLeave
	OpTypeMove
)

func (ot OpType) String() string {
	switch ot {
	case OpTypeQuery:
		return "Query"
	case OpTypeJoin:
		return "Join"
	case OpTypeLeave:
		return "Leave"
	case OpTypeMove:
		return "Move"
	default:
		return "UNSUPPORT_OP_TYPE"
	}
}

type Op struct {
	// Your data here.
	RequestID string
	Type      OpType
	// Query
	Num int
	// Join
	Servers map[int][]string
	// Leave
	GIDs []int
	// Move
	Shard int
	GID   int
}

type resultFunc func(Err)

type Session struct {
	Index       int
	Term        int
	Subscribers []resultFunc
	Done        bool
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	log := raft.ExtendLoggerWithCorrelationID(sc.log, args.CorrelationID)

	log.Printf("-> Join rID:%s S:%+v", args.RequestID, args.Servers)

	op := Op{
		Type:      OpTypeJoin,
		RequestID: args.RequestID,
		Servers:   args.Servers,
	}

	ctx := raft.AddCorrelationID(context.Background(), args.CorrelationID)

	reply.Err = sc.callRaft(ctx, op)

	if reply.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	log := raft.ExtendLoggerWithCorrelationID(sc.log, args.CorrelationID)

	log.Printf("-> Leave rID:%s GUIs:%d", args.RequestID, args.GIDs)

	op := Op{
		Type:      OpTypeLeave,
		RequestID: args.RequestID,
		GIDs:      args.GIDs,
	}

	ctx := raft.AddCorrelationID(context.Background(), args.CorrelationID)

	reply.Err = sc.callRaft(ctx, op)

	if reply.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	log := raft.ExtendLoggerWithCorrelationID(sc.log, args.CorrelationID)

	log.Printf("-> Move rID:%s S:%d GID:%d", args.RequestID, args.Shard, args.GID)

	op := Op{
		Type:      OpTypeMove,
		RequestID: args.RequestID,
		Shard:     args.Shard,
		GID:       args.GID,
	}

	ctx := raft.AddCorrelationID(context.Background(), args.CorrelationID)

	reply.Err = sc.callRaft(ctx, op)

	if reply.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	log := raft.ExtendLoggerWithCorrelationID(sc.log, args.CorrelationID)

	log.Printf("-> Get rID:%s N:%d", args.RequestID, args.Num)

	op := Op{
		RequestID: args.RequestID,
		Num:       args.Num,
		Type:      OpTypeQuery,
	}

	ctx := raft.AddCorrelationID(context.Background(), args.CorrelationID)

	if err := sc.callRaft(ctx, op); err != OK {
		reply.Err = err

		if err == ErrWrongLeader {
			reply.WrongLeader = true
		}

		return
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	if args.Num > len(sc.configs)-1 || args.Num == -1 {
		reply.Config = sc.configs[len(sc.configs)-1]
	} else {
		reply.Config = sc.configs[args.Num]
	}

	reply.WrongLeader = false
	reply.Err = OK
}

func (sc *ShardCtrler) callRaft(ctx context.Context, op Op) Err {
	log := raft.ExtendLoggerWithCorrelationID(sc.log, raft.GetCorrelationID(ctx))
	wg := sync.WaitGroup{}

	var result Err

	wg.Add(1)

	resultFunc := func(incomingResult Err) {
		result = incomingResult
		log.Printf("R: %v", result)
		wg.Done()
	}

	go func() {
		sc.mu.Lock()
		defer sc.mu.Unlock()

		lastExecutedSN, ok := sc.dRequest[clerkID(op.RequestID)]
		if ok && SN(op.RequestID) <= lastExecutedSN {
			log.Printf("Request %s is old", op.RequestID)
			resultFunc(OK)

			return
		}

		s, ok := sc.sessions[op.RequestID]
		if ok && s.Done {
			log.Printf("Request %s is already executed", op.RequestID)
			resultFunc(OK)

			return
		}

		if !ok {
			log.Printf("-> rf.Start Op:%+v", op)

			idx, term, ok := sc.rf.StartWithCorrelationID(raft.GetCorrelationID(ctx), op)

			log.Printf("<- rf.Start rID:%s I:%d T:%d S:%v",
				op.RequestID, idx, term, ok)

			if !ok {
				resultFunc(ErrWrongLeader)

				return
			}

			s.Term = term
			s.Index = idx
		}

		s.Subscribers = append(s.Subscribers, resultFunc)
		sc.sessions[op.RequestID] = s
	}()

	wg.Wait()

	log.Printf("R: %v", result)

	return result
}

func (sc *ShardCtrler) rebalance(c Config) Config {
	// count by occupied slots
	count := make(map[int]int, len(c.Groups))
	for gid := range c.Groups {
		count[gid] = 0
	}
	for _, gid := range c.Shards {
		count[gid]++
	}

	// optimal distribution
	od := NShards / len(count)

	dCondidates := []int{} // dicreasing
	iCondidates := []int{} // increasing

	for gid, c := range count {
		switch {
		case c > od:
			dCondidates = append(dCondidates, gid)
		case c < od:
			iCondidates = append(iCondidates, gid)
		}
	}

	sort.Ints(dCondidates)
	sort.Ints(iCondidates)

	sc.log.Printf("count:%+v ", count)
	sc.log.Printf("iCondidates:%+v ", iCondidates)
	sc.log.Printf("dCondidates:%+v ", dCondidates)

	move := func(from, to int) {
		for i := len(c.Shards) - 1; i >= 0; i-- {
			if c.Shards[i] == from {
				c.Shards[i] = to

				return
			}
		}
	}

	d, i := 0, 0 // iterators

	for {
		if d == len(dCondidates) || i == len(iCondidates) {
			break
		}

		move(dCondidates[d], iCondidates[i])
		count[dCondidates[d]]--
		count[iCondidates[i]]++

		if count[dCondidates[d]] == od {
			d++
		}

		if count[iCondidates[i]] == od {
			i++
		}
	}

	mod := NShards % len(count)
	sc.log.Printf("op:%+v ", od)
	sc.log.Printf("mod:%+v ", mod)

	iCondidates = []int{} // increasing
	dCondidates = []int{} // increasing

	for gid, c := range count {
		if c > od+1 {
			dCondidates = append(dCondidates, gid)
		}
		if c == od {
			iCondidates = append(iCondidates, gid)
		}
	}

	sort.Ints(dCondidates)
	sort.Ints(iCondidates)

	sc.log.Printf("count:%+v ", count)
	sc.log.Printf("iCondidates:%+v ", iCondidates)
	sc.log.Printf("dCondidates:%+v ", dCondidates)

	d, i = 0, 0
	for {
		if d == len(dCondidates) || i == len(iCondidates) {
			break
		}

		move(dCondidates[d], iCondidates[i])
		count[dCondidates[d]]--
		count[iCondidates[i]]++

		if count[dCondidates[d]] == od {
			d++
		}
		i++
	}

	sc.log.Printf("count:%+v ", count)
	sc.log.Printf("out:%+v ", c)

	return c
}

func (sc *ShardCtrler) processOp(op Op) {
	switch op.Type {
	case OpTypeQuery:
	case OpTypeJoin:
		// The Join RPC is used by an administrator to add new replica groups.
		// Its argument is a set of mappings from unique, non-zero replica group
		// identifiers (GIDs) to lists of server names. The shardctrler should react
		// by creating a new configuration that includes the new replica groups.
		// The new configuration should divide the shards as evenly as possible
		// among the full set of groups, and should move as few shards as possible
		// to achieve that goal. The shardctrler should allow re-use of a GID
		// if it's not part of the current configuration
		// (i.e. a GID should be allowed to Join, then Leave, then Join again).
		nc := sc.configs[len(sc.configs)-1].DeepCopy()

		nc.Num += 1

		ngids := make([]int, 0, len(op.Servers))
		for gid, servers := range op.Servers {
			nc.Groups[gid] = servers
			ngids = append(ngids, gid)
		}

		sort.Ints(ngids)

		// occupy free slots
		for i := range nc.Shards {
			if nc.Shards[i] == 0 {
				nc.Shards[i] = ngids[0]
			}
		}

		nc = sc.rebalance(nc)

		sc.configs = append(sc.configs, nc)

		sc.log.Printf("Join rID:%s PC:%+v NC:%+v",
			op.RequestID, sc.configs[nc.Num-1], nc)
	case OpTypeLeave:
		// The Leave RPC's argument is a list of GIDs of previously joined groups.
		// The shardctrler should create a new configuration that does not include
		// those groups, and that assigns those groups' shards to the remaining
		// groups. The new configuration should divide the shards as evenly
		// as possible among the groups, and should move as few shards as possible
		// to achieve that goal.
		nc := sc.configs[len(sc.configs)-1].DeepCopy()

		nc.Num += 1

		for _, gid := range op.GIDs {
			delete(nc.Groups, gid)
		}

		if len(nc.Groups) != 0 {
			// occupy free slots
			rgids := make([]int, 0, len(nc.Groups)) // remain gids
			for gid := range nc.Groups {
				rgids = append(rgids, gid)
			}

			sort.Ints(rgids)

			for i, gid := range nc.Shards {
				_, ok := nc.Groups[gid]
				if !ok {
					nc.Shards[i] = rgids[0]
				}
			}
		} else {
			for i, _ := range nc.Shards {
				nc.Shards[i] = 0
			}
		}

		nc = sc.rebalance(nc)

		sc.configs = append(sc.configs, nc)

		sc.log.Printf("Leave rID:%s PC:%+v NC:%+v",
			op.RequestID, sc.configs[nc.Num-1], nc)
	case OpTypeMove:
		// The Move RPC's arguments are a shard number and a GID. The shardctrler
		// should create a new configuration in which the shard is assigned to
		// the group. The purpose of Move is to allow us to test your software.
		// A Join or Leave following a Move will likely un-do the Move, since
		// Join and Leave re-balance.
		nc := sc.configs[len(sc.configs)-1].DeepCopy()

		nc.Num += 1

		nc.Shards[op.Shard] = op.GID

		sc.configs = append(sc.configs, nc)

		sc.log.Printf("Move rID:%s PC:%+v NC:%+v",
			op.RequestID, sc.configs[nc.Num-1], nc)
	}
}

func (sc *ShardCtrler) processApplyCh() {
	for msg := range sc.applyCh {
		// Snapshot case
		if !msg.CommandValid {
			sc.log.Printf("Incoming snapshot ST:%d, SI:%d, len:%d",
				msg.SnapshotTerm, msg.SnapshotIndex, len(msg.Snapshot))

			panic("Snapshot unsupported")

			continue
		}

		sc.log.Printf("Incoming applyMsg CI:%d, Command:%+v",
			msg.CommandIndex, msg.Command)

		op, ok := msg.Command.(Op)
		if !ok {
			sc.log.Printf("WRN returned command not Op type I:%d CMD:%+v",
				msg.CommandIndex, msg.Command)

			continue
		}

		sc.mu.Lock()

		duplicate := false

		if s, ok := sc.sessions[op.RequestID]; ok && s.Done {
			duplicate = true
		} else {
			s.Done = true
			sc.sessions[op.RequestID] = s
		}

		if SN(op.RequestID) <= sc.dRequest[clerkID(op.RequestID)] {
			duplicate = true
		}

		if !duplicate {
			sc.processOp(op)
		}

		if SN(op.RequestID) > sc.dRequest[clerkID(op.RequestID)] {
			sc.dRequest[clerkID(op.RequestID)] = SN(op.RequestID)
		}

		sc.informResult(op.RequestID, OK)

		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) informResult(requestID string, result Err) {
	s, ok := sc.sessions[requestID]
	if !ok {
		return
	}

	sc.log.Printf("rID:%s I:%d D:%v len(chs):%d",
		requestID, s.Index, s.Done, len(s.Subscribers))

	for _, sub := range s.Subscribers {
		sub(result)
	}

	s.Subscribers = s.Subscribers[:0]
	sc.sessions[requestID] = s

	for rID, e := range sc.sessions {
		if e.Index >= s.Index {
			continue
		}

		if len(e.Subscribers) == 0 || e.Done {
			continue
		}

		sc.log.Printf("Found the uncommitted predecessor %d < %d rID:%s",
			e.Index, s.Index, rID)

		for _, sub := range e.Subscribers {
			sub(ErrWrongLeader)
		}

		delete(sc.sessions, rID)
	}
}

func (sc *ShardCtrler) cleanupStaleSessions() {
	for {
		sc.mu.Lock()
		term, _ := sc.rf.GetState()

		for rID, s := range sc.sessions {
			if s.Term >= term || s.Done {
				continue
			}

			sc.log.Printf("Found a stale session rID:%s T:%d CT:%d",
				rID, s.Term, term)

			sc.informResult(rID, ErrWrongLeader)

			delete(sc.sessions, rID)
		}

		sc.mu.Unlock()

		time.Sleep(cleanupStaleSessionCheckPeriod)
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(
		servers, me, persister, sc.applyCh,
		[]func(rf *raft.Raft){func(rf *raft.Raft) {
			rf.SetGroupName("SCTR")
		}}...)

	// Your code here.
	sc.log = log.New(
		os.Stdout, fmt.Sprintf("S%d ", me), log.Lshortfile|log.Lmicroseconds)
	sc.log = raft.ExtendLoggerWithTopic(sc.log, raft.LoggerTopicService)

	sc.sessions = make(map[string]Session)
	sc.dRequest = make(map[int]int64)

	go sc.processApplyCh()
	go sc.cleanupStaleSessions()

	sc.log.Printf("Started")

	return sc
}
