package kvraft

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType int

const (
	OpTypePut OpType = iota
	OpTypeAppend
	OpTypeGet
)

func (ot OpType) String() string {
	switch ot {
	case OpTypePut:
		return "Put"
	case OpTypeAppend:
		return "Append"
	case OpTypeGet:
		return "Get"
	default:
		return "UNSUPPORT_OP_TYPE"
	}
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestID string
	Type      OpType
	Key       string
	Value     string
}

func (op Op) Equal(a *Op) bool {
	if a == nil {
		return false
	}

	switch {
	case op.Type != a.Type:
		return false
	case op.Key != a.Key:
		return false
	case op.Value != a.Value:
		return false
	default:
		return true
	}
}

type resultFunc func(Err)

type request struct {
	Op          *Op
	Index       int
	ResultFuncs []resultFunc
	Done        bool
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	log      *log.Logger
	store    map[string]string
	requests map[string]request
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	log := raft.ExtendLoggerWithCorrelationID(kv.log, args.CorrelationID)

	log.Printf("-> Get rID:%s K:%s", args.RequestID, args.Key)

	op := Op{
		RequestID: args.RequestID,
		Key:       args.Key,
		Type:      OpTypeGet,
	}

	ctx := raft.AddCorrelationID(context.Background(), args.CorrelationID)

	if err := kv.callRaft(ctx, op); err != OK {
		reply.Err = err

		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	v, ok := kv.store[args.Key]
	if !ok {
		reply.Err = ErrNoKey

		return
	}

	reply.Err = OK
	reply.Value = v
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	log := raft.ExtendLoggerWithCorrelationID(kv.log, args.CorrelationID)

	log.Printf("-> PutAppend Op:%s rID:%s K:%s V:%s",
		args.Op, args.RequestID, args.Key, args.Value)

	op := Op{
		RequestID: args.RequestID,
		Key:       args.Key,
		Value:     args.Value,
	}

	switch args.Op {
	case "Append":
		op.Type = OpTypeAppend
	case "Put":
		op.Type = OpTypePut
	default:
		reply.Err = ErrWrongOpType

		return
	}

	ctx := raft.AddCorrelationID(context.Background(), args.CorrelationID)

	reply.Err = kv.callRaft(ctx, op)
}

func (kv *KVServer) callRaft(ctx context.Context, op Op) Err {
	log := raft.ExtendLoggerWithCorrelationID(kv.log, raft.GetCorrelationID(ctx))
	wg := sync.WaitGroup{}

	var result Err

	wg.Add(1)

	var resultFunc resultFunc

	resultFunc = func(incomingResult Err) {
		result = incomingResult
		log.Printf("R: %v", result)
		wg.Done()
	}

	go func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()

		r, ok := kv.requests[op.RequestID]

		if ok && r.Done {
			log.Printf("Request %s is already executed", op.RequestID)
			resultFunc(OK)

			return
		}

		if !ok {
			log.Printf("-> rf.Start Op:%+v", op)

			idx, term, ok := kv.rf.StartWithCorrelationID(raft.GetCorrelationID(ctx), op)

			log.Printf("<- rf.Start rID:%s I:%d T:%d S:%v",
				op.RequestID, idx, term, ok)

			if !ok {
				resultFunc(ErrWrongLeader)

				return
			}

			r.Op = &op
			r.Index = idx
		}

		r.ResultFuncs = append(r.ResultFuncs, resultFunc)
		kv.requests[op.RequestID] = r
	}()

	wg.Wait()

	log.Printf("R: %v", result)

	return result
}

func (kv *KVServer) informResult(requestID string, idx int, result Err) {
	r, ok := kv.requests[requestID]
	if !ok {
		return
	}

	kv.log.Printf("rID:%s I:%d D:%v len(chs):%d",
		requestID, r.Index, r.Done, len(r.ResultFuncs))

	for _, rf := range r.ResultFuncs {
		rf(result)
	}

	r.ResultFuncs = []resultFunc{}
	r.Index = idx
	r.Done = true
	kv.requests[requestID] = r

	// kv.log.Printf("!!!rID:%+v", kv.requests)

	for rID, e := range kv.requests {
		if e.Index >= r.Index {
			continue
		}

		if len(e.ResultFuncs) == 0 {
			continue
		}

		kv.log.Printf("Found the uncommited predecessor %d < %d rID:%s Op:%+v",
			e.Index, r.Index, rID, e.Op)

		for _, rf := range e.ResultFuncs {
			rf(ErrWrongLeader)
		}

		e.ResultFuncs = []resultFunc{}
		kv.requests[rID] = e
	}
}

func (kv *KVServer) processOp(op Op) {
	switch op.Type {
	case OpTypeGet:
	case OpTypePut:
		kv.store[op.Key] = op.Value

		kv.log.Printf("Apply %s rID:%s K:%s V:%s",
			OpTypePut, op.RequestID, op.Key, op.Value)
	case OpTypeAppend:
		v := kv.store[op.Key]
		kv.store[op.Key] = v + op.Value

		kv.log.Printf("Apply %s rID:%s K:%s OV:%s AV:%s",
			OpTypeAppend, op.RequestID, op.Key, v, op.Value)
	}
}

func (kv *KVServer) processApplyCh() {
	for msg := range kv.applyCh {
		// Snapshot case
		if !msg.CommandValid {
			kv.log.Printf("Incomming stapshot ST:%d, SI:%d, len:%d",
				msg.SnapshotTerm, msg.SnapshotIndex, len(msg.Snapshot))

			continue
		}

		kv.log.Printf("Incoming applyMsg CI:%d, Command:%+v",
			msg.CommandIndex, msg.Command)

		op, ok := msg.Command.(Op)
		if !ok {
			kv.log.Printf("WRN returned command not Op type I:%d CMD:%+v",
				msg.CommandIndex, msg.Command)

			continue
		}

		kv.mu.Lock()
		var err Err

		r, ok := kv.requests[op.RequestID]

		switch {
		case ok && !op.Equal(r.Op):
			kv.log.Printf("WRN not leader anymore Op != origOp %+v != %+v",
				op, r.Op)

			err = ErrWrongLeader
		// already done: duplicate entry in the log
		case ok && r.Done:
			err = OK
		// first see or did not apply yet
		default:
			err = OK
			kv.processOp(op)
			r.Done = true
			kv.requests[op.RequestID] = r
		}

		kv.informResult(op.RequestID, msg.CommandIndex, err)
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(
	servers []*labrpc.ClientEnd, me int,
	persister *raft.Persister, maxraftstate int,
) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.log = log.New(
		os.Stdout, fmt.Sprintf("S%d ", me), log.Lshortfile|log.Lmicroseconds)
	kv.log = raft.ExtendLoggerWithTopic(kv.log, raft.LoggerTopicService)

	kv.store = make(map[string]string)
	kv.requests = make(map[string]request)

	go kv.processApplyCh()

	kv.log.Printf("Started")

	return kv
}
