package shardkv

import (
	"bytes"
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
	"6.824/shardctrler"
)

const (
	cleanupStaleSessionCheckPeriod = 500 * time.Microsecond
	refreshConfigPeriod            = 300 * time.Microsecond
)

type OpType int

const (
	OpTypePut OpType = iota
	OpTypeAppend
	OpTypeGet
	OpTypeTransfer
	OpTypeTransferCompleted
)

func (ot OpType) String() string {
	switch ot {
	case OpTypePut:
		return "Put"
	case OpTypeAppend:
		return "Append"
	case OpTypeGet:
		return "Get"
	case OpTypeTransfer:
		return "Transfer"
	case OpTypeTransferCompleted:
		return "Transfer_Completed"
	default:
		return "UNSUPPORT_OP_TYPE"
	}
}

type Op struct {
	RequestID string
	Type      OpType
	Key       string
	Value     string
	ConfigNum int
	KeyValues map[string]string
}

type resultFunc func(Err)

type Session struct {
	Index       int
	Term        int
	Subscribers []resultFunc
	Done        bool
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	config       shardctrler.Config
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister
	scclerk      *shardctrler.Clerk
	// ctrlers      []*labrpc.ClientEnd

	// Your definitions here.
	log   *log.Logger
	store map[string]string

	// last done request groupded by cleck IDs
	dRequest map[int]int64

	// active sessins waited for responce from applyCh
	sessions map[string]Session

	reconfig map[int]Reconfig
}

type TransferStatus int

const (
	// waiting for transfering (set by new config)
	transferStatusWaiting TransferStatus = iota
	// transfered (set by transfer)
	transferStatusApplied
)

type Reconfig struct {
	Completed    bool
	LockedShards map[int]int // sid -> gid
	LockedGroups map[int]TransferStatus
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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

func (kv *ShardKV) Transfer(args *TransferArgs, reply *TransferReply) {
	log := raft.ExtendLoggerWithCorrelationID(kv.log, args.CorrelationID)

	log.Printf("-> Transfer rID:%s CN:%d len(KVs):%d",
		args.RequestID, args.ConfigNum, len(args.KeyValues))

	op := Op{
		RequestID: args.RequestID,
		ConfigNum: args.ConfigNum,
		KeyValues: args.KeyValues,
		Type:      OpTypeTransfer,
	}

	ctx := raft.AddCorrelationID(context.Background(), args.CorrelationID)

	reply.Err = kv.callRaft(ctx, op)
}

func (kv *ShardKV) transferCompleted(configNum int) {
	var (
		correlationID = raft.NewCorrelationID()
		ctx           = raft.AddCorrelationID(context.Background(), correlationID)
		log           = raft.ExtendLoggerWithCorrelationID(kv.log, correlationID)
		requestID     = fmt.Sprintf("%d%d_%d", kv.gid, kv.me, configNum)
		op            = Op{
			RequestID: requestID,
			ConfigNum: configNum,
			Type:      OpTypeTransferCompleted,
		}
	)

	log.Printf("-> TransferCompleted rID:%s CN:%v", requestID, configNum)

	err := kv.callRaft(ctx, op)

	log.Printf("<- TransferCompleted rID:%s CN:%v Err:%v",
		requestID, configNum, err)
}

func (kv *ShardKV) callRaft(ctx context.Context, op Op) Err {
	log := raft.ExtendLoggerWithCorrelationID(kv.log, raft.GetCorrelationID(ctx))
	wg := sync.WaitGroup{}

	var result Err

	wg.Add(1)

	resultFunc := func(incomingResult Err) {
		result = incomingResult
		log.Printf("R: %v", result)
		wg.Done()
	}

	go func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()

		lastExecutedSN, ok := kv.dRequest[clerkID(op.RequestID)]
		if ok && SN(op.RequestID) <= lastExecutedSN {
			log.Printf("Request %s is old", op.RequestID)
			resultFunc(OK)

			return
		}

		if op.Type == OpTypePut || op.Type == OpTypeAppend || op.Type == OpTypeGet {
			if kv.config.Shards[key2shard(op.Key)] != kv.gid {
				log.Printf("Wrong shard group RGUID:%d != GID:%d rID:%v",
					kv.config.Shards[key2shard(op.Key)], kv.gid, op.RequestID)

				resultFunc(ErrWrongGroup)

				return
			}

			lockedShards := kv.reconfig[kv.config.Num].LockedShards
			if gid, ok := lockedShards[key2shard(op.Key)]; ok && gid != -1 {
				log.Printf("Waiting transfer from previous group GID:%d", gid)

				resultFunc(ErrWrongGroup)

				return
			}
		}

		s, ok := kv.sessions[op.RequestID]
		if ok && s.Done {
			log.Printf("Request %s is already executed", op.RequestID)
			resultFunc(OK)

			return
		}
		if !ok {
			log.Printf("-> rf.Start CN:%d Op:%+v", kv.config.Num, op)

			idx, term, ok := kv.rf.StartWithCorrelationID(raft.GetCorrelationID(ctx), op)

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
		kv.sessions[op.RequestID] = s
	}()

	wg.Wait()

	log.Printf("R: %v", result)

	return result
}

func (kv *ShardKV) informResult(requestID string, result Err) {
	s, ok := kv.sessions[requestID]
	if !ok {
		return
	}

	kv.log.Printf("rID:%s I:%d D:%v len(chs):%d",
		requestID, s.Index, s.Done, len(s.Subscribers))

	for _, sub := range s.Subscribers {
		sub(result)
	}

	s.Subscribers = s.Subscribers[:0]
	kv.sessions[requestID] = s

	for rID, e := range kv.sessions {
		if e.Index >= s.Index {
			continue
		}

		if len(e.Subscribers) == 0 || e.Done {
			continue
		}

		kv.log.Printf("Found the uncommitted predecessor %d < %d rID:%s",
			e.Index, s.Index, rID)

		for _, sub := range e.Subscribers {
			sub(ErrWrongLeader)
		}

		delete(kv.sessions, rID)
	}
}

func (kv *ShardKV) cleanupStaleSessions() {
	for {
		kv.mu.Lock()
		term, _ := kv.rf.GetState()

		for rID, s := range kv.sessions {
			if s.Term >= term || s.Done {
				continue
			}

			kv.log.Printf("Found a stale session rID:%s T:%d CT:%d",
				rID, s.Term, term)

			kv.informResult(rID, ErrWrongLeader)

			delete(kv.sessions, rID)
		}
		kv.mu.Unlock()

		time.Sleep(cleanupStaleSessionCheckPeriod)
	}
}

func (kv *ShardKV) processOp(op Op) Err {
	switch op.Type {
	case OpTypeGet:
	case OpTypePut:
		if kv.config.Shards[key2shard(op.Key)] != kv.gid {
			log.Printf("Wrong shard group RGUID:%d != GID:%d",
				kv.config.Shards[key2shard(op.Key)], kv.gid)

			return ErrWrongGroup
		}

		kv.store[op.Key] = op.Value

		kv.log.Printf("Apply %s rID:%s K:%s V:%s",
			OpTypePut, op.RequestID, op.Key, op.Value)
	case OpTypeAppend:
		if kv.config.Shards[key2shard(op.Key)] != kv.gid {
			log.Printf("Wrong shard group RGUID:%d != GID:%d",
				kv.config.Shards[key2shard(op.Key)], kv.gid)

			return ErrWrongGroup
		}

		v := kv.store[op.Key]
		kv.store[op.Key] = v + op.Value

		kv.log.Printf("Apply %s rID:%s K:%s OV:%s AV:%s",
			OpTypeAppend, op.RequestID, op.Key, v, op.Value)
	case OpTypeTransfer:
		if _, ok := kv.reconfig[op.ConfigNum]; !ok {
			kv.reconfig[op.ConfigNum] = Reconfig{
				LockedShards: make(map[int]int),
				LockedGroups: make(map[int]TransferStatus),
			}
		}

		lockedShards := kv.reconfig[op.ConfigNum].LockedShards
		lockedGroups := kv.reconfig[op.ConfigNum].LockedGroups

		kv.log.Printf("Apply Transfer rID:%s CN:%d len(KVs):%d LS:%v LD:%v",
			op.RequestID, op.ConfigNum, len(op.KeyValues),
			lockedShards, lockedGroups)

		for key := range op.KeyValues {
			gid, sok := lockedShards[key2shard(key)]
			status, gok := lockedGroups[gid]

			if (sok && gid == -1) ||
				(sok && gok && status == transferStatusApplied) {
				kv.log.Printf("Apply Transfer dublicate ID:%s CN:%d",
					op.RequestID, op.ConfigNum)

				return OK
			}

			break
		}

		for key, value := range op.KeyValues {
			kv.store[key] = value
		}

		for key, _ := range op.KeyValues {
			gid, ok := lockedShards[key2shard(key)]

			if ok {
				for sid, pgid := range lockedShards {
					if gid == pgid {
						lockedShards[sid] = -1
					}
				}

				lockedGroups[gid] = transferStatusApplied
			} else {
				// need to mark with -1 to identefy that transfer already done
				lockedShards[key2shard(key)] = -1
			}

			break
		}

		kv.log.Printf("Apply Transfer rID:%s CN:%d len(KVs):%d LS:%v LG:%v",
			op.RequestID, op.ConfigNum, len(op.KeyValues),
			lockedShards, lockedGroups)
	case OpTypeTransferCompleted:
		if _, ok := kv.reconfig[op.ConfigNum]; !ok {
			kv.reconfig[op.ConfigNum] = Reconfig{
				LockedShards: make(map[int]int),
				LockedGroups: make(map[int]TransferStatus),
			}
		}

		r := kv.reconfig[op.ConfigNum]
		r.Completed = true
		kv.reconfig[op.ConfigNum] = r

		kv.log.Printf("Apply TransferCompleted rID:%s CN:%d",
			op.RequestID, op.ConfigNum)
	}

	return OK
}

func (kv *ShardKV) processApplyCh() {
	for msg := range kv.applyCh {
		// Snapshot case
		if !msg.CommandValid {
			kv.log.Printf("Incoming snapshot ST:%d, SI:%d, len:%d",
				msg.SnapshotTerm, msg.SnapshotIndex, len(msg.Snapshot))

			kv.readSnapshot(msg.Snapshot)

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

		duplicate := false

		s, ok := kv.sessions[op.RequestID]
		if ok && s.Done {
			duplicate = true
		}

		if SN(op.RequestID) <= kv.dRequest[clerkID(op.RequestID)] {
			duplicate = true
		}

		if !duplicate {
			if err := kv.processOp(op); err != OK {
				kv.informResult(op.RequestID, err)

				kv.mu.Unlock()
				continue
			}
		}

		if SN(op.RequestID) > kv.dRequest[clerkID(op.RequestID)] {
			kv.dRequest[clerkID(op.RequestID)] = SN(op.RequestID)
		}

		s.Done = true
		kv.sessions[op.RequestID] = s

		kv.informResult(op.RequestID, OK)

		if kv.maxraftstate != -1 && kv.maxraftstate <= kv.persister.RaftStateSize() {
			kv.rf.Snapshot(msg.CommandIndex, kv.snapshot())
		}

		kv.mu.Unlock()
	}
}

func (kv *ShardKV) snapshot() []byte {
	log := raft.ExtendLoggerWithTopic(kv.log, raft.LoggerTopicSnapshot)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.store)
	log.Printf("save len(store):%d len(snapshot):%d", len(kv.store), w.Len())

	e.Encode(kv.dRequest)
	log.Printf("save len(dRequests):%d  len(snapshot):%d",
		len(kv.dRequest), w.Len())

	e.Encode(kv.config)
	log.Printf("save config:%+v  len(snapshot):%d", kv.config, w.Len())

	e.Encode(kv.reconfig)
	log.Printf("save len(reconfig):%d  len(snapshot):%d",
		len(kv.reconfig), w.Len())

	return w.Bytes()
}

func (kv *ShardKV) readSnapshot(data []byte) {
	log := raft.ExtendLoggerWithTopic(kv.log, raft.LoggerTopicSnapshot)

	log.Printf("load len(snapshot):%d", len(data))

	if len(data) == 0 {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if err := d.Decode(&kv.store); err != nil {
		panic(fmt.Sprintf("store not found: %v", err))
	}

	log.Printf("load len(store):%d", len(kv.store))

	if err := d.Decode(&kv.dRequest); err != nil {
		panic(fmt.Sprintf("dRequests not found: %v", err))
	}

	log.Printf("load len(dRequests):%d dRequests:%v",
		len(kv.dRequest), kv.dRequest)

	if err := d.Decode(&kv.config); err != nil {
		panic(fmt.Sprintf("config not found: %v", err))
	}

	log.Printf("load config:%+v", kv.config)

	if err := d.Decode(&kv.reconfig); err != nil {
		panic(fmt.Sprintf("reconfig not found: %v", err))
	}

	log.Printf("load len(reconfig):%d reconfig:%+v",
		len(kv.reconfig), kv.reconfig)
}

type road int

const (
	roadSkip road = iota
	roadProgress
	roadReapply
)

func (kv *ShardKV) earliestUncompletedConfigNum() int {
	cns := []int{}

	for cn, r := range kv.reconfig {
		if !r.Completed {
			cns = append(cns, cn)
		}
	}

	if len(cns) == 0 {
		return 0
	}

	sort.Ints(cns)

	return cns[0]
}

func (kv *ShardKV) choiceOfTheRoad(
	log *log.Logger,
	isLeader bool,
	oldConfigNum int,
	newConfigNum int,
) road {
	isNewConfig := oldConfigNum < newConfigNum

	someoneWait := false
	for _, v := range kv.reconfig[oldConfigNum].LockedGroups {
		if v == transferStatusWaiting {
			someoneWait = true
			break
		}
	}

	eucn := kv.earliestUncompletedConfigNum()
	needToRetransfer := eucn != 0 && eucn <= oldConfigNum

	log.Printf("choiceOfTheRoad L:%v RT:%v NC:%v SW:%v OC:%d NC:%d",
		isLeader, needToRetransfer, isNewConfig, someoneWait,
		oldConfigNum, newConfigNum)

	log.Printf("choiceOfTheRoad Reconfig:%v", kv.reconfig)

	switch {
	case !isLeader && !isNewConfig:
		log.Printf("Skip: nothing new and not a leader")
		return roadSkip
	case !isLeader && isNewConfig && someoneWait:
		log.Printf("Wait transfer from previous configuration CN:%d LS:%v",
			oldConfigNum, kv.reconfig[oldConfigNum].LockedGroups)
		return roadSkip
	case !isLeader && isNewConfig && !someoneWait:
		log.Printf("New config and everything is done")
		return roadProgress
	case isLeader && needToRetransfer:
		log.Printf("Leader need to re-execture previous transfer")
		return roadReapply
	case isLeader && !isNewConfig:
		log.Printf("Skip: nothing new and not a fresh leader")
		return roadSkip
	case isLeader && isNewConfig && someoneWait:
		log.Printf("Wait transfer from previous configuration CN:%d LS:%v",
			oldConfigNum, kv.reconfig[oldConfigNum].LockedGroups)
		return roadSkip
	case isLeader && isNewConfig && !someoneWait:
		log.Printf("New config and everything is done")
		return roadProgress
	}

	log.Printf("WRN: unexpected combunation L:%v NC:%v SW:%v NR:%v",
		isLeader, isNewConfig, someoneWait, needToRetransfer)

	return roadSkip
}

func (kv *ShardKV) refreshConfig() {
	ticker := time.NewTicker(refreshConfigPeriod)
	log := raft.ExtendLoggerWithTopic(kv.log, raft.LoggerTopic("RFCFG"))
	onlyTransfer := false

	for range ticker.C {
		kv.mu.Lock()
		oConfig := kv.config
		kv.mu.Unlock()

		nConfig := kv.scclerk.Query(oConfig.Num + 1)

		kv.mu.Lock()
		if oConfig.Num != kv.config.Num {
			kv.mu.Unlock()
			continue
		}

		_, isLeader := kv.rf.GetState()
		nextStep := kv.choiceOfTheRoad(log, isLeader, oConfig.Num, nConfig.Num)

		switch nextStep {
		case roadSkip:
			kv.mu.Unlock()
			continue
		case roadReapply:
			eucn := kv.earliestUncompletedConfigNum()

			oConfig = kv.scclerk.Query(eucn - 1)
			nConfig = kv.scclerk.Query(eucn)
			onlyTransfer = true
		case roadProgress:
			kv.config = nConfig
		}

		log.Printf("Apply config %d OC:%+v NC:%+v OT:%v",
			nConfig.Num, oConfig.Shards, nConfig.Shards, onlyTransfer)

		// compare old and new config
		if _, ok := kv.reconfig[nConfig.Num]; !ok {
			kv.reconfig[nConfig.Num] = Reconfig{
				LockedShards: make(map[int]int),
				LockedGroups: make(map[int]TransferStatus),
			}
		}
		lockedShards := kv.reconfig[nConfig.Num].LockedShards
		lockedGroups := kv.reconfig[nConfig.Num].LockedGroups
		lostGID := make(map[int]chan []string)
		lostSID := make(map[int]struct{})
		alreadyTransfered := map[int]struct{}{}
		gainedSIDs := []int{}
		wg := &sync.WaitGroup{}

		for sid := 0; sid < shardctrler.NShards; sid++ {
			switch {
			// lost Shard need to be transfer
			case oConfig.Shards[sid] == kv.gid && nConfig.Shards[sid] != kv.gid:
				if isLeader {
					lostSID[sid] = struct{}{}
					gid := nConfig.Shards[sid]
					if _, ok := lostGID[gid]; ok {
						continue
					}
					kvChan := make(chan []string)
					lostGID[gid] = kvChan
					wg.Add(1)
					go kv.transferToShard(wg, kvChan, nConfig, sid)
				}
			// if take responcebility from someone else-> lock handle that shard
			// until unlock responce would be given from previous owner
			case oConfig.Shards[sid] != kv.gid &&
				nConfig.Shards[sid] == kv.gid && oConfig.Shards[sid] != 0:
				gid := oConfig.Shards[sid]

				if lockedShards[sid] == -1 {
					alreadyTransfered[gid] = struct{}{}
				}

				gainedSIDs = append(gainedSIDs, sid)
			}
		}

		for _, sid := range gainedSIDs {
			gid := oConfig.Shards[sid]
			if v, ok := lockedGroups[gid]; ok && v == transferStatusApplied {
				lockedShards[sid] = -1
				continue
			}

			if _, ok := lockedShards[sid]; ok {
				continue
			}

			if _, ok := alreadyTransfered[gid]; ok {
				lockedShards[sid] = -1
				lockedGroups[gid] = transferStatusApplied
				continue
			}

			lockedShards[sid] = gid
			lockedGroups[gid] = transferStatusWaiting
		}

		// lost responcebility -> initiate transfer related K/Vs and dSessions
		// at the end issue unlock call to the new owner
		records := []string{} // [key0, value0, key1, value1, ...]

		if isLeader {
			for key, value := range kv.store {
				if _, ok := lostSID[key2shard(key)]; ok {
					records = append(records, []string{key, value}...)
				}
			}
		}

		log.Printf("Diff CN:%d L:%v LS:%v LG:%v len(records):%v",
			nConfig.Num, isLeader, lockedShards, lockedGroups, len(records))

		kv.mu.Unlock()

		if isLeader {
			for i := 0; i < len(records); i += 2 {
				gid := nConfig.Shards[key2shard(records[i])]
				kvChan := lostGID[gid]

				kvChan <- []string{records[i], records[i+1]}
			}

			for i := range lostGID {
				close(lostGID[i])
			}

			wg.Wait()
			kv.transferCompleted(nConfig.Num)
		}

		onlyTransfer = false
	}
}

func (kv *ShardKV) transferToShard(
	wg *sync.WaitGroup, kvChan chan []string,
	config shardctrler.Config, shard int,
) {
	defer wg.Done()

	kvPairs := make(map[string]string)

	for kv := range kvChan {
		kvPairs[kv[0]] = kv[1]
	}

	var (
		requestID = fmt.Sprintf("%d%d_%d", kv.gid, kv.me, config.Num)
		args      = TransferArgs{
			ConfigNum: config.Num,
			RequestID: requestID,
			KeyValues: kvPairs,
		}
		gid     = config.Shards[shard]
		servers = config.Groups[gid]
	)

	args.CorrelationID = raft.NewCorrelationID()
	log := raft.ExtendLoggerWithCorrelationID(kv.log, args.CorrelationID)

	for {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply TransferReply

			log.Printf("Transfer -> S%d-%d rID:%s CN:%d len(KVs):%d KV:%v",
				gid, si, requestID, args.ConfigNum, len(args.KeyValues), args.KeyValues)

			ok := srv.Call("ShardKV.Transfer", &args, &reply)

			log.Printf("Transfer <- S%d-%d OK:%v CN:%d rID:%s Err:%s",
				gid, si, ok, config.Num, requestID, reply.Err)

			if ok && reply.Err == OK {
				return
			}

			if ok && reply.Err == ErrWrongGroup {
				panic("ErrWrongGroup")
			}

			if ok && reply.Err == ErrWrongConfigNumber {
				time.Sleep(refreshConfigPeriod)
			}
		}
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	log := raft.ExtendLoggerWithTopic(kv.log, raft.LoggerTopicCommon)
	log.Printf("The KILL signal")

	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[]: contains the ports of the servers in this group.
// me: the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
// gid: is this group's GID, for interacting with the shardctrler.
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(
	servers []*labrpc.ClientEnd, me int, persister *raft.Persister,
	maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd,
	make_end func(string) *labrpc.ClientEnd,
) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	kv.make_end = make_end
	kv.gid = gid
	// kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.scclerk = shardctrler.MakeClerk(ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.log = log.New(
		os.Stdout, fmt.Sprintf("S%d-%d ", gid, me), log.Lshortfile|log.Lmicroseconds)
	kv.log = raft.ExtendLoggerWithTopic(kv.log, raft.LoggerTopicService)

	kv.store = make(map[string]string)
	kv.sessions = make(map[string]Session)
	kv.dRequest = make(map[int]int64)
	kv.reconfig = make(map[int]Reconfig)

	kv.readSnapshot(persister.ReadSnapshot())

	go kv.processApplyCh()
	go kv.cleanupStaleSessions()
	go kv.refreshConfig()

	kv.log.Printf("Started")

	return kv
}
