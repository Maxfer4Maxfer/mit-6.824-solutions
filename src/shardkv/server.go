package shardkv

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const (
	cleanupStaleSessionCheckPeriod = 500 * time.Millisecond
	refreshConfigPeriod            = 300 * time.Millisecond
	checkTransferPeriod            = 100 * time.Millisecond
)

type OpType int

const (
	OpTypePut OpType = iota
	OpTypeAppend
	OpTypeGet
	OpTypeChangeConfig
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
	case OpTypeChangeConfig:
		return "ChangeConfig"
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
	GID       int
	KeyValues map[string]string
	DRequests map[int]int64
}

type resultFunc func(Err)

type Session struct {
	Index       int
	Term        int
	Subscribers []resultFunc
	Done        bool
}

type ShardKV struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	make_end func(string) *labrpc.ClientEnd
	gid      int
	// config       shardctrler.Config
	configNum    int
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	log   *log.Logger
	store map[string]string

	configs *configCache

	// last done request groupded by cleck IDs
	dRequests map[int]int64

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
		Type:      OpTypeTransfer,
		RequestID: args.RequestID,
		ConfigNum: args.ConfigNum,
		KeyValues: args.KeyValues,
		GID:       args.GID,
		DRequests: args.DRequests,
	}

	ctx := raft.AddCorrelationID(context.Background(), args.CorrelationID)

	reply.Err = kv.callRaft(ctx, op)
}

func (kv *ShardKV) config() shardctrler.Config {
	return kv.configs.get(kv.configNum)
}

func (kv *ShardKV) changeConfig(configNum int) {
	var (
		correlationID = raft.NewCorrelationID()
		ctx           = raft.AddCorrelationID(context.Background(), correlationID)
		log           = raft.ExtendLoggerWithCorrelationID(kv.log, correlationID)
		requestID     = fmt.Sprintf("%d%d%d_%d", kv.gid, kv.me, 1, configNum)
		op            = Op{
			RequestID: requestID,
			ConfigNum: configNum,
			Type:      OpTypeChangeConfig,
		}
	)

	log.Printf("-> ChangeConfig rID:%s CN:%v", requestID, configNum)

	err := kv.callRaft(ctx, op)

	log.Printf("<- ChangeConfig rID:%s CN:%v Err:%v",
		requestID, configNum, err)
}

func (kv *ShardKV) transferCompleted(configNum int) {
	var (
		correlationID = raft.NewCorrelationID()
		ctx           = raft.AddCorrelationID(context.Background(), correlationID)
		log           = raft.ExtendLoggerWithCorrelationID(kv.log, correlationID)
		requestID     = fmt.Sprintf("%d%d%d_%d", kv.gid, kv.me, 2, configNum)
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

		lastExecutedSN, ok := kv.dRequests[clerkID(op.RequestID)]
		if ok && SN(op.RequestID) <= lastExecutedSN {
			log.Printf("Request %s is old", op.RequestID)
			resultFunc(OK)

			return
		}

		if op.Type == OpTypePut || op.Type == OpTypeAppend || op.Type == OpTypeGet {
			if kv.config().Shards[key2shard(op.Key)] != kv.gid {
				log.Printf("Wrong shard group RGUID:%d != GID:%d rID:%v",
					kv.config().Shards[key2shard(op.Key)], kv.gid, op.RequestID)

				resultFunc(ErrWrongGroup)

				return
			}

			lockedShards := kv.reconfig[kv.configNum].LockedShards
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

		log.Printf("-> rf.Start CN:%d Op:%+v", kv.configNum, op)

		idx, term, ok := kv.rf.StartWithCorrelationID(raft.GetCorrelationID(ctx), op)

		log.Printf("<- rf.Start rID:%s I:%d T:%d S:%v",
			op.RequestID, idx, term, ok)

		if !ok {
			resultFunc(ErrWrongLeader)

			return
		}

		s.Term = term
		s.Index = idx
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

	kv.log.Printf("rID:%s I:%d D:%v len(chs):%d R:%v",
		requestID, s.Index, s.Done, len(s.Subscribers), result)

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
	log := raft.ExtendLoggerWithTopic(kv.log, raft.LoggerTopicApply)

	switch op.Type {
	case OpTypeGet:
		if kv.config().Shards[key2shard(op.Key)] != kv.gid {
			log.Printf("Get wrong shard group RGUID:%d != GID:%d",
				kv.config().Shards[key2shard(op.Key)], kv.gid)

			return ErrWrongGroup
		}

		kv.log.Printf("Apply Get rID:%s K:%s", op.RequestID, op.Key)
	case OpTypePut:
		if kv.config().Shards[key2shard(op.Key)] != kv.gid {
			log.Printf("Put wrong shard group RGUID:%d != GID:%d",
				kv.config().Shards[key2shard(op.Key)], kv.gid)

			return ErrWrongGroup
		}

		kv.store[op.Key] = op.Value

		kv.log.Printf("Apply %s rID:%s K:%s V:%s",
			OpTypePut, op.RequestID, op.Key, op.Value)
	case OpTypeAppend:
		if kv.config().Shards[key2shard(op.Key)] != kv.gid {
			log.Printf("Append wrong shard group RGUID:%d != GID:%d",
				kv.config().Shards[key2shard(op.Key)], kv.gid)

			return ErrWrongGroup
		}

		v := kv.store[op.Key]
		kv.store[op.Key] = v + op.Value

		log.Printf("Apply %s rID:%s K:%s OV:%s AV:%s",
			OpTypeAppend, op.RequestID, op.Key, v, op.Value)
	case OpTypeChangeConfig:
		if op.ConfigNum <= kv.configNum {
			kv.log.Printf("Apply config dublicate rID:%s CN:%d",
				op.RequestID, op.ConfigNum)

			return OK
		}

		nConfig := kv.configs.get(op.ConfigNum)
		oConfig := kv.config()
		kv.configNum = nConfig.Num

		if _, ok := kv.reconfig[op.ConfigNum]; !ok {
			kv.reconfig[nConfig.Num] = Reconfig{
				LockedShards: make(map[int]int),
				LockedGroups: make(map[int]TransferStatus),
			}
		}

		lockedShards := kv.reconfig[nConfig.Num].LockedShards
		lockedGroups := kv.reconfig[nConfig.Num].LockedGroups

		log.Printf("Apply config %d OC:%v NC:%v LS:%v LG:%v", nConfig.Num,
			oConfig.Shards, nConfig.Shards, lockedShards, lockedGroups)

		for sid := 0; sid < shardctrler.NShards; sid++ {
			switch {
			// if take responcebility from someone else-> lock handle that shard
			// until unlock responce would be given from previous owner
			case oConfig.Shards[sid] != kv.gid &&
				nConfig.Shards[sid] == kv.gid && oConfig.Shards[sid] != 0:
				gid := oConfig.Shards[sid]

				if v, ok := lockedGroups[gid]; ok && v == transferStatusApplied {
					lockedShards[sid] = -1
					continue
				}

				lockedShards[sid] = gid
				lockedGroups[gid] = transferStatusWaiting
			}
		}
		log.Printf("Apply config %d LS:%v LG:%v",
			nConfig.Num, lockedShards, lockedGroups)
	case OpTypeTransfer:
		if _, ok := kv.reconfig[op.ConfigNum]; !ok {
			kv.reconfig[op.ConfigNum] = Reconfig{
				LockedShards: make(map[int]int),
				LockedGroups: make(map[int]TransferStatus),
			}
		}

		lockedShards := kv.reconfig[op.ConfigNum].LockedShards
		lockedGroups := kv.reconfig[op.ConfigNum].LockedGroups

		log.Printf("Apply Transfer rID:%s CN:%d len(KVs):%d LS:%v LD:%v",
			op.RequestID, op.ConfigNum, len(op.KeyValues),
			lockedShards, lockedGroups)

		if s, ok := lockedGroups[op.GID]; ok && s == transferStatusApplied {
			kv.log.Printf("Apply Transfer dublicate rID:%s CN:%d",
				op.RequestID, op.ConfigNum)

			return OK
		}

		for key, value := range op.KeyValues {
			kv.store[key] = value
		}

		lockedGroups[op.GID] = transferStatusApplied

		for sid, gid := range lockedShards {
			if op.GID == gid {
				lockedShards[sid] = -1
			}
		}

		for cID, sn := range op.DRequests {
			curSN, ok := kv.dRequests[cID]
			if ok && curSN > sn {
				continue
			}

			kv.dRequests[cID] = sn
		}

		log.Printf("Apply Transfer rID:%s CN:%d len(KVs):%d LS:%v LG:%v",
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

		if r.Completed {
			kv.log.Printf("Apply TransferCompleted already applied rID:%s CN:%d",
				op.RequestID, op.ConfigNum)

			return OK
		}

		r.Completed = true
		kv.reconfig[op.ConfigNum] = r

		oConfig := kv.configs.get(op.ConfigNum - 1)
		nConfig := kv.configs.get(op.ConfigNum)
		lostSID := make(map[int]struct{})

		for sid := 0; sid < shardctrler.NShards; sid++ {
			if oConfig.Shards[sid] == kv.gid && nConfig.Shards[sid] != kv.gid {
				if !kv.hasHigherIncome(sid, nConfig.Num) {
					lostSID[sid] = struct{}{}
				}
			}
		}

		kv.log.Printf("Apply TransferCompleted rID:%s CN:%d lostSID:%v",
			op.RequestID, op.ConfigNum, lostSID)

		for key, _ := range kv.store {
			if _, ok := lostSID[key2shard(key)]; ok {
				delete(kv.store, key)
			}
		}

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
			kv.log.Printf("Duplicate rID:%s, S:%+v", op.RequestID, s)

			duplicate = true
		}

		if SN(op.RequestID) <= kv.dRequests[clerkID(op.RequestID)] {
			kv.log.Printf("Duplicate rID:%s, SN(op):%d <= dRequest:%d",
				op.RequestID, SN(op.RequestID), kv.dRequests[clerkID(op.RequestID)])

			duplicate = true
		}

		if !duplicate {
			if err := kv.processOp(op); err != OK {
				kv.informResult(op.RequestID, err)

				kv.mu.Unlock()
				continue
			}
		}

		if SN(op.RequestID) > kv.dRequests[clerkID(op.RequestID)] {
			kv.dRequests[clerkID(op.RequestID)] = SN(op.RequestID)
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

	e.Encode(kv.dRequests)
	log.Printf("save len(dRequests):%d  len(snapshot):%d",
		len(kv.dRequests), w.Len())

	e.Encode(kv.configNum)
	log.Printf("save configNum:%+v  len(snapshot):%d", kv.configNum, w.Len())

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

	kv.store = make(map[string]string)
	if err := d.Decode(&kv.store); err != nil {
		panic(fmt.Sprintf("store not found: %v", err))
	}

	log.Printf("load len(store):%d", len(kv.store))

	kv.dRequests = make(map[int]int64)
	if err := d.Decode(&kv.dRequests); err != nil {
		panic(fmt.Sprintf("dRequests not found: %v", err))
	}

	log.Printf("load len(dRequests):%d dRequests:%v",
		len(kv.dRequests), kv.dRequests)

	if err := d.Decode(&kv.configNum); err != nil {
		panic(fmt.Sprintf("configNum not found: %v", err))
	}

	log.Printf("load configNum:%+v", kv.configNum)

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
)

func (kv *ShardKV) hasHigherIncome(sid int, configNum int) bool {
	for cn, r := range kv.reconfig {
		if cn <= configNum {
			continue
		}

		if r.LockedShards[sid] == -1 {
			return true
		}

		prevCfg := kv.configs.get(cn - 1)

		gid := prevCfg.Shards[sid]

		if s, ok := r.LockedGroups[gid]; ok && s == transferStatusApplied {
			return true
		}
	}

	return false
}

func (kv *ShardKV) earliestUncompletedConfigNum() int {
	cns := []int{}

	for cn, r := range kv.reconfig {
		if !r.Completed && cn <= kv.configNum {
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

	log.Printf("choiceOfTheRoad RT:%v NC:%v SW:%v OC:%d NC:%d",
		needToRetransfer, isNewConfig, someoneWait, oldConfigNum, newConfigNum)

	log.Printf("choiceOfTheRoad Reconfig:%v", kv.reconfig)

	switch {
	case needToRetransfer:
		log.Printf("Leader need to re-execture previous transfer")
		return roadSkip
	case someoneWait:
		log.Printf("Wait transfer from previous configuration CN:%d LS:%v",
			oldConfigNum, kv.reconfig[oldConfigNum].LockedGroups)
		return roadSkip
	case !isNewConfig:
		log.Printf("Skip: nothing new and not a fresh leader")
		return roadSkip
	case isNewConfig:
		log.Printf("New config and everything is done")
		return roadProgress
	}

	log.Printf("WRN: unexpected combunation NC:%v SW:%v NR:%v",
		isNewConfig, someoneWait, needToRetransfer)

	return roadSkip
}

func (kv *ShardKV) transferReconfig() {
	ticker := time.NewTicker(checkTransferPeriod)
	log := raft.ExtendLoggerWithTopic(kv.log, raft.LoggerTopic("TRANS"))

	for range ticker.C {
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		eucn := kv.earliestUncompletedConfigNum()

		if eucn == 0 || !isLeader {
			kv.mu.Unlock()
			continue
		}

		oConfig := kv.configs.get(eucn - 1)
		nConfig := kv.configs.get(eucn)

		log.Printf("Transfer config %d OC:%+v NC:%+v",
			nConfig.Num, oConfig.Shards, nConfig.Shards)

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
		wg := &sync.WaitGroup{}
		dRequests := make(map[int]int64, len(kv.dRequests))

		for k, v := range kv.dRequests {
			dRequests[k] = v
		}

		for sid := 0; sid < shardctrler.NShards; sid++ {
			switch {
			// lost Shard need to be transfer
			case oConfig.Shards[sid] == kv.gid && nConfig.Shards[sid] != kv.gid:
				lostSID[sid] = struct{}{}
				gid := nConfig.Shards[sid]
				if _, ok := lostGID[gid]; ok {
					continue
				}
				kvChan := make(chan []string)
				lostGID[gid] = kvChan
				wg.Add(1)
				go kv.transferToShard(wg, kvChan, nConfig, sid, dRequests)
			}
		}

		// lost responcebility -> initiate transfer related K/Vs and dSessions
		// at the end issue unlock call to the new owner
		records := []string{} // [key0, value0, key1, value1, ...]

		for key, value := range kv.store {
			if _, ok := lostSID[key2shard(key)]; ok {
				records = append(records, []string{key, value}...)
			}
		}

		log.Printf("Diff CN:%d L:%v LS:%v LG:%v len(records):%v",
			nConfig.Num, isLeader, lockedShards, lockedGroups, len(records))

		kv.mu.Unlock()

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
}
func (kv *ShardKV) refreshConfig() {
	ticker := time.NewTicker(refreshConfigPeriod)
	log := raft.ExtendLoggerWithTopic(kv.log, raft.LoggerTopic("RFCFG"))

	for range ticker.C {
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			kv.mu.Unlock()
			continue
		}

		oConfig := kv.config()
		kv.mu.Unlock()

		nConfig := kv.configs.get(oConfig.Num + 1)

		kv.mu.Lock()
		_, isLeader = kv.rf.GetState()
		if !isLeader {
			kv.mu.Unlock()
			continue
		}

		nextStep := kv.choiceOfTheRoad(log, oConfig.Num, nConfig.Num)

		kv.mu.Unlock()

		switch nextStep {
		case roadSkip:
			continue
		case roadProgress:
			log.Printf("Change config %d OC:%+v NC:%+v",
				nConfig.Num, oConfig.Shards, nConfig.Shards)

			kv.changeConfig(nConfig.Num)
		}
	}
}

func (kv *ShardKV) transferToShard(
	wg *sync.WaitGroup, kvChan chan []string,
	config shardctrler.Config, shard int, dRequests map[int]int64,
) {
	defer wg.Done()

	kvPairs := make(map[string]string)

	for kv := range kvChan {
		kvPairs[kv[0]] = kv[1]
	}

	var (
		requestID = fmt.Sprintf("%d%d_%d", kv.gid, kv.me, config.Num)
		args      = TransferArgs{
			RequestID: requestID,
			ConfigNum: config.Num,
			GID:       kv.gid,
			KeyValues: kvPairs,
			DRequests: dRequests,
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
				panic("ErrWrongConfigNumber")
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

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(
		servers, me, persister, kv.applyCh,
		[]func(rf *raft.Raft){func(rf *raft.Raft) {
			rf.SetGroupName(strconv.Itoa(gid))
		}}...)

	kv.log = log.New(
		os.Stdout, fmt.Sprintf("S%d-%d ", gid, me), log.Lshortfile|log.Lmicroseconds)
	kv.log = raft.ExtendLoggerWithTopic(kv.log, raft.LoggerTopicService)

	kv.configs = newConfigCache(kv.log, ctrlers)

	kv.store = make(map[string]string)
	kv.sessions = make(map[string]Session)
	kv.dRequests = make(map[int]int64)
	kv.reconfig = make(map[int]Reconfig)

	kv.readSnapshot(persister.ReadSnapshot())

	go kv.processApplyCh()
	go kv.cleanupStaleSessions()
	go kv.transferReconfig()
	go kv.refreshConfig()

	kv.log.Printf("Started")

	return kv
}
