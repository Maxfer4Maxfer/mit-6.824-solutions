package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func SN(rID string) int64 {
	ps := strings.Split(rID, "_")
	sn, _ := strconv.Atoi(ps[1])

	return int64(sn)
}

func clerkID(rID string) int {
	ps := strings.Split(rID, "_")
	cID, _ := strconv.Atoi(ps[0])

	return cID
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	log *log.Logger
	ID  int
	rID int64
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(
	ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd,
) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.ID = int(nrand())
	log := log.New(
		os.Stdout, fmt.Sprintf("CK%d ", ck.ID), log.Lshortfile|log.Lmicroseconds)
	ck.log = raft.ExtendLoggerWithTopic(log, raft.LoggerTopicClerk)

	log.Printf("New CK_%d", ck.ID)

	return ck
}

func (ck *Clerk) nextRequestID() int64 {
	return atomic.AddInt64(&ck.rID, 1)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]

		requestID := fmt.Sprintf("%d_%d", ck.ID, ck.nextRequestID())
		args.RequestID = requestID
		args.CorrelationID = raft.NewCorrelationID()

		log := raft.ExtendLoggerWithCorrelationID(ck.log, args.CorrelationID)

		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply

				log.Printf("Get -> S%d Sh:%d GID:%d CN:%d rID:%s K:%s",
					si, shard, gid, ck.config.Num, requestID, key)

				ok := srv.Call("ShardKV.Get", &args, &reply)

				log.Printf("Get <- S%d Sh:%d GID:%d CN:%d rID:%s K:%s V:%s, Err:%s",
					si, shard, gid, ck.config.Num, requestID,
					key, reply.Value, reply.Err)

				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	var (
		requestID = fmt.Sprintf("%d_%d", ck.ID, ck.nextRequestID())
		args      = PutAppendArgs{
			RequestID: requestID,
			Key:       key,
			Value:     value,
			Op:        op,
		}
	)

	for {
		args.CorrelationID = raft.NewCorrelationID()

		log := raft.ExtendLoggerWithCorrelationID(ck.log, args.CorrelationID)

		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply

				log.Printf("%s -> S%d Sh:%d GID:%d CN:%d rID:%s K:%s V:%s",
					op, si, shard, gid, ck.config.Num, requestID, key, value)

				ok := srv.Call("ShardKV.PutAppend", &args, &reply)

				log.Printf("%s <- S%d Sh:%d GID:%d CN:%d rID:%s K:%s V:%s, Err:%s",
					op, si, shard, gid, ck.config.Num, requestID,
					key, value, reply.Err)

				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
