package kvraft

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"os"
	"sync"

	"6.824/labrpc"
	"6.824/raft"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	log    *log.Logger
	mu     sync.RWMutex
	leader int
	id     int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = int(nrand()) % 100

	log := log.New(
		os.Stdout, fmt.Sprintf("CK%d ", ck.id), log.Lshortfile|log.Lmicroseconds)
	ck.log = raft.ExtendLoggerWithTopic(log, raft.LoggerTopicClerk)

	return ck
}

func (ck *Clerk) changeLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	oldl := ck.leader

	for {
		newl := int(nrand()) % len(ck.servers)
		if newl != oldl {
			ck.leader = newl

			ck.log.Printf("Change server %d -> %d", oldl, newl)

			return newl
		}

	}
}

func (ck *Clerk) Leader() int {
	ck.mu.RLock()
	defer ck.mu.RUnlock()

	return ck.leader
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	var (
		args = GetArgs{
			Key: key,
		}
		reply = GetReply{}
	)

	i := ck.Leader()

	for {
		requestID := fmt.Sprintf("%d_%d", ck.id, int(nrand()))
		args.CorrelationID = raft.NewCorrelationID()
		args.RequestID = requestID

		log := raft.ExtendLoggerWithCorrelationID(ck.log, args.CorrelationID)

		log.Printf("Get -> S%d rID:%s K:%s", i, requestID, key)

		if ok := ck.servers[i].Call("KVServer.Get", &args, &reply); !ok {
			ck.log.Printf("WRN failed to call KVServer.Get")

			continue
		}

		log.Printf("Get <- S%d rID:%s K:%s V:%s ERR:%s",
			i, key, requestID, reply.Value, reply.Err)

		switch {
		case reply.Err == OK:
			return reply.Value
		case reply.Err == ErrNoKey:
			return ""
		case reply.Err == ErrWrongLeader:
			i = ck.changeLeader()

			continue
		default:
			log.Printf("WRN unexpected error %v", reply.Err)

			continue
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	var (
		requestID = fmt.Sprintf("%d_%d", ck.id, int(nrand()))
		args      = PutAppendArgs{
			RequestID: requestID,
			Key:       key,
			Value:     value,
			Op:        op,
		}
		reply = PutAppendReply{}
	)

	i := ck.Leader()

	for {
		args.CorrelationID = raft.NewCorrelationID()

		log := raft.ExtendLoggerWithCorrelationID(ck.log, args.CorrelationID)

		log.Printf("%s -> S%d rID:%s K:%s V:%s", op, i, requestID, key, value)

		if ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply); !ok {
			log.Printf("WRN failed to call KVServer.Get")

			i = ck.changeLeader()

			continue
		}

		log.Printf("%s <- S%d rID:%s K:%s V:%s Err:%s",
			op, i, requestID, key, value, reply.Err)

		switch {
		case reply.Err == OK:
			return
		case reply.Err == ErrWrongLeader:
			i = ck.changeLeader()

			continue
		default:
			log.Printf("WRN unexpected error %v", reply.Err)

			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
