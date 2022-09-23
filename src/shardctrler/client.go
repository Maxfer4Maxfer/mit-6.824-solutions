package shardctrler

//
// Shardctrler clerk.
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
)

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
	servers []*labrpc.ClientEnd
	// Your data here.
	log *log.Logger
	ID  int
	rID int64
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
	// Your code here.
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

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num

	for {
		requestID := fmt.Sprintf("%d_%d", ck.ID, ck.nextRequestID())
		args.RequestID = requestID
		args.CorrelationID = raft.NewCorrelationID()

		log := raft.ExtendLoggerWithCorrelationID(ck.log, args.CorrelationID)

		// try each known server.
		for i, srv := range ck.servers {
			var reply QueryReply

			log.Printf("Query -> S%d rID:%s n:%d", i, requestID, num)

			ok := srv.Call("ShardCtrler.Query", args, &reply)

			log.Printf("Query <- S%d rID:%s n:%d C:%+v WL:%v, ERR:%s",
				i, requestID, num, reply.Config, reply.WrongLeader, reply.Err)

			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers

	requestID := fmt.Sprintf("%d_%d", ck.ID, ck.nextRequestID())
	args.RequestID = requestID
	args.CorrelationID = raft.NewCorrelationID()

	log := raft.ExtendLoggerWithCorrelationID(ck.log, args.CorrelationID)

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply JoinReply

			log.Printf("Join -> S%d rID:%s S:%+v", i, requestID, servers)

			ok := srv.Call("ShardCtrler.Join", args, &reply)

			log.Printf("Join <- S%d rID:%s WL:%v ERR:%s",
				i, requestID, reply.WrongLeader, reply.Err)

			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids

	requestID := fmt.Sprintf("%d_%d", ck.ID, ck.nextRequestID())
	args.RequestID = requestID
	args.CorrelationID = raft.NewCorrelationID()

	log := raft.ExtendLoggerWithCorrelationID(ck.log, args.CorrelationID)

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply LeaveReply

			log.Printf("Leave -> S%d rID:%s GIDs:%v", i, requestID, gids)

			ok := srv.Call("ShardCtrler.Leave", args, &reply)

			log.Printf("Leave <- S%d rID:%s WL:%v ERR:%s",
				i, requestID, reply.WrongLeader, reply.Err)

			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	requestID := fmt.Sprintf("%d_%d", ck.ID, ck.nextRequestID())
	args.RequestID = requestID
	args.CorrelationID = raft.NewCorrelationID()

	log := raft.ExtendLoggerWithCorrelationID(ck.log, args.CorrelationID)

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply MoveReply

			log.Printf("Move -> S%d rID:%s S:%d GID:%d", i, requestID, shard, gid)

			ok := srv.Call("ShardCtrler.Move", args, &reply)

			log.Printf("Move <- S%d rID:%s WL:%v ERR:%s",
				i, requestID, reply.WrongLeader, reply.Err)

			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
