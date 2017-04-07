package raftkv

import (
	"crypto/rand"
	"fmt"
	"labrpc"
	"math/big"
	"sync"
	"time"

	"github.com/jhzhu89/log"
	"github.com/satori/go.uuid"
)

type Clerk struct {
	sync.Mutex
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderMu sync.Mutex
	leader   int
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
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{key, uuid.NewV1()}
	var wrongLeader bool
	for {
		log.V(2).Field("key", key).Infoln("client, Get...")
		ck.leaderMu.Lock()
		if wrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
		leader := ck.leader
		ck.leaderMu.Unlock()
		reply := GetReply{}
		ok := ck.doRPCRetry(leader, "RaftKV.Get", &args, &reply)
		if !ok || reply.WrongLeader || reply.Err != "" {
			log.V(1).Field("reply", reply).Field("server", leader).Info("...")
			wrongLeader = true
			log.Field("reply", reply).Field("server", leader).Warningln("...")
			continue
		}
		if reply.Pending {
			wrongLeader = false
			time.Sleep(200 * time.Millisecond)
			continue
		}
		log.V(1).Infoln("client, Get finished...")
		return reply.Value
	}
	panic(fmt.Sprintf("no leader found..."))
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{key, value, op, uuid.NewV1()}
	var wrongLeader bool
	for {
		reply := PutAppendReply{}
		ck.leaderMu.Lock()
		if wrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
		leader := ck.leader
		ck.leaderMu.Unlock()
		log.V(2).Field("key", key).Field("value", value).Field("server", leader).
			Field("op", op).Infoln("client, PutAppend...")
		ok := ck.doRPCRetry(leader, "RaftKV.PutAppend", &args, &reply)
		log.V(1).Field("reply", reply).Field("server", leader).Info("...")
		if !ok || reply.WrongLeader || reply.Err != "" {
			wrongLeader = true
			continue
		}
		if reply.Pending {
			wrongLeader = true
			time.Sleep(20 * time.Millisecond)
			continue
		}
		log.V(1).Field("key", key).Field("value", value).
			Field("op", op).Infoln("client, finished PutAppend...")
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) doRPCWithTimeout(server int, svcMeth string, args interface{},
	reply interface{}, timeout time.Duration) bool {
	done := make(chan bool)
	go func() { done <- ck.servers[server].Call(svcMeth, args, reply) }()
	select {
	case <-time.After(timeout):
		log.V(1).Field("method", svcMeth).Field("server", server).
			Field("args", args).Infoln("do RPC timed out...")
		return false
	case ok := <-done:
		return ok
	}
}

func (ck *Clerk) doRPCRetry(server int, svcMeth string, args interface{},
	reply interface{}) (ok bool) {
	for i := 0; i < 1; i++ {
		ok = ck.doRPCWithTimeout(server, svcMeth, args, reply, time.Second)
		if ok {
			return
		}
	}
	return
}
