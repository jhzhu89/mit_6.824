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
	var tryAnother bool
	for {
		log.V(2).F("key", key).F("uuid", args.Uuid).Infoln("client, Get...")
		ck.leaderMu.Lock()
		if tryAnother {
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
		leader := ck.leader
		ck.leaderMu.Unlock()
		reply := GetReply{}
		ok := ck.doRPC(leader, "RaftKV.Get", &args, &reply)
		if !ok || reply.WrongLeader {
			tryAnother = true
			continue
		}
		if reply.Pending || reply.Err != "" {
			if reply.Err != "" {
				log.Fs("key", key, "uuid", args.Uuid, "err", reply.Err).Errorln("request failed...")
			}
			tryAnother = false
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
	var tryAnother bool
	for {
		reply := PutAppendReply{}
		ck.leaderMu.Lock()
		if tryAnother {
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
		leader := ck.leader
		ck.leaderMu.Unlock()
		log.V(2).F("key", key).F("value", value).F("server", leader).
			F("op", op).F("uuid", args.Uuid).Infoln("client, PutAppend...")
		ok := ck.doRPC(leader, "RaftKV.PutAppend", &args, &reply)
		if !ok || reply.WrongLeader {
			tryAnother = true
			continue
		}
		if reply.Pending || reply.Err != "" {
			if reply.Err != "" {
				log.Fs("key", key, "uuid", args.Uuid, "err", reply.Err).Errorln("request failed...")
			}
			tryAnother = false
			continue
		}
		log.V(1).F("key", key).F("value", value).
			F("op", op).Infoln("client, finished PutAppend...")
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
		log.V(1).F("method", svcMeth).F("server", server).
			F("args", args).Infoln("do RPC timed out...")
		return false
	case ok := <-done:
		return ok
	}
}

func (ck *Clerk) doRPC(server int, svcMeth string, args interface{},
	reply interface{}) (ok bool) {
	return ck.doRPCWithTimeout(server, svcMeth, args, reply, 200*time.Millisecond)
}
