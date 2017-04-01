package raftkv

import "labrpc"
import "crypto/rand"
import "fmt"
import "math/big"
import "sync"
import "sync/atomic"
import "time"

type Int32 int32

func (i *Int32) AtomicGet() Int32 {
	return Int32(atomic.LoadInt32((*int32)(i)))
}

func (i *Int32) AtomicSet(v Int32) {
	atomic.StoreInt32((*int32)(i), int32(v))
}

type Clerk struct {
	sync.Mutex
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int32
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
	time.Sleep(2 * time.Second)
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
	DPrintf("client: get %v", key)
	args := GetArgs{key}
	for i := 0; i < len(ck.servers); i++ {
		reply := GetReply{}
		ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
		if !ok {
			continue
		}
		if reply.WrongLeader {
			DPrintf("%v is wrong leader: %v, reply: %v", i, reply.Err, reply)
			continue
		}

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
	DPrintf("client: %v: %v, %v", op, key, value)
	args := PutAppendArgs{key, value, op}

	for i := 0; i < len(ck.servers); i++ {
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
		if !ok {
			continue
		}
		if reply.WrongLeader {
			//DPrintf("wrong leader: %v", reply.Err)
			continue
		}
		DPrintf("client finish: %v: %v, %v", op, key, value)
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
