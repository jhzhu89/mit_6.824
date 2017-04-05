package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	//"log"
	"raft"
	"sync"
	"time"

	"github.com/jhzhu89/log"
	"github.com/patrickmn/go-cache"
	"github.com/satori/go.uuid"
)

const Debug = 1

//func DPrintf(format string, a ...interface{}) (n int, err error) {
//	if Debug > 0 {
//		log.Printf(format, a...)
//	}
//	return
//}

var ttlCache *cache.Cache

func init() {
	ttlCache = cache.New(time.Minute, time.Minute)
}

type opCode uint8

var codes = [...]string{
	"GET",
	"PUT",
	"APPEND",
}

func (c opCode) String() string {
	return codes[c]
}

const (
	GET opCode = iota
	PUT
	APPEND
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Code  opCode
	Uuid  uuid.UUID
}

func (o Op) String() string {
	return fmt.Sprintf("{key: %v, value: %v, code: %v, uuid: %v}",
		o.Key, o.Value, o.Code, o.Uuid)
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	applyNotifier *applyNotifier

	store *kvstore
}

const (
	pendding uint8 = iota
	done
)

type cacheItem struct {
	*GetReply
	*PutAppendReply
	status uint8
	doneCh chan struct{}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	if v, hit := ttlCache.Get(fmt.Sprintf("%s", args.Uuid)); hit {
		r := v.(cacheItem)
		if r.status == pendding {
			reply.Err = Err("Get operation pending")
			return
		}
		reply.Err = r.GetReply.Err
		reply.Value = r.GetReply.Value
		reply.WrongLeader = r.GetReply.WrongLeader
		return
	}

	reply.Err = ""
	reply.Value = ""
	reply.WrongLeader = false
	// Your code here.
	op := Op{
		Key:  args.Key,
		Code: GET,
		Uuid: args.Uuid,
	}

	index, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	ttlCache.SetDefault(fmt.Sprintf("%s", args.Uuid), cacheItem{nil, nil, pendding})

	future, e := kv.applyNotifier.add(index, op.Uuid)
	if e != nil {
		reply.Err = Err(e.Error())
		return
	}
	e = future.Error()
	_, isLeader = kv.rf.GetState()
	if e != nil {
		reply.WrongLeader = !isLeader
		reply.Err = Err(e.Error())
		return
	} else {
		reply.WrongLeader = false
		reply.Value = future.value.(string)
		reply.Err = ""
		ttlCache.SetDefault(fmt.Sprintf("%s", args.Uuid),
			cacheItem{&GetReply{reply.WrongLeader, reply.Err, reply.Value}, nil, done})
		return
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if v, hit := ttlCache.Get(fmt.Sprintf("%s", args.Uuid)); hit {
		r := v.(cacheItem)
		if r.status == pendding {
			reply.Err = Err("PutAppend operation pendding")
			return
		}
		reply.Err = r.PutAppendReply.Err
		reply.WrongLeader = r.PutAppendReply.WrongLeader
		return
	}

	var code opCode
	if args.Op == "Put" {
		code = PUT
	} else {
		code = APPEND
	}
	op := Op{
		Key:   args.Key,
		Value: args.Value,
		Code:  code,
		Uuid:  args.Uuid,
	}

	index, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	ttlCache.SetDefault(fmt.Sprintf("%s", args.Uuid), cacheItem{nil, nil, pendding})

	future, e := kv.applyNotifier.add(index, op.Uuid)
	if e != nil {
		reply.Err = Err(e.Error())
		return
	}
	e = future.Error()
	_, isLeader = kv.rf.GetState()
	if e != nil {
		reply.WrongLeader = !isLeader
		reply.Err = Err(e.Error())
		return
	} else {
		reply.Err = ""
		ttlCache.SetDefault(fmt.Sprintf("%s", args.Uuid),
			cacheItem{nil, &PutAppendReply{reply.WrongLeader, reply.Err}, done})
		return
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.applyNotifier = newApplyNotifier()
	kv.store = newKvstore()
	go kv.processApplyMsg()
	return kv
}

func (kv *RaftKV) processApplyMsg() {
	for {
		select {
		case msg := <-kv.applyCh:
			//DPrintf("received an msg: %v", msg)
			// 1. verify if the command is the one we Start()ed.
			v := kv.applyNotifier.get(msg.Index)
			op := msg.Command.(Op)
			//DPrintf("op: %v", op)
			// v != nil means that we were the leader
			if v != nil && op.Uuid != v.uuid {
				v.future.Respond(nil, fmt.Errorf("uuid mismatch - sent: %v, received: %v",
					v.uuid, op.Uuid))
				return
			}
			var value interface{}
			var err error
			switch op.Code {
			case PUT:
				kv.store.put(op.Key, op.Value)
				//DPrintf("value put: %v...", op.Value)
			case APPEND:
				kv.store.append(op.Key, op.Value)
				//DPrintf("value appended: %v...", op.Value)
			case GET:
				value = kv.store.get(op.Key)
				//DPrintf("value got: %v...", value)
			default:
				err = fmt.Errorf("the operation is unknown")
			}

			if v != nil {
				v.future.Respond(value, err)
			}
		}
	}
}

// ------------- applyNotifier -------------

type applyFuture struct {
	errCh chan error
	value interface{}
}

func newApplyFuture() *applyFuture {
	return &applyFuture{errCh: make(chan error, 1)}
}

func (f *applyFuture) Error() error {
	return <-f.errCh
}

func (f *applyFuture) Value() interface{} {
	return f.value
}

func (f *applyFuture) Respond(value interface{}, err error) {
	f.value = value
	f.errCh <- err
}

type routerPair struct {
	future *applyFuture
	uuid   uuid.UUID
}

type applyNotifier struct {
	sync.RWMutex
	router map[int]*routerPair
}

func newApplyNotifier() *applyNotifier {
	return &applyNotifier{router: make(map[int]*routerPair)}
}

func (n *applyNotifier) add(index int, uuid uuid.UUID) (*applyFuture, error) {
	n.Lock()
	defer n.Unlock()
	log.V(1).Field("log_index", index).Infoln("leader adds a notifier for a log entry...")
	if _, hit := n.router[index]; hit {
		return nil, fmt.Errorf("conflict: %v has already been added to the router", index)
	}
	n.router[index] = &routerPair{newApplyFuture(), uuid}
	log.V(2).Field("log_index", index).Infoln("notifier added...")
	return n.router[index].future, nil
}

func (n *applyNotifier) get(index int) *routerPair {
	n.RLock()
	defer n.RUnlock()
	return n.router[index]
}
