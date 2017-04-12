package raftkv

import (
	"encoding/gob"
	"fmt"
	"kvraft/cache"
	"labrpc"
	"raft"
	"sync"
	"time"

	"github.com/jhzhu89/log"
	"github.com/satori/go.uuid"
)

const (
	pending uint8 = iota
	done
)

const ApplyOpTimeout = time.Minute

type ttlCache struct {
	*cache.Cache
}

type cacheItem struct {
	value  interface{}
	err    error // the request is executed but got an error when applying it.
	status uint8
	term   int
}

type result struct {
	value       string
	err         Err // the request is executed but got an error when applying it.
	pending     bool
	wrongLeader bool
}

func uuidStr(id uuid.UUID) string {
	return fmt.Sprintf("%s", id)
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
	applyFutures *applyFutureMap
	store        *kvstore
	ttlCache     *ttlCache
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	res := kv.handleRPC(Op{Key: args.Key, Code: GET, Uuid: args.Uuid})
	reply.WrongLeader = res.wrongLeader
	reply.Pending = res.pending
	reply.Value = res.value
	reply.Err = res.err
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	var code opCode
	if args.Op == "Put" {
		code = PUT
	} else {
		code = APPEND
	}
	res := kv.handleRPC(Op{Key: args.Key, Value: args.Value, Code: code, Uuid: args.Uuid})
	reply.WrongLeader = res.wrongLeader
	reply.Pending = res.pending
	reply.Err = res.err
}

func (kv *RaftKV) handleRPC(op Op) (res result) {
	curTerm, isLeader := kv.rf.GetState()
	if !isLeader {
		res.wrongLeader = true
		return
	}

	kv.ttlCache.Lock()
	if v, hit := kv.ttlCache.Get(uuidStr(op.Uuid)); hit {
		r := v.(cacheItem)
		if r.status == done {
			// the result is stored in ttlCache only when this request succeeded.
			if r.err != nil {
				res.err = Err(r.err.Error())
			}
			if op.Code == GET {
				res.value = r.value.(string)
			}
			log.V(1).Infoln("return from cache...")
			kv.ttlCache.Unlock()
			return
		}

		if r.term == curTerm && r.status == pending {
			res.pending = true
			log.V(1).Fs("server_id", kv.me, "cache_item", r, "op", op).
				Infoln("server, got this request from cache...")
			kv.ttlCache.Unlock()
			return
		}
		// remove the request in old term.
		kv.ttlCache.Delete(uuidStr(op.Uuid))
	}

	index, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		res.wrongLeader = true
		kv.ttlCache.Unlock()
		return
	}

	kv.ttlCache.SetDefault(uuidStr(op.Uuid), cacheItem{nil, nil, pending, curTerm})
	kv.ttlCache.Unlock()

	future, e := kv.applyFutures.add(index, op.Uuid)
	if e != nil {
		log.E(e).Warningln("error adding applyNotifier...")
		res.pending = true
		return
	}
	select {
	case <-time.After(ApplyOpTimeout):
		log.E(e).Warningf("timeout: failed to apply this command within %v", ApplyOpTimeout)
		res.err = Err("timeout to apply this op")
		return
	case <-future.Done():
	}
	e = future.Error()
	_, isLeader = kv.rf.GetState()
	if e != nil {
		res.wrongLeader = !isLeader
		res.err = Err(e.Error())
		return
	}
	res.wrongLeader = false
	if op.Code == GET {
		res.value = future.value.(string)
	}
	return
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

	kv.applyCh = make(chan raft.ApplyMsg, 1024)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.applyFutures = newApplyFutureMap()
	kv.store = newKvstore()
	kv.ttlCache = &ttlCache{Cache: cache.New(time.Minute, time.Minute)}
	go kv.processApplyMsg()
	return kv
}

func (kv *RaftKV) processApplyMsg() {
	for {
		select {
		case msg := <-kv.applyCh:
			var value interface{}
			var err error
			op := msg.Command.(Op)
			item, hit := kv.ttlCache.RSGet(uuidStr(op.Uuid))
			if hit && item.(cacheItem).status == done {
				value, err = item.(cacheItem).value, item.(cacheItem).err
			} else {
				// apply this op.
				switch op.Code {
				case PUT:
					kv.store.put(op.Key, op.Value)
				case APPEND:
					kv.store.append(op.Key, op.Value)
				case GET:
					value = kv.store.get(op.Key)
				default:
					err = fmt.Errorf("the operation is unknown")
				}
			}
			v := kv.applyFutures.get(msg.Index)
			// v != nil means that we were the leader
			if v != nil {
				if op.Uuid != v.uuid {
					v.Respond(nil, fmt.Errorf("uuid mismatch - sent: %v, received: %v", v.uuid, op.Uuid))
					log.V(3).Fs("server", kv.me, "op.uuid", op.Uuid, "v.uuid", v.uuid).Infoln("breaking a...")
				} else {
					v.Respond(value, err)
				}
			}

			log.V(3).F("uuid", op.Uuid).F("server", kv.me).
				F("value", value).F("error", err).Info("applied...")
			term, _ := kv.rf.GetState()
			kv.ttlCache.RSSetDefault(uuidStr(op.Uuid), cacheItem{value, err, done, term})
		}
	}
}

// ------------- applyNotifier -------------

type applyFuture struct {
	value  interface{}
	err    error
	doneCh chan struct{}
}

func newApplyFuture() *applyFuture {
	return &applyFuture{doneCh: make(chan struct{})}
}

func (f *applyFuture) Error() error {
	return f.err
}

func (f *applyFuture) Value() interface{} {
	return f.value
}

func (f *applyFuture) Done() <-chan struct{} {
	return f.doneCh
}

func (f *applyFuture) Respond(value interface{}, err error) {
	f.value = value
	f.err = err
	close(f.doneCh)
}

type uniqueApplyFuture struct {
	*applyFuture
	uuid uuid.UUID
}

type applyFutureMap struct {
	sync.RWMutex
	futures map[int]*uniqueApplyFuture
}

func newApplyFutureMap() *applyFutureMap {
	return &applyFutureMap{futures: make(map[int]*uniqueApplyFuture)}
}

func (n *applyFutureMap) add(index int, uuid uuid.UUID) (*applyFuture, error) {
	n.Lock()
	defer n.Unlock()
	log.V(1).F("log_index", index).Infoln("leader adds a notifier for a log entry...")
	if _, hit := n.futures[index]; hit {
		log.F("log_index", index).Warningf("conflict: %v has already been added to the router", index)
		return nil, fmt.Errorf("conflict: %v has already been added to the router", index)
	}
	n.futures[index] = &uniqueApplyFuture{newApplyFuture(), uuid}
	log.V(1).F("log_index", index).Infoln("notifier added...")
	return n.futures[index].applyFuture, nil
}

func (n *applyFutureMap) get(index int) *uniqueApplyFuture {
	n.RLock()
	defer n.RUnlock()
	rp := n.futures[index]
	delete(n.futures, index)
	return rp
}
