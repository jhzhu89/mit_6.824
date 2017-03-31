package raft

import (
	"fmt"
	"raft/util"
	"strconv"
	"sync"
	"time"

	"github.com/jhzhu89/log"
)

type replicator struct {
	leader   int // Sender id.
	follower int // Receiver id.

	nextIndexMu sync.RWMutex
	nextIndex   int

	tryCommitToMu sync.Mutex
	tryCommitTo   int

	matchIndex int // TODO: check matchIndex
	raft       *Raft
	triggerCh  chan struct{}
}

type rangeT struct{ from, to int }

func newReplicator(rg *util.RoutineGroup, stepDownSig util.Signal, raft *Raft,
	leader, follower int) *replicator {
	r := &replicator{
		leader:     leader,
		follower:   follower,
		nextIndex:  raft.lastIndex(),
		matchIndex: 0,
		raft:       raft,
		triggerCh:  make(chan struct{}, 1),
	}
	if r.nextIndex <= 0 {
		r.nextIndex = 1 // Valid nextIndex starts from 1.
	}
	rg.GoFunc(func(ctx util.CancelContext) { r.run(rg, ctx, stepDownSig) })
	return r
}

// TODO: split retry logic from timeout logic.
func (r *replicator) retryReplicateTo(ctx util.CancelContext, stepDownSig util.Signal,
	timeout time.Duration) (crange rangeT) {
	retryOnError := 0

	for retryOnError < 3 && r.raft.state.AtomicGet() == Leader {
		rrange := rangeT{}
		r.nextIndexMu.RLock()
		rrange.from = r.nextIndex
		r.nextIndexMu.RUnlock()
		r.raft.persistentState.RLock()
		rrange.to = r.raft.lastIndex()
		r.raft.persistentState.RUnlock()
		success, _crange, err := r.replicateTo(ctx, stepDownSig, rrange)
		if err != nil {
			retryOnError++
			if retryOnError >= 3 {
				return
			}
			// Retry.
			time.Sleep(HeartbeatTimeout / 3)
			continue
		}
		retryOnError = 0
		if success {
			if _crange.to > 0 { // > 0 means that we have replicated some entries.
				crange = _crange
				r.nextIndexMu.Lock()
				if r.nextIndex < crange.to+1 {
					r.nextIndex = crange.to + 1
				}
				r.nextIndexMu.Unlock()
			}
			return
		} else {
			r.nextIndexMu.Lock()
			if r.nextIndex < rrange.from { // Others already decremented nextIndex and retry.
				r.nextIndexMu.Unlock()
				return
			}
			r.nextIndex /= 2
			// Retry imediately.
			r.nextIndexMu.Unlock()
			continue
		}
	}
	return
}

func (r *replicator) periodicReplicate(ctx util.CancelContext, stepDownSig util.Signal) {
	for r.raft.state.AtomicGet() == Leader {
		select {
		// At least try to replicate previous log entries once in a Term.
		// 1. replicate logs of previous terms. May also replicate logs in the current term
		//    because of the retry, so also need to try to commit logs.
		// 2. forward commit index.
		// 3. act as heartbeat.
		// make sure at least one heartbeat is send during the ElectionTimeout. So
		// sendAppendEntries should return within ElectionTimeout (the RPCTimeout equals
		// ElectionTimeout).
		case <-time.After(randomTimeout(CommitTimeout)):
			// set a small timeout, so that learder commit can be forwarded quickly.
			//crange := r.replicateToWithTimeout(ctx, stepDownSig, randomTimeout(CommitTimeout))
			crange := r.retryReplicateTo(ctx, stepDownSig, RPCTimeout)
			if crange.from != 0 {
				r.asyncTryCommitRange(crange)
			}
		case <-ctx.Done():
			return
		case <-stepDownSig.Received():
			return
		}
	}
}

// Respond to trigger.
func (r *replicator) immediateReplicate(ctx util.CancelContext, stepDownSig util.Signal) {
	for r.raft.state.AtomicGet() == Leader {
		select {
		case <-r.triggerCh:
			// Only commit logs in current term.
			crange := r.retryReplicateTo(ctx, stepDownSig, RPCTimeout)
			if crange.from != 0 {
				r.asyncTryCommitRange(crange)
			}
		case <-ctx.Done():
			return
		case <-stepDownSig.Received():
			return
		}
	}
}

func (r *replicator) run(rg *util.RoutineGroup, ctx util.CancelContext,
	stepDownSig util.Signal) {
	rg.GoFunc(func(ctx util.CancelContext) { r.immediateReplicate(ctx, stepDownSig) })
	rg.GoFunc(func(ctx util.CancelContext) { r.periodicReplicate(ctx, stepDownSig) })
}

func (r *replicator) replicate() {
	// Async send. Do not block the run loop (cause blocking the leader loop).
	select {
	case r.triggerCh <- struct{}{}:
	default:
	}
}

func (r *replicator) replicateTo(ctx util.CancelContext, stepDownSig util.Signal,
	rrange rangeT) (success bool, crange rangeT, err error) {
	var prevLogIndex, prevLogTerm int = 0, -1
	var req *AppendEntriesArgs
	var rep *AppendEntriesReply = new(AppendEntriesReply)

	rep.Term = -1
	r.raft.persistentState.RLock()
	prevLog := r.raft.getLogEntry(rrange.from - 1)
	r.raft.persistentState.RUnlock()
	if prevLog != nil {
		prevLogIndex, prevLogTerm = prevLog.Index, prevLog.Term
	}
	// Prepare entries.
	es := r.prepareLogEntries(rrange)
	if len(es) != 0 {
		crange.from, crange.to = es[0].Index, rrange.to
		log.V(0).Field(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
			Field("from", crange.from).Field("to", crange.to).Field("last_entry", es[len(es)-1]).
			Infof("replicate to %v...", r.follower)
	}

	// Send RPC.
	req = &AppendEntriesArgs{
		Term:         int(r.raft.currentTerm.AtomicGet()),
		LeaderId:     r.raft.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: int(r.raft.commitIndex.AtomicGet()),
		Entires:      es,
	}
	ok := r.raft.sendAppendEntries(r.follower, req, rep)
	if !ok {
		log.Field(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
			Field("follower", r.follower).Field("req", req).Warningln("failed to sendAppendEntries (maybe I am not leader anymore)...")
		err = fmt.Errorf("RPC failed")
		return
	}

	// Check response.
	if rep.Term > int(r.raft.currentTerm.AtomicGet()) {
		stepDownSig.Send()
		log.V(0).Field(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
			Infoln("step down signal sent...")
		return
	}

	if rep.Success {
		success = true
	} else {
		crange.from, crange.to = 0, 0
	}
	return
}

func (r *replicator) asyncTryCommitRange(crange rangeT) {
	r.tryCommitToMu.Lock()
	defer r.tryCommitToMu.Unlock()

	if r.tryCommitTo == 0 {
		r.tryCommitTo = crange.from
	}
	if crange.to < r.tryCommitTo {
		return
	}
	if crange.from < r.tryCommitTo {
		crange.from = r.tryCommitTo
	}

	log.V(0).Field(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
		Field("id", r.follower).Field("from", crange.from).Field("to", crange.to).
		Infoln("follower try to commit range...")
	e := r.raft.committer.tryCommitRange(crange.from, crange.to)
	if e != nil {
		log.Field(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
			Field("id", r.follower).Field("from", crange.from).Field("to", crange.to).
			Err(e).Infoln("replicated logs in previous term, committer rejected them...")
	} else {
		r.tryCommitTo = crange.to + 1
	}
}

func (r *replicator) prepareLogEntries(prange rangeT) (es []*LogEntry) {
	if prange.from < 1 {
		prange.from = 1
	}
	r.raft.persistentState.Lock()
	for i := prange.from; i <= prange.to; i++ {
		es = append(es, r.raft.getLogEntry(i))
	}
	r.raft.persistentState.Unlock()
	return
}
