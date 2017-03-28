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

	nextIndexMu sync.Mutex
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

func (r *replicator) sendHeartbeat(ctx util.CancelContext, stepDownSig util.Signal) {
	for r.raft.state.AtomicGet() == Leader {
		select {
		case <-time.After(randomTimeout(HeartbeatTimeout)):
			go func() {
				reply := &AppendEntriesReply{}
				if r.raft.sendAppendEntries(r.follower,
					&AppendEntriesArgs{
						Term:     int(r.raft.currentTerm.AtomicGet()),
						LeaderId: r.raft.me},
					reply) {
					if reply.Term > int(r.raft.currentTerm.AtomicGet()) {
						stepDownSig.Send()
					}
				}
			}()

		case <-ctx.Done():
			return
		case <-stepDownSig.Received():
			return
		}
	}
}

func (r *replicator) asyncReplicateTo(ctx util.CancelContext, stepDownSig util.Signal,
	timeout time.Duration) (crange rangeT) {
	// rrange: replicate range, crange: try to commit range
	type res struct {
		success bool
		rangeT
		err error
	}
	done := make(chan res)
	retryOnError := 0

	for r.raft.state.AtomicGet() == Leader {
		rrange := rangeT{}
		r.nextIndexMu.Lock()
		rrange.from = r.nextIndex
		r.nextIndexMu.Unlock()
		r.raft.persistentState.RLock()
		rrange.to = r.raft.lastIndex()
		r.raft.persistentState.RUnlock()
		goFunc(func() {
			rrange := rrange
			success, crange, err := r.replicateTo(ctx, stepDownSig, rrange)
			done <- res{success, crange, err}
		})

		select {
		case <-time.After(timeout):
			// Timed out.
			return
		case <-ctx.Done():
			return
		case <-stepDownSig.Received():
			return
		case res := <-done:
			if res.err != nil {
				retryOnError++
				if retryOnError >= 3 {
					return
				}
				// Retry.
				time.Sleep(timeout / 3)
				continue
			}
			retryOnError = 0
			if res.success {
				if res.rangeT.to > 0 { // > 0 means that we have replicated some entries.
					crange = res.rangeT
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
	}
	return
}

func (r *replicator) timeoutReplicate(ctx util.CancelContext, stepDownSig util.Signal) {
	for r.raft.state.AtomicGet() == Leader {
		select {
		// At least try to replicate previous log entries once in a Term.
		// 1.replicate logs of previous terms. May also replicate logs in the current term
		//   because of the retry, so also need to try to commit logs.
		// 2. forward commit index.
		case <-time.After(randomTimeout(CommitTimeout)):
			crange := r.asyncReplicateTo(ctx, stepDownSig, CommitTimeout)
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
func (r *replicator) respond(rg *util.RoutineGroup, ctx util.CancelContext,
	stepDownSig util.Signal) {
	do := func(ctx util.CancelContext) {
		for r.raft.state.AtomicGet() == Leader {
			select {
			case <-r.triggerCh:
				// Only commit logs in current term.
				crange := r.asyncReplicateTo(ctx, stepDownSig, RPCTimeout)
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

	// Spawn 2 routines to replicate.
	for i := 0; i < 2; i++ {
		rg.GoFunc(do)
	}
}

func (r *replicator) run(rg *util.RoutineGroup, ctx util.CancelContext,
	stepDownSig util.Signal) {
	//rg.GoFunc(func(ctx util.CancelContext) { r.sendHeartbeat(ctx, stepDownSig) })
	rg.GoFunc(func(ctx util.CancelContext) { r.respond(rg, ctx, stepDownSig) })
	rg.GoFunc(func(ctx util.CancelContext) { r.timeoutReplicate(ctx, stepDownSig) })
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
	r.nextIndexMu.Lock()
	nextIndex := r.nextIndex
	r.nextIndexMu.Unlock()
	prevLog := r.raft.getLogEntry(nextIndex - 1)
	r.raft.persistentState.RUnlock()
	if prevLog != nil {
		prevLogIndex, prevLogTerm = prevLog.Index, prevLog.Term
	}
	// Prepare entries.
	es := r.prepareLogEntries(rrange)
	if len(es) != 0 {
		crange.from, crange.to = es[0].Index, rrange.to
		log.V(0).WithField(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
			WithField("from", crange.from).WithField("to", crange.to).WithField("last_entry", es[len(es)-1]).
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
		log.WithField(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
			WithField("follower", r.follower).WithField("req", req).Warningln("failed to sendAppendEntries (maybe I am not leader anymore)...")
		err = fmt.Errorf("RPC failed")
		return
	}

	// Check response.
	if rep.Term > int(r.raft.currentTerm.AtomicGet()) {
		stepDownSig.Send()
		log.V(0).WithField(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
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

	log.V(0).WithField(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
		WithField("id", r.follower).WithField("from", crange.from).WithField("to", crange.to).
		Infoln("follower try to commit range...")
	e := r.raft.committer.tryCommitRange(crange.from, crange.to)
	if e != nil {
		log.WithField(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
			WithField("id", r.follower).WithField("to", crange.to).WithError(e).
			Errorln("error try to commit range...")
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
