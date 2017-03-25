package raft

import (
	"fmt"
	"raft/util"
	"strconv"
	"time"

	"github.com/jhzhu89/log"
)

type replicator struct {
	leader     int // Sender id.
	follower   int // Receiver id.
	nextIndex  int
	matchIndex int
	raft       *Raft
	triggerCh  chan struct{}
}

func newReplicator(leader, follower int, raft *Raft) *replicator {
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
	toidx int, timeout time.Duration) (from, to int) {
	type res struct {
		success  bool
		from, to int
		err      error
	}
	done := make(chan res)
	do := func() {
		r.raft.raftLog.Lock()
		last := r.raft.lastIndex()
		r.raft.raftLog.Unlock()
		// TODO: add a from param.
		success, from, to, err := r.replicateTo(ctx, stepDownSig, last)
		done <- res{success, from, to, err}
	}

	retryOnError := 0
	for r.raft.state.AtomicGet() == Leader {
		goFunc(do)
		select {
		//case <-time.After(RPCTimeout):
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
				time.Sleep(ElectionTimeout / 3)
				continue
			}
			retryOnError = 0
			if res.success {
				if res.to > 0 { // > 0 means that we have replicated some entries.
					from, to = res.from, res.to
					r.nextIndex = res.to + 1
				}
				return
			} else {
				r.nextIndex /= 2
				// Retry imediately.
				continue
			}
		}
	}
	return
}

func (r *replicator) run(ctx util.CancelContext, stepDownSig util.Signal) {
	replicate := func(timeout time.Duration) (from, to int) {
		r.raft.raftLog.Lock()
		last := r.raft.lastIndex()
		r.raft.raftLog.Unlock()
		from, to = r.asyncReplicateTo(ctx, stepDownSig, last, timeout)
		return
	}

	// Heartbeat never retry.
	goFunc(func() { r.sendHeartbeat(ctx, stepDownSig) })

	// TODO: only check the Leder state, step down sig is not needed.
	for r.raft.state.AtomicGet() == Leader {
		select {
		case <-time.After(randomTimeout(ElectionTimeout / 2)):
			// At least try to replicate previous log entries once in a Term.
			// 1.replicate logs of previous terms. May also replicate logs in the current term
			//   because of the retry, so also need to try to commit logs.
			// 2. forward commit index.
			// Do not running too long, since it will delays the trigger.
			from, to := replicate(ElectionTimeout / 2)
			if from != 0 {
				r.commitRange(from, to)
			}
		case <-r.triggerCh:
			// Only commit logs in current term.
			from, to := replicate(RPCTimeout)
			if from != 0 {
				r.commitRange(from, to)
			}
		case <-ctx.Done():
			return
		case <-stepDownSig.Received():
			return
		}
	}
}

func (r *replicator) replicate() {
	// Async send. Do not block the run loop (cause blocking the leader loop).
	select {
	case r.triggerCh <- struct{}{}:
	default:
	}
}

func (r *replicator) replicateTo(ctx util.CancelContext, stepDownSig util.Signal,
	toidx int) (success bool, from, to int, err error) {
	var prevLogIndex, prevLogTerm int = 0, -1
	var req *AppendEntriesArgs
	var rep *AppendEntriesReply = new(AppendEntriesReply)

	type pair struct{ s, e int }
	var p pair

	rep.Term = -1
	r.raft.raftLog.Lock()
	prevLog := r.raft.getLogEntry(r.nextIndex - 1)
	r.raft.raftLog.Unlock()
	if prevLog != nil {
		prevLogIndex, prevLogTerm = prevLog.Index, prevLog.Term
	}
	// Prepare entries.
	es := r.prepareLogEntries(toidx)
	if len(es) != 0 {
		p.s, p.e = es[0].Index, toidx
		log.V(0).WithField(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
			WithField("from", p.s).WithField("to", p.e).WithField("last_entry", es[len(es)-1]).
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
		from, to = p.s, p.e
	}
	return
}

func (r *replicator) commitRange(from, to int) {
	log.V(0).WithField(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
		WithField("id", r.follower).WithField("from", from).WithField("to", to).
		//WithField("commiter_start", r.raft.committer.start).
		//WithField("commiter_end", r.raft.committer.end).
		Infoln("follower try to commit range...")
	e := r.raft.committer.tryToCommitRange(from, to)
	if e != nil {
		log.WithField(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
			WithField("id", r.follower).WithField("toidx", to).WithError(e).
			Errorln("error tryToCommit...")
	}
}

func (r *replicator) prepareLogEntries(toidx int) (es []*LogEntry) {
	start := r.nextIndex
	if start < 1 {
		start = 1
	}
	r.raft.raftLog.Lock()
	for i := start; i <= toidx; i++ {
		es = append(es, r.raft.getLogEntry(i))
	}
	r.raft.raftLog.Unlock()
	return
}
