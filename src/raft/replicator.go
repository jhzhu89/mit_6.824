package raft

import (
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

func (r *replicator) run(ctx util.CancelContext, stepDownSig util.Signal) {
	replicate := func() (from, to int) {
		r.raft.raftLog.Lock()
		last := r.raft.lastIndex()
		r.raft.raftLog.Unlock()
		from, to = r.replicateTo(ctx, stepDownSig, last)
		return
	}

	for {
		select {
		case <-time.After(randomTimeout(ElectionTimeout / 10)):
			// 1. forward leader commit index (replicate logs of previous terms at same time);
			// 2. act as heartbeat message.
			replicate()
		case <-r.triggerCh:
			// Only commit logs in current term.
			from, to := replicate()
			r.commitRange(from, to)
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

func (r *replicator) replicateTo(ctx util.CancelContext, stepDownSig util.Signal, toidx int) (from, to int) {
	var prevLogIndex, prevLogTerm int = 0, -1
	var req *AppendEntriesArgs
	var rep *AppendEntriesReply = new(AppendEntriesReply)
	var withEntries = true

	type pair struct{ s, e int }
	var p pair

	for {
		select {
		case <-ctx.Done():
			log.V(0).WithField(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
				WithField("to", r.follower).Infoln("cancelled to replicate...")
			return
		case <-stepDownSig.Received():
			log.V(1).WithField(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
				Infoln("received stepdown signal...")
			return
		default:
		}

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
				WithField("from", p.s).WithField("to", p.e).Infof("replicate to %v...", r.follower)
		} else {
			withEntries = false
		}

		// Send RPC.
		req = &AppendEntriesArgs{
			Term:         int(r.raft.CurrentTerm.AtomicGet()),
			LeaderId:     r.raft.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: int(r.raft.commitIndex.AtomicGet()),
			Entires:      es,
		}
		ok := r.raft.sendAppendEntries(r.follower, req, rep)
		if !ok {
			log.WithField(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
				WithField("to", r.follower).WithField("req", req).Warningln("failed to sendAppendEntries...")
			return
		}

		// Check response.
		if rep.Term > int(r.raft.CurrentTerm.AtomicGet()) {
			stepDownSig.Send()
			log.V(0).WithField(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
				Infoln("step down signal sent...")
			return
		}

		if rep.Success {
			break
		}

		// Decrement nextIndex and retry.
		log.V(1).WithField(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
			Infoln("decrement nextIndex by 1 and retry...")
		r.nextIndex--
		if r.nextIndex == 0 {
			log.V(0).WithField(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
				WithField("to", r.follower).Infoln("failed to replicate logs...")
			return
		}
	}

	if withEntries { // The RPC has replicates some entries to follower.
		r.nextIndex = p.e + 1
		from, to = p.s, p.e
	}
	return
}

func (r *replicator) commitRange(from, to int) {
	log.V(0).WithField(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
		WithField("id", r.follower).WithField("from", from).WithField("to", to).
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
