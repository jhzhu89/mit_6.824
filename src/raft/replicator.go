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

	legitimateTerm int // the term in which I will replicate logs

	indexMu    sync.RWMutex // protect next, match index
	nextIndex  int
	matchIndex int

	tryCommitToMu sync.Mutex
	tryCommitTo   int

	raft      *Raft
	committer *committer // a local copy, raft may create new committer after transiting state.
	triggerCh chan struct{}
	stepDown  util.Signal
}

type rangeT struct{ from, to int }

func newReplicator(rg *util.RoutineGroup, stepDown util.Signal, raft *Raft,
	leader, follower int) *replicator {
	raft.persistentState.RLock()
	lastIndex := raft.lastIndex()
	raft.persistentState.RUnlock()
	r := &replicator{
		leader:         leader,
		follower:       follower,
		legitimateTerm: int(raft.currentTerm.AtomicGet()),
		nextIndex:      lastIndex,
		matchIndex:     0,
		raft:           raft,
		committer:      raft.committer,
		triggerCh:      make(chan struct{}, 1),
		stepDown:       stepDown,
	}
	if r.nextIndex <= 0 {
		r.nextIndex = 1 // Valid nextIndex starts from 1.
	}
	rg.GoFunc(func(ctx util.Context) { r.run(rg, ctx) })
	return r
}

func (r *replicator) periodicReplicate(ctx util.Context) {
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
			go r.replicate(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// Respond to trigger.
func (r *replicator) immediateReplicate(ctx util.Context) {
	for r.raft.state.AtomicGet() == Leader {
		select {
		case <-r.triggerCh:
			// Only commit logs in current term.
			go r.replicate(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (r *replicator) run(rg *util.RoutineGroup, _ util.Context) {
	rg.GoFunc(r.immediateReplicate)
	rg.GoFunc(r.periodicReplicate)
}

func (r *replicator) Replicate() {
	// Async send. Do not block the run loop (cause blocking the leader loop).
	select {
	case r.triggerCh <- struct{}{}:
	default:
	}
}

// replicate logs from nextIndex to log.lastIndex
func (r *replicator) replicate(ctx util.Context) {
	retryCount := 0
	for retryCount < 3 && r.raft.state.AtomicGet() == Leader {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var prevLogIndex, prevLogTerm int = 0, -1
		var prevLog *LogEntry
		var req *AppendEntriesArgs
		var rep *AppendEntriesReply = new(AppendEntriesReply)
		var withLogs = false

		rep.Term = -1
		r.indexMu.RLock()
		nextIndex := r.nextIndex
		r.indexMu.RUnlock()
		r.raft.persistentState.Lock()
		prevLog = r.raft.getLogEntry(nextIndex - 1)
		if prevLog != nil {
			prevLogIndex, prevLogTerm = prevLog.Index, prevLog.Term
		}
		// Prepare entries.
		rrange := rangeT{nextIndex, r.raft.raftLog.lastIndex()}
		es := r.prepareLogEntries(rrange)
		lastEntry := r.raft.getLogEntry(r.raft.raftLog.lastIndex())
		r.raft.persistentState.Unlock()
		if len(es) != 0 {
			withLogs = true
			log.V(1).F(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
				F("from", rrange.from).F("to", rrange.to).F("last_entry", lastEntry).
				Infof("replicate to %v...", r.follower)
		}

		// Send RPC.
		req = &AppendEntriesArgs{
			Term:         r.legitimateTerm,
			LeaderId:     r.raft.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: int(r.raft.commitIndex.AtomicGet()),
			Entires:      es,
		}

		log.V(2).F(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
			F("follower", r.follower).F("req", req).Infoln("sending sendAppendEntries...")
		ok := r.raft.sendAppendEntries(r.follower, req, rep)
		if !ok {
			log.V(2).F(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
				F("follower", r.follower).Infoln("failed to sendAppendEntries (maybe I am not leader anymore)...")
			retryCount++
			continue
		}

		retryCount = 0
		// Check response.
		if rep.Term > r.legitimateTerm {
			r.raft.state.AtomicSet(Follower)
			log.V(1).F(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).F("larger_term", rep.Term).
				Infoln("larger term seen, step down, change to follower...")
			r.raft.currentTerm.AtomicSet(int32(rep.Term))
			r.stepDown.Send()
			return
		}

		if r.legitimateTerm < int(r.raft.currentTerm.AtomicGet()) {
			log.V(1).F(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
				Fs("send_term", r.legitimateTerm, "cur_term", r.raft.currentTerm.AtomicGet()).
				Infoln("my term changed, drop this reply...")
			return
		}

		if rep.Success {
			if withLogs {
				r.indexMu.Lock()
				if rrange.to > r.matchIndex {
					r.matchIndex = rrange.to
					r.nextIndex = rrange.to + 1
				} else {
					log.V(2).F(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
						Fs("matchindex", r.matchIndex, "nextindex", r.nextIndex, "rrange", rrange).
						Infoln("rrange is lower than matchindex, do not update the matchindex...")
				}
				r.indexMu.Unlock()
				r.tryCommitRange(rrange)
			}
			return
		} else {
			// decrement the nextIndex
			r.indexMu.Lock()
			if r.nextIndex >= rrange.from {
				// decrement nextIndex and resend
				r.nextIndex /= 2
				if r.nextIndex <= r.matchIndex {
					r.nextIndex = r.matchIndex + 1
				}
				log.V(2).F(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
					Fs("matchindex", r.matchIndex, "nextindex", r.nextIndex, "rrange", rrange).
					Infoln("after decrementing nextindex...")
				r.indexMu.Unlock()
			} else { // else {} // Others already decremented nextIndex and retry.
				r.indexMu.Unlock()
				return
			}
		}
	}
	return
}

// tryCommitRange should be routine safe.
func (r *replicator) tryCommitRange(crange rangeT) {
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

	log.V(1).F(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
		F("id", r.follower).F("from", crange.from).F("to", crange.to).
		Infoln("follower try to commit range...")
	e := r.committer.tryCommitRange(crange.from, crange.to)
	if e != nil {
		log.V(1).F(strconv.Itoa(r.raft.me), r.raft.state.AtomicGet()).
			F("id", r.follower).F("from", crange.from).F("to", crange.to).
			F("err", e.Error()).Infoln("replicated logs in previous term, committer rejected them...")
	} else {
		r.tryCommitTo = crange.to + 1
	}
}

func (r *replicator) prepareLogEntries(prange rangeT) (es []*LogEntry) {
	if prange.from < 1 {
		prange.from = 1
	}
	for i := prange.from; i <= prange.to; i++ {
		e := r.raft.getLogEntry(i)
		if e == nil {
			panic(fmt.Sprintf("got a nil entry for index %v, prange: %v", i, prange))
		}
		es = append(es, r.raft.getLogEntry(i))
	}
	return
}
