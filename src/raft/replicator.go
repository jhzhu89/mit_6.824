package raft

import (
	"fmt"
	"raft/util"
	"strconv"
	"sync"
	"time"

	"github.com/jhzhu89/go_util/hook"
	"github.com/jhzhu89/log"
)

// committer only commits logs in current term.
type committer struct {
	sync.Locker
	start, end int // [start, end) defines the window. start is read-only.
	toCommit   int // the most recent index to commit
	quoromSize int // Read-only once set.

	logs  map[int]*LogEntry // Logs to be committed.
	count map[*LogEntry]int

	committedLogs []*LogEntry

	committedCh chan struct{}
}

func newCommitter(committedCh chan struct{}, toCommit int) *committer {
	c := &committer{
		Locker:        new(sync.Mutex),
		start:         0,
		end:           0,
		toCommit:      toCommit,
		logs:          make(map[int]*LogEntry),
		count:         make(map[*LogEntry]int),
		committedLogs: make([]*LogEntry, 0),
		committedCh:   committedCh,
	}
	return c
}

func (c *committer) addLogs(es []*LogEntry) {
	if len(es) == 0 {
		return
	}

	c.Lock()
	if c.start <= 0 {
		c.start = es[0].Index
		c.toCommit = c.start
	}

	for _, e := range es {
		c.logs[e.Index] = e
		c.count[e] = 1
	}

	c.end = es[len(es)-1].Index + 1
	c.Unlock()
}

func (c *committer) tryToCommitOne(index int) (e error) {
	defer hook.T(false).TraceEnterLeave()()
	c.Lock()
	defer c.Unlock()
	if c.toCommit == 0 {
		e = fmt.Errorf("nothing to commit")
		return
	}

	if index < c.start {
		return
	}

	if index >= c.end {
		e = fmt.Errorf("index should never be greater than end")
		return
	}

	log, hit := c.logs[index]
	if !hit { // Already committed, this item has been deleted from map.
		return
	}
	c.count[log]++
	if c.count[log] >= c.quoromSize {
		if index == c.toCommit {
			c.toCommit++
			delete(c.logs, index)
			delete(c.count, log)
			c.committedLogs = append(c.committedLogs, log)
			select {
			case c.committedCh <- struct{}{}:
			default:
			}
		}
	}
	return
}

func (c *committer) getCommitIndex() int {
	c.Lock()
	defer c.Unlock()
	return c.toCommit - 1
}

func (c *committer) tryToCommitRange(s, e int) (err error) {
	for i := s; i <= e; i++ {
		err = c.tryToCommitOne(i)
		if err != nil {
			return
		}
	}
	return
}

func (c *committer) getCommitted() []*LogEntry {
	var res []*LogEntry
	c.Lock()
	res, c.committedLogs = c.committedLogs, make([]*LogEntry, 0)
	c.Unlock()
	return res
}

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

func (r *replicator) run(canceller util.Canceller, stepDownSig util.Signal) {
	do := func(needToCommit bool) {
		r.raft.raftLog.Lock()
		last := r.raft.lastIndex()
		r.raft.raftLog.Unlock()
		if needToCommit {
			from, to := r.replicateTo(canceller, stepDownSig, last)
			r.commitRange(from, to)
		} else {
			r.replicateTo(canceller, stepDownSig, last)
		}
	}

	for {
		select {
		case <-time.After(randomTimeout(ElectionTimeout / 10)):
			// 1. forward leader commit index;
			// 2. replicate logs of previous terms;
			// 3. act as heartbeat message.
			do(false)
		case <-r.triggerCh:
			// Only commit logs in current term.
			do(true)
		case <-canceller.Cancelled():
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

func (r *replicator) replicateTo(canceller util.Canceller, stepDownSig util.Signal, toidx int) (from, to int) {
	var prevLogIndex, prevLogTerm int = 0, -1
	var req *AppendEntriesArgs
	var rep *AppendEntriesReply = new(AppendEntriesReply)
	var withEntries = true

	type pair struct{ s, e int }
	var p pair

	for {
		select {
		case <-canceller.Cancelled():
			log.V(0).WithField(strconv.Itoa(r.raft.me), r.raft.raftState.AtomicGet()).
				WithField("to", r.follower).Infoln("cancelled to replicate...")
			return
		case <-stepDownSig.Received():
			log.V(1).WithField(strconv.Itoa(r.raft.me), r.raft.raftState.AtomicGet()).
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
			log.V(0).WithField(strconv.Itoa(r.raft.me), r.raft.raftState.AtomicGet()).
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
			log.WithField(strconv.Itoa(r.raft.me), r.raft.raftState.AtomicGet()).
				WithField("to", r.follower).WithField("req", req).Warningln("failed to sendAppendEntries...")
			return
		}

		// Check response.
		if rep.Term > int(r.raft.CurrentTerm.AtomicGet()) {
			stepDownSig.Send()
			log.V(0).WithField(strconv.Itoa(r.raft.me), r.raft.raftState.AtomicGet()).
				Infoln("step down signal sent...")
			return
		}

		if rep.Success {
			break
		}

		// Decrement nextIndex and retry.
		log.V(1).WithField(strconv.Itoa(r.raft.me), r.raft.raftState.AtomicGet()).
			Infoln("decrement nextIndex by 1 and retry...")
		r.nextIndex--
		if r.nextIndex == 0 {
			log.V(0).WithField(strconv.Itoa(r.raft.me), r.raft.raftState.AtomicGet()).
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
	log.V(0).WithField(strconv.Itoa(r.raft.me), r.raft.raftState.AtomicGet()).
		WithField("id", r.follower).WithField("from", from).WithField("to", to).
		Infoln("follower try to commit range...")
	e := r.raft.committer.tryToCommitRange(from, to)
	if e != nil {
		log.WithField(strconv.Itoa(r.raft.me), r.raft.raftState.AtomicGet()).
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
