package raft

import (
	"fmt"
	"raft/util"
	"sync"

	"github.com/jhzhu89/go_util/hook"
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

func newCommitter(committedCh chan struct{}) *committer {
	c := &committer{
		Locker:        new(sync.Mutex),
		start:         0,
		end:           0,
		toCommit:      0,
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
	nextIndex  Int32
	matchIndex int
	raft       *Raft
	triggerCh  chan int
}

func newReplicator(leader, follower int, raft *Raft) *replicator {
	r := &replicator{
		leader:     leader,
		follower:   follower,
		nextIndex:  Int32(raft.lastIndex()),
		matchIndex: 0,
		raft:       raft,
		triggerCh:  make(chan int),
	}
	if r.nextIndex.AtomicGet() <= 0 {
		r.nextIndex.AtomicSet(1) // Valid nextIndex starts from 1.
	}
	return r
}

func (r *replicator) run(canceller util.Canceller, stepDownSig util.Signal) {
	for toidx := range r.triggerCh {
		r.do(canceller, stepDownSig, toidx)
		select {
		case <-canceller.Cancelled():
			return
		default:
		}
	}
}

func (r *replicator) replicateTo(toidx int) {
	r.triggerCh <- toidx
}

func (r *replicator) do(canceller util.Canceller, stepDownSig util.Signal, toidx int) {
	var prevLogIndex, prevLogTerm int = 0, -1
	var req *AppendEntriesArgs
	var rep *AppendEntriesReply = new(AppendEntriesReply)

	type pair struct{ s, e int }
	var p pair

	for {
		select {
		case <-canceller.Cancelled():
			DPrintf("[%v - %v] - cancelled to replicate to %v...\n", r.raft.me,
				r.raft.raftState.AtomicGet(), r.follower)
			return
		default:
		}

		rep.Term = -1
		r.raft.raftLog.Lock()
		prevLog := r.raft.getLogEntry(int(r.nextIndex.AtomicGet() - 1))
		r.raft.raftLog.Unlock()
		if prevLog != nil {
			prevLogIndex, prevLogTerm = prevLog.Index, prevLog.Term
		}
		// Prepare entries.
		es := r.prepareLogEntries(toidx)
		if len(es) == 0 {
			return // do nothing.
		}
		//DPrintf("es: %v\n", es)
		p.s, p.e = es[0].Index, es[len(es)-1].Index
		// Send RPC.
		req = &AppendEntriesArgs{
			Term:         int(r.raft.CurrentTerm.AtomicGet()),
			LeaderId:     r.raft.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: int(r.raft.commitIndex.AtomicGet()),
			Entires:      es,
		}
		r.raft.sendAppendEntries(r.follower, req, rep)

		// Check response.
		if rep.Term > int(r.raft.CurrentTerm.AtomicGet()) {
			stepDownSig.Send()
			DPrintf("[%v - %v] - step down signal sent...\n", r.raft.me, r.raft.raftState.AtomicGet())
			return
		}

		if rep.Success {
			break
		}

		// Decrement nextIndex and retry.
		DPrintf("[%v - %v] - decrement nextIndex by 1 and retry...\n", r.raft.me, r.raft.raftState.AtomicGet())
		r.nextIndex.AtomicAdd(-1)
	}

	DPrintf("[%v - %v] - follower %v try to commit range: %v, %v\n", r.raft.me, r.raft.raftState.AtomicGet(),
		r.follower, p.s, p.e)
	e := r.raft.committer.tryToCommitRange(p.s, p.e)
	r.nextIndex.AtomicSet(int32(toidx + 1))
	if e != nil {
		DPrintf("[%v - %v] - error tryToCommit to %v to %v...\n", r.raft.me, r.raft.raftState.AtomicGet(),
			r.follower, toidx)
		return
	}
}

func (r *replicator) prepareLogEntries(toidx int) (es []*LogEntry) {
	start := int(r.nextIndex.AtomicGet())
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
