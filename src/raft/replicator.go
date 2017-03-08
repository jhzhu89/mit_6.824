package raft

import (
	"fmt"
	"github.com/jhzhu89/go_util/hook"
	"sync"
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
}

func newCommitter() *committer {
	c := &committer{
		Locker:        new(sync.Mutex),
		start:         -1,
		end:           -1,
		toCommit:      -1,
		logs:          make(map[int]*LogEntry),
		count:         make(map[*LogEntry]int),
		committedLogs: make([]*LogEntry, 0),
	}
	return c
}

func (c *committer) addLogs(es []*LogEntry) {
	if len(es) == 0 {
		return
	}

	c.Lock()
	if c.start < 0 {
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
	if c.toCommit == -1 {
		e = fmt.Errorf("nothing to commit")
		return
	}

	if index < c.start {
		return
	}

	c.Lock()
	if index >= c.end {
		e = fmt.Errorf("index should never be greater than end")
		return
	}

	log, hit := c.logs[index]
	if !hit { // Already committed, this item has been deleted from map.
		c.Unlock()
		return
	}
	c.count[log]++
	if c.count[log] >= c.quoromSize {
		if index == c.toCommit {
			c.toCommit++
			delete(c.logs, index)
			delete(c.count, log)
			c.committedLogs = append(c.committedLogs, log)
		}
	}
	c.Unlock()
	return
}

func (c *committer) tryToCommitRange(s, e int) (err error) {
	for i := s; i < e; i++ {
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
	*committer

	raft *Raft
}

func newReplicator(leader, follower int, raft *Raft) *replicator {
	return nil
}

func (r *replicator) replicateTo(toidx int) {
	var prevLogIndex, prevLogTerm int = -1, -1
	prevLog := r.raft.getLogEntry(r.nextIndex - 1)
	if prevLog != nil {
		prevLogIndex, prevLogTerm = prevLog.Index, prevLog.Term
	}
	// Prepare entries.
	es := r.prepareLogEntries(toidx)
	if len(es) == 0 {
		return // do nothing.
	}
	// Send RPC.
	req := &AppendEntriesArgs{
		Term:         r.raft.CurrentTerm,
		LeaderId:     r.raft.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: r.raft.commitIndex,
		Entires:      es,
	}
	reply := &AppendEntriesReply{}
	r.raft.sendAppendEntries(r.follower, req, reply)

	// Check response.
}

func (r *replicator) prepareLogEntries(toidx int) (es []*LogEntry) {
	for i := r.nextIndex; i <= toidx; i++ {
		es = append(es, r.raft.getLogEntry(i))
	}
	return
}