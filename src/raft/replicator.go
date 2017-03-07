package raft

import (
	"sync"
)

// committer only commits logs in current term.
type committer struct {
	sync.Locker
	start, end    int // start, end defines the window. start is read-only.
	lastCommitted int
	quoromSize    int // Read-only once set.

	logs   map[int]*LogEntry // Logs to be committed.
	counts map[*LogEntry]int

	committedLogs []*LogEntry
}

func (c *committer) tryToCommitOne(index int) {
	if index < c.start {
		return
	}

	c.Lock()
	if index >= c.end {
		panic("index should never be greater than end")
	}

	log, hit := c.logs[index]
	if !hit { // Already committed, this item has been deleted from map.
		c.Unlock()
		return
	}
	c.counts[log]++
	if c.counts[log] > c.quoromSize && index == c.lastCommitted+1 {
		c.lastCommitted++
		delete(c.logs, index)
		delete(c.counts, log)
		c.committedLogs = append(c.committedLogs, log)
	}
	c.Unlock()
}

func (c *committer) tryToCommitRange(s, e int) {
	for i := s; i < e; i++ {
		c.tryToCommitOne(i)
	}
}

func (c *committer) getCommitted() []*LogEntry {
	var res []*LogEntry
	c.Lock()
	res, c.committedLogs = c.committedLogs, make([]*LogEntry, 0)
	c.Unlock()
	return res
}

type replicator struct {
	leader, follower int // Sender and receiver id.
	*committer
}

func (r *replicator) replicateTo(toidx int) {

}
