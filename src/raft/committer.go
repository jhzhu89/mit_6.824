package raft

import (
	"fmt"
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

func (c *committer) tryCommitOne(index int) (e error) {
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

func (c *committer) tryCommitRange(s, e int) (err error) {
	for i := s; i <= e; i++ {
		err = c.tryCommitOne(i)
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
