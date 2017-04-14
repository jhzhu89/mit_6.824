package raft

import "fmt"

//import "github.com/jhzhu89/log"

// Log Index start from 1.
type raftLog struct {
	logs map[int]*LogEntry
	last int
}

func newRaftLog() *raftLog {
	return &raftLog{logs: make(map[int]*LogEntry), last: 0}
}

func (l *raftLog) getLogEntry(index int) *LogEntry {
	// may be nil
	log, ok := l.logs[index]
	if !ok {
		return nil
	}
	return log
}

func (l *raftLog) getLogEntries(from, to int) []*LogEntry {
	res := make([]*LogEntry, 0)
	for i := from; i <= to; i++ {
		e := l.getLogEntry(i)
		if e == nil {
			//log.Errorf("got a nil entry for index %v", i)
			panic(fmt.Sprintf("got a nil entry for index %v", i))
			return res
		}
		res = append(res, e)
	}
	return res
}

func (l *raftLog) appendLogs(entries []*LogEntry) {
	//log.V(1).Infof("entries: %v", entries)
	for i, e := range entries {
		le := l.getLogEntry(e.Index)
		if le != nil {
			if e.Term == le.Term {
				continue
			}
			// mismatch
			l.removeSuffix(le.Index)
		}

		for _, e := range entries[i:] {
			l.appendOne(e)
		}
		break
	}
}

func (l *raftLog) appendOne(entry *LogEntry) bool {
	if entry.Index != l.last+1 {
		return false
	}
	if _, ok := l.logs[entry.Index]; ok {
		return false
	}
	l.logs[entry.Index] = entry
	l.last++
	return true
}

func (l *raftLog) lastLogEntry() *LogEntry {
	if len(l.logs) == 0 {
		return nil
	}

	return l.logs[l.last]
}

func (l *raftLog) lastIndex() int {
	return l.last
}

func (l *raftLog) removeSuffix(from int) {
	if from <= 0 || from > l.last {
		return
	}

	for i := from; i <= l.last; i++ {
		delete(l.logs, i)
	}

	l.last = from - 1
}
