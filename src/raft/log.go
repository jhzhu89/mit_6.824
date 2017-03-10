package raft

// Log Index start from 1.
type raftLog struct {
	Logs  map[int]*LogEntry
	first int
	last  int
}

func newRaftLog() *raftLog {
	return &raftLog{Logs: make(map[int]*LogEntry), first: 0, last: 0}
}

func (l *raftLog) getLogEntry(index int) *LogEntry {
	// may be nil
	return l.Logs[index]
}

func (l *raftLog) append(entry *LogEntry) bool {
	if entry.Index <= 0 {
		return false
	}

	if _, ok := l.Logs[entry.Index]; ok {
		return false
	}

	if l.first <= 0 || entry.Index < l.first {
		l.first = entry.Index
	}
	if entry.Index > l.last {
		l.last = entry.Index
	}

	l.Logs[entry.Index] = entry

	return true
}

func (l *raftLog) lastLogEntry() *LogEntry {
	if len(l.Logs) == 0 {
		return nil
	}

	return l.Logs[l.last]
}

func (l *raftLog) lastIndex() int {
	return l.last
}
