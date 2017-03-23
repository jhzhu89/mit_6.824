package raft

import (
	"bytes"
	"encoding/gob"
	"sync/atomic"

	"github.com/jhzhu89/log"
)

type RaftState uint32

func (rs *RaftState) AtomicGet() RaftState {
	return RaftState(atomic.LoadUint32((*uint32)(rs)))
}

func (rs *RaftState) AtomicSet(v RaftState) {
	atomic.StoreUint32((*uint32)(rs), uint32(v))
}

func (rs RaftState) String() string {
	switch rs {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Invalid"
	}
}

// Persistent state on all servers.
type persistentState struct {
	CurrentTerm Int32 // TODO: add atomic read/write op on it.
	VotedFor    int
	raftLog
}

func (p *persistentState) persistRaftState(persister *Persister) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if e := encoder.Encode(p); e != nil {
		log.Errorln("fail to encode raftState")
		return
	}

	persister.SaveRaftState(buf.Bytes())
}

func (p *persistentState) readRaftState(persister *Persister) {
	var buf bytes.Buffer
	if _, e := buf.Read(persister.ReadRaftState()); e != nil {
		log.Errorln("fail to read raftState")
		return
	}
	decoder := gob.NewDecoder(&buf)
	if e := decoder.Decode(p); e != nil {
		log.Errorln("fail to encode raftState")
		return
	}
}

func (p *persistentState) truncateLogPrefix(i int) {
	// TODO
}

// Including ith.
func (p *persistentState) truncateLogSuffix(i int) {

}

// Volatile state on all servers.
type volatileState struct {
	commitIndex Int32 // need to read/write commitIndex atomically.
	lastApplied int
	state       RaftState
}

// Volatile state on leaders.
type leaderVolatileState struct {
	replicators map[int]*replicator
	committer   *committer
}
