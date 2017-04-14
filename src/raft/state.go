package raft

import (
	"bytes"
	"encoding/gob"
	"sync"
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
	sync.RWMutex
	currentTerm Int32
	votedFor    int
	raftLog
}

type _persistentState struct {
	CurrentTerm int
	VotedFor    int
	Logs        map[int]*LogEntry
	Last        int
}

func (p *persistentState) persistRaftState(persister *Persister) {
	ps := _persistentState{
		CurrentTerm: int(p.currentTerm.AtomicGet()),
		VotedFor:    p.votedFor,
		Logs:        p.logs,
		Last:        p.last,
	}

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if e := encoder.Encode(ps); e != nil {
		log.E(e).Errorln("fail to encode raftState...")
		return
	}

	persister.SaveRaftState(buf.Bytes())
}

func (p *persistentState) readRaftState(persister *Persister) {
	b := persister.ReadRaftState()
	if b == nil || len(b) == 0 {
		log.V(1).Infoln("no raft state persisted yet...")
		return
	}
	ps := _persistentState{}
	var buf bytes.Buffer
	if _, e := buf.Write(b); e != nil {
		log.E(e).Errorln("fail to create buffer from bytes...")
		return
	}
	decoder := gob.NewDecoder(&buf)
	if e := decoder.Decode(&ps); e != nil {
		log.E(e).Errorln("fail to decode raftState...")
		return
	}

	p.currentTerm.AtomicSet(int32(ps.CurrentTerm))
	p.votedFor = ps.VotedFor
	p.logs = ps.Logs
	p.last = ps.Last
}

func (p *persistentState) truncateLogPrefix(i int) {
	// TODO
}

// Including ith.
func (p *persistentState) truncateLogSuffix(i int) {

}
