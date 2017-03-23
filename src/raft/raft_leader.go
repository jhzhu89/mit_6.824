package raft

import (
	"github.com/jhzhu89/log"
	"raft/util"
	"strconv"
)

func (rf *Raft) replicate(appMsg *AppendMsg) {
	defer close(appMsg.done)
	// 1. store the logentry
	log := &appMsg.LogEntry
	log.Index = rf.lastIndex() + 1
	log.Term = int(rf.CurrentTerm.AtomicGet())
	rf.raftLog.Lock()
	rf.append(log)
	rf.raftLog.Unlock()
	rf.committer.addLogs([]*LogEntry{log})

	// 2. replicate to others
	for _, repl := range rf.replicators {
		repl.replicate()
	}
}

func (rf *Raft) runLeader() {
	rf.committedCh = make(chan struct{}, 1)
	rf.committer = newCommitter(rf.committedCh, int(rf.commitIndex)+1)
	rf.committer.quoromSize = rf.quorum()
	defer func() { rf.committer, rf.committedCh = nil, nil }()

	rgm := util.NewRoutineGroupMonitor()
	defer rgm.Done()
	stepDownSig := util.NewSignal()
	rf.replicators = make(map[int]*replicator, 0)
	for i, _ := range rf.peers {
		if i != rf.me {
			repl := newReplicator(rf.me, i, rf)
			rf.replicators[i] = repl
			rgm.GoFunc(func(canceller util.Canceller) { repl.run(canceller, stepDownSig) })
		}
	}
	defer func() { rf.replicators = nil }()

	for rf.raftState.AtomicGet() == Leader {
		select {
		case rpc := <-rf.rpcCh:
			log.V(0).WithField(strconv.Itoa(rf.me), rf.raftState.AtomicGet()).
				WithField("rpc", rpc.args).Infoln("received a RPC request...")
			// TODO: handlers return next state, and we change the state in this loop.
			rf.processRPC(rpc)
		case msg := <-rf.appendCh:
			log.V(0).WithField(strconv.Itoa(rf.me), rf.raftState.AtomicGet()).
				WithField("app", msg).Infoln("received an append msg...")
			rf.replicate(msg)
		case <-stepDownSig.Received():
			log.V(0).WithField(strconv.Itoa(rf.me), rf.raftState.AtomicGet()).
				Infoln("received step down signal in leader loop...")
			rf.raftState.AtomicSet(Follower)
			return
		case <-rf.committedCh:
			log.V(0).WithField(strconv.Itoa(rf.me), rf.raftState.AtomicGet()).
				Infoln("receives commit signal, send to applyCh...")
			newCommitIndex := rf.committer.getCommitIndex()
			rf.commitIndex.AtomicSet(int32(newCommitIndex))
			log.V(1).WithField(strconv.Itoa(rf.me), rf.raftState.AtomicGet()).
				WithField("lastApplied", rf.lastApplied).Infoln("before apply...")
			for i := rf.lastApplied + 1; i <= newCommitIndex; i++ {
				log := rf.getLogEntry(i)
				rf.applyCh <- ApplyMsg{Index: log.Index, Command: log.Command}
			}
			rf.lastApplied = newCommitIndex
			log.V(1).WithField(strconv.Itoa(rf.me), rf.raftState.AtomicGet()).
				WithField("lastApplied", rf.lastApplied).Infoln("after apply...")
		}
	}
}
