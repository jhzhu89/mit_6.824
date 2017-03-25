package raft

import (
	"fmt"
	"github.com/jhzhu89/log"
	"raft/util"
	"strconv"
)

func (rf *Raft) replicate(appMsg *AppendMsg) {
	defer close(appMsg.done)
	// 1. store the logentry
	log := &appMsg.LogEntry
	log.Index = rf.lastIndex() + 1
	log.Term = int(rf.currentTerm.AtomicGet())
	rf.raftLog.Lock()
	rf.append(log)
	rf.persistRaftState(rf.persister)
	rf.raftLog.Unlock()
	rf.committer.addLogs([]*LogEntry{log})

	// 2. replicate to others
	for _, repl := range rf.replicators {
		repl.replicate()
	}
}

func (rf *Raft) runLeader() {
	defer rf.committedChH(&rf.committedCh)()
	rf.committer = newCommitter(rf.committedCh, int(rf.commitIndex)+1)
	rf.committer.quoromSize = rf.quorum()
	defer func() { rf.committer = nil }()

	rg := util.NewRoutineGroup()
	stepDownSig := util.NewSignal()
	rf.replicators = make(map[int]*replicator, 0)
	for i, _ := range rf.peers {
		if i != rf.me {
			repl := newReplicator(rf.me, i, rf)
			rf.replicators[i] = repl
			rg.GoFunc(func(ctx util.CancelContext) { repl.run(ctx, stepDownSig) })
		}
	}
	defer func() { rf.replicators = nil }()
	defer rg.Done()

	rg.GoFunc(func(ctx util.CancelContext) {
		applyLogEntries(ctx, rf, func() int {
			newCommitIndex := rf.committer.getCommitIndex()
			rf.commitIndex.AtomicSet(int32(newCommitIndex))
			return newCommitIndex
		})
	})

	logV0 := log.V(0).WithField(strconv.Itoa(rf.me), fmt.Sprintf("%v, %v",
		rf.state.AtomicGet(), rf.currentTerm.AtomicGet()))
	logV1 := log.V(1).WithField(strconv.Itoa(rf.me), fmt.Sprintf("%v, %v",
		rf.state.AtomicGet(), rf.currentTerm.AtomicGet()))

	for rf.state.AtomicGet() == Leader {
		select {
		case rpc := <-rf.rpcCh:
			logV0.Clone().WithField("rpc", rpc.args).Infoln("received a RPC request...")
			// TODO: handlers return next state, and we change the state in this loop.
			// should send step down sig when in the handler recevied larger term.
			rf.processRPC(rpc)
		case msg := <-rf.appendCh:
			logV1.Clone().WithField("app", msg).Infoln("received an append msg...")
			rf.replicate(msg)
		case <-stepDownSig.Received():
			logV0.Clone().Infoln("received step down signal in leader loop...")
			rf.state.AtomicSet(Follower)
			return
			//case <-rf.committedCh:
			//	logV0.Clone().Infoln("receives commit signal, send to applyCh...")
			//	newCommitIndex := rf.committer.getCommitIndex()
			//	rf.commitIndex.AtomicSet(int32(newCommitIndex))
			//	logV1.Clone().WithField("lastApplied", rf.lastApplied).Infoln("before apply...")
			//	for i := rf.lastApplied + 1; i <= newCommitIndex; i++ {
			//		log := rf.getLogEntry(i)
			//		rf.applyCh <- ApplyMsg{Index: log.Index, Command: log.Command}
			//	}
			//	rf.lastApplied = newCommitIndex
			//	logV1.Clone().WithField("lastApplied", rf.lastApplied).Infoln("after apply...")
		}
	}
}
