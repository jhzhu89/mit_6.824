package raft

import (
	"fmt"
	"github.com/jhzhu89/log"
	"raft/util"
	"strconv"
)

func replicate(rf *Raft, appMsg *AppendMsg) {
	defer close(appMsg.done)
	// 1. store the logentry
	appMsg.isLeader = true
	l := &appMsg.LogEntry
	rf.persistentState.Lock()
	l.Index = rf.lastIndex() + 1
	l.Term = int(rf.currentTerm.AtomicGet())
	if rf.appendOne(l) == false {
		panic(fmt.Sprintf("failed to append %v to raft log", l))
	}
	rf.persistRaftState(rf.persister)
	rf.persistentState.Unlock()
	rf.committer.addLogs([]*LogEntry{l})

	// 2. replicate to others
	for _, repl := range rf.replicators {
		repl.replicate()
	}
}

func leaderHandleAppendMsg(raft *Raft, ctx util.CancelContext, stepDownSig util.Signal) {
	defer func() {
		// Reject the remaining appendMsg(s) in appendCh.
		for {
			select {
			case msg := <-raft.appendCh:
				msg.isLeader = false
				msg.Index = -1
				msg.Term = int(raft.currentTerm.AtomicGet())
				close(msg.done)
			default:
				return
			}
		}
	}()

	logV2 := log.V(2).F(strconv.Itoa(raft.me), fmt.Sprintf("%v, %v",
		raft.state.AtomicGet(), raft.currentTerm.AtomicGet()))
	for {
		select {
		case msg := <-raft.appendCh:
			logV2.Clone().F("app", msg).Infoln("received an append msg...")
			replicate(raft, msg)
		case <-stepDownSig.Received():
			return
		case <-ctx.Done():
			return
		}
	}
}

func (rf *Raft) runLeader() {
	defer rf.committedChH(&rf.committedCh)()
	defer rf.committerH(&rf.committer)()

	rg, donef := util.NewRoutineGroup()
	stepDownSig := util.NewSignal()
	rf.replicators = make(map[int]*replicator, 0)
	for i, _ := range rf.peers {
		if i != rf.me {
			rf.replicators[i] = newReplicator(rg, stepDownSig, rf, rf.me, i)
		}
	}
	defer func() { rf.replicators = nil }()
	defer donef()

	rg.GoFunc(func(ctx util.CancelContext) {
		applyLogEntries(ctx, rf, func() int {
			newCommitIndex := rf.committer.getCommitIndex()
			rf.commitIndex.AtomicSet(int32(newCommitIndex))
			return newCommitIndex
		})
	})
	rg.GoFunc(func(ctx util.CancelContext) { leaderHandleAppendMsg(rf, ctx, stepDownSig) })

	logV1 := log.V(1).F(strconv.Itoa(rf.me), fmt.Sprintf("%v, %v",
		rf.state.AtomicGet(), rf.currentTerm.AtomicGet()))

	// Start a NOOP letting leader be able to commit logs in previous term.
	_, _, isLeader := rf.Start(NOOP)
	if !isLeader {
		log.Warningf("trying to apply a NOOP but failed, change to Follower...")
		rf.state.AtomicSet(Follower)
		return
	}

	for rf.state.AtomicGet() == Leader {
		select {
		case rpc := <-rf.rpcCh:
			logV1.Clone().F("rpc", rpc.args).Infoln("received a RPC request...")
			// TODO: handlers return next state, and we change the state in this loop.
			// should send step down sig when in the handler recevied larger term.
			rf.processRPC(rpc)
		case <-stepDownSig.Received():
			logV1.Clone().Infoln("received step down signal in leader loop...")
			rf.state.AtomicSet(Follower)
			return
		}
	}
}
