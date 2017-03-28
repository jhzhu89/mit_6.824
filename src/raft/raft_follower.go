package raft

import (
	"fmt"
	"raft/util"
	"strconv"

	"github.com/jhzhu89/log"
)

// Shared by follower and candicate.
func applyLogEntries(ctx util.CancelContext, raft *Raft, getCommitIndex func() int) {
	logV0 := log.V(0).WithField(strconv.Itoa(raft.me), fmt.Sprintf("%v, %v",
		raft.state.AtomicGet(), raft.currentTerm.AtomicGet()))
	logV1 := log.V(1).WithField(strconv.Itoa(raft.me), fmt.Sprintf("%v, %v",
		raft.state.AtomicGet(), raft.currentTerm.AtomicGet()))
	for {
		select {
		case <-ctx.Done():
			return
		case <-raft.committedCh:
			logV0.Clone().Infoln("received commit signal...")
			commitTo := getCommitIndex()
			logV0.Clone().WithField("lastApplied", raft.lastApplied).Infoln("before apply...")
			raft.persistentState.RLock()
			entries := raft.getLogEntries(raft.lastApplied+1, commitTo)
			for _, entry := range entries {
				raft.applyCh <- ApplyMsg{Index: entry.Index, Command: entry.Command}
				logV1.Clone().WithField("applyMsg_index", entry.Index).Infoln("")
			}
			raft.persistentState.RUnlock()
			raft.lastApplied = commitTo
			logV0.Clone().WithField("lastApplied", raft.lastApplied).Infoln("after apply...")
		}
	}
}

//
// Run RPC handlers in the main loop.
//
func (rf *Raft) runFollower() {
	rg := util.NewRoutineGroup()
	defer rf.committedChH(&rf.committedCh)()
	rg.GoFunc(func(ctx util.CancelContext) {
		applyLogEntries(ctx, rf, func() int { return int(rf.commitIndex.AtomicGet()) })
	})
	defer rf.timerH(&rf.electTimer)()
	defer rg.Done() // Defer this at last bacause of the race condition.

	logV0 := log.V(0).WithField(strconv.Itoa(rf.me), fmt.Sprintf("%v, %v",
		rf.state.AtomicGet(), rf.currentTerm.AtomicGet()))
	logV2 := log.V(2).WithField(strconv.Itoa(rf.me), fmt.Sprintf("%v, %v",
		rf.state.AtomicGet(), rf.currentTerm.AtomicGet()))
	for rf.state.AtomicGet() == Follower {
		select {
		case rpc := <-rf.rpcCh:
			logV2.Clone().WithField("rpc", rpc.args).Infoln("received a RPC request...")
			rf.processRPC(rpc)
		case <-rf.electTimer.C:
			logV0.Infoln("election timed out, promote to candidate...")
			rf.state.AtomicSet(Candidate)
			return
		}
	}
}
