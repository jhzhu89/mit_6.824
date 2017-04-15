package raft

import (
	"fmt"
	"raft/util"
	"strconv"
	"time"

	"github.com/jhzhu89/log"
)

// Shared by follower and candicate.
func applyLogEntries(ctx util.Context, raft *Raft, getCommitIndex func() int) {
	logV1 := log.V(1).F(strconv.Itoa(raft.me), fmt.Sprintf("%v, %v",
		raft.state.AtomicGet(), raft.currentTerm.AtomicGet()))
	logV2 := log.V(2).F(strconv.Itoa(raft.me), fmt.Sprintf("%v, %v",
		raft.state.AtomicGet(), raft.currentTerm.AtomicGet()))
	for {
		select {
		case <-ctx.Done():
			return
		case <-raft.committedCh:
			logV1.Clone().Infoln("received commit signal...")
			commitTo := getCommitIndex()
			logV2.Clone().F("lastApplied", raft.lastApplied).Infoln("before apply...")
			raft.persistentState.RLock()
			entries := raft.getLogEntries(raft.lastApplied+1, commitTo)
			for _, entry := range entries {
				raft.applyCh <- ApplyMsg{Index: entry.Index, Command: entry.Command}
				logV2.Clone().F("applied_message", entry).Infoln("")
			}
			raft.persistentState.RUnlock()
			raft.lastApplied = commitTo
			logV1.Clone().F("lastApplied", raft.lastApplied).Infoln("after apply...")
		}
	}
}

func rejectAppendMsg(raft *Raft, ctx util.Context) {
	for {
		select {
		case msg := <-raft.appendCh:
			msg.isLeader = false
			msg.Index = -1
			msg.Term = int(raft.currentTerm.AtomicGet())
			close(msg.done)
		case <-ctx.Done():
			return
		}
	}
}

//
// Run RPC handlers in the main loop.
//
func (rf *Raft) runFollower() {
	// init resources
	rf.committedCh = make(chan struct{}, 1)
	rg, donef := util.NewRoutineGroup()

	defer func() {
		donef()
		rf.committedCh = nil
	}()

	rg.GoFunc(func(ctx util.Context) {
		applyLogEntries(ctx, rf, func() int { return int(rf.commitIndex.AtomicGet()) })
	})
	rg.GoFunc(func(ctx util.Context) { rejectAppendMsg(rf, ctx) })

	logV1 := log.V(1).F(strconv.Itoa(rf.me), fmt.Sprintf("%v, %v",
		rf.state.AtomicGet(), rf.currentTerm.AtomicGet()))
	logV2 := log.V(2).F(strconv.Itoa(rf.me), fmt.Sprintf("%v, %v",
		rf.state.AtomicGet(), rf.currentTerm.AtomicGet()))

	rf.electTimer = time.NewTimer(randomTimeout(ElectionTimeout))
	defer func() { rf.electTimer = nil }()
	for rf.state.AtomicGet() == Follower {
		select {
		case rpc := <-rf.rpcCh:
			logV2.Clone().F("rpc", rpc.args).Infoln("received a RPC request...")
			rf.processRPC(rpc)
		case <-rf.electTimer.C:
			logV1.Infoln("election timed out, promote to candidate...")
			rf.state.AtomicSet(Candidate)
			return
		}
	}
}
