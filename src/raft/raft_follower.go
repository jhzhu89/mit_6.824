package raft

import (
	"fmt"
	"raft/util"
)

// Shared by follower and candicate.
func applyLogEntries(canceller util.Canceller, raft *Raft) {
	for {
		select {
		case <-canceller.Cancelled():
			return
		case <-raft.committedCh:
			commitIndex := int(raft.commitIndex.AtomicGet())
			raft.raftLog.Lock()
			for i := raft.lastApplied + 1; i <= commitIndex; i++ {
				l := raft.getLogEntry(i)
				if l == nil {
					panic(fmt.Sprintf("the log entry at %v should not be nil", i))
				}
				DPrintf("[%v - %v] - apply log %#v...", raft.me, raft.raftState.AtomicGet(), *l)
				raft.applyCh <- ApplyMsg{Index: l.Index, Command: l.Command}
			}
			raft.raftLog.Unlock()
			raft.lastApplied = commitIndex
		}
	}
}

//
// Run RPC handlers in the main loop, receive heart beats in another routine.
//
func (rf *Raft) runFollower() {
	DPrintf("[node: %v] - in runFollower()...", rf.me)
	rgm := util.NewRoutineGroupMonitor()
	defer rf.committedChRH(&rf.committedCh)()
	rgm.GoFunc(func(canceller util.Canceller) { applyLogEntries(canceller, rf) })
	defer rf.timerRH(&rf.electionTimer)()
	defer rgm.Done() // Defer this at last bacause of the race condition.

	for rf.raftState.AtomicGet() == Follower {
		select {
		case rpc := <-rf.rpcCh:
			//DPrintf("[%v - %v] - received a RPC request: %v...\n", rf.me, rf.raftState.AtomicGet(), rpc.args)
			rf.processRPC(rpc)
		case <-rf.electionTimer.C:
			DPrintf("[%v - %v] - election timed out, promote to candidate...", rf.me, rf.raftState.AtomicGet())
			rf.raftState.AtomicSet(Candidate)
			return
		}
	}
}
