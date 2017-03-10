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
			for i := raft.lastApplied + 1; i <= raft.commitIndex; i++ {
				l := raft.getLogEntry(i)
				if l == nil {
					panic(fmt.Sprintf("the log entry at %v should not be nil", i))
				}
				raft.applyCh <- ApplyMsg{Index: l.Index, Command: l.Command}
			}
			raft.lastApplied = raft.commitIndex
		}
	}
}

//
// Run RPC handlers in the main loop, receive heart beats in another routine.
//
func (rf *Raft) runFollower() {
	DPrintf("[node: %v] - in runFollower()...", rf.me)
	rgm := util.NewRoutineGroupMonitor()
	defer rgm.Done()
	defer rf.committedChRH(&rf.committedCh)()
	rgm.GoFunc(func(canceller util.Canceller) { applyLogEntries(canceller, rf) })

	defer rf.timerRH(&rf.electionTimer)()

	for rf.raftState.AtomicGet() == Follower {
		select {
		case rpc := <-rf.rpcCh:
			DPrintf("[%v - %v] - received a RPC request: %v...\n", rf.me, rf.raftState.AtomicGet(), rpc.args)
			rf.processRPC(rpc)
		case <-rf.electionTimer.C:
			DPrintf("[%v - %v] - election timed out, promote to candidate...", rf.me, rf.raftState.AtomicGet())
			rf.raftState.AtomicSet(Candidate)
			return
		}
	}
}
