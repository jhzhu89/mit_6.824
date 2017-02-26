package raft

import (
	"time"
)

//
// Run RPC handlers in the main loop, receive heart beats in another routine.
//
func (rf *Raft) runFollower() {
	DPrintf("[node: %v] - in runFollower()...", rf.me)
	rf.electionTimer = time.NewTimer(randomTimeout(ElectionTimeout))
	defer func() { rf.electionTimer = nil }()

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
