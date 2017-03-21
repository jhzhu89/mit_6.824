package raft

import (
	"fmt"
	"raft/util"
	"strconv"

	"github.com/jhzhu89/log"
)

// Shared by follower and candicate.
func applyLogEntries(canceller util.Canceller, raft *Raft) {
	for {
		select {
		case <-canceller.Cancelled():
			return
		case <-raft.committedCh:
			log.V(0).WithField(strconv.Itoa(raft.me), raft.raftState.AtomicGet()).
				Infoln("follower/candicate received commit signal...")
			commitIndex := int(raft.commitIndex.AtomicGet())
			log.V(1).WithField(strconv.Itoa(raft.me), raft.raftState.AtomicGet()).
				WithField("lastApplied", raft.lastApplied).Infoln("before apply...")
			raft.raftLog.Lock()
			for i := raft.lastApplied + 1; i <= commitIndex; i++ {
				l := raft.getLogEntry(i)
				if l == nil {
					panic(fmt.Sprintf("the log entry at %v should not be nil", i))
				}
				raft.applyCh <- ApplyMsg{Index: l.Index, Command: l.Command}
			}
			raft.raftLog.Unlock()
			raft.lastApplied = commitIndex
			log.V(1).WithField(strconv.Itoa(raft.me), raft.raftState.AtomicGet()).
				WithField("lastApplied", raft.lastApplied).Infoln("after apply...")
		}
	}
}

//
// Run RPC handlers in the main loop, receive heart beats in another routine.
//
func (rf *Raft) runFollower() {
	rgm := util.NewRoutineGroupMonitor()
	defer rf.committedChRH(&rf.committedCh)()
	rgm.GoFunc(func(canceller util.Canceller) { applyLogEntries(canceller, rf) })
	defer rf.timerRH(&rf.electionTimer)()
	defer rgm.Done() // Defer this at last bacause of the race condition.

	for rf.raftState.AtomicGet() == Follower {
		select {
		case rpc := <-rf.rpcCh:
			log.V(2).WithField(strconv.Itoa(rf.me), rf.raftState.AtomicGet()).WithField("rpc", rpc.args).Infoln("received a RPC request...")
			rf.processRPC(rpc)
		case <-rf.electionTimer.C:
			log.V(0).WithField(strconv.Itoa(rf.me), rf.raftState.AtomicGet()).Infoln("election timed out, promote to candidate...")
			rf.raftState.AtomicSet(Candidate)
			return
		}
	}
}
