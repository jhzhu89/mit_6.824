package raft

import (
	"raft/util"
	"time"
)

func (rf *Raft) replicate(monitor *util.RoutineGroupMonitor, stepDownSig util.Signal, appMsg *AppendMsg) {
	defer close(appMsg.done)
	// 1. store the logentry
	log := &appMsg.LogEntry
	log.Index = rf.lastIndex() + 1
	log.Term = rf.CurrentTerm
	rf.append(log)
	rf.committer.addLogs([]*LogEntry{log})

	// 2. replicate to others
	for _, repl := range rf.replicators {
		repl_ := repl
		monitor.GoFunc(func(canceller util.Canceller) {
			repl_.replicateTo(canceller, stepDownSig, log.Index)
		})
	}
}

func (rf *Raft) sendHeartbeats(canceller util.Canceller, stepDownSig util.Signal) {
	for {
		select {
		case <-canceller.Cancelled():
			return
		case <-time.After(randomTimeout(ElectionTimeout / 10)):
			for i, _ := range rf.peers {
				if i != rf.me {
					go func(from, to int) {
						// Make sure we are still the leader.
						if rf.raftState.AtomicGet() != Leader {
							DPrintf("[%v - %v] - not leader anymore, stop send heartbeat...\n",
								from, rf.raftState.AtomicGet())
							return
						}
						reply := &AppendEntriesReply{}
						DPrintf("[%v - %v] - send heardbeat to %v...\n", from, rf.raftState.AtomicGet(), to)
						if rf.sendAppendEntries(to,
							&AppendEntriesArgs{
								Term:         rf.CurrentTerm,
								LeaderId:     from,
								LeaderCommit: rf.commitIndex},
							reply) {
							if reply.Term > rf.CurrentTerm {
								// Fall back to Follower
								// raftState change should be taken in run loop to avoid race condition.
								select {
								case <-stepDownSig.Received():
									DPrintf("[%v - %v] - someone already sent step down signal...\n",
										rf.me, rf.raftState.AtomicGet())
									return
								default:
								}
								stepDownSig.Send()
								DPrintf("[%v - %v] - step down signal sent...\n", rf.me, rf.raftState.AtomicGet())
							}
						}
					}(rf.me, i)
				}
			}
		}
	}
}

func (rf *Raft) runLeader() {
	rf.replicators = make(map[int]*replicator, 0)
	for i, _ := range rf.peers {
		if i != rf.me {
			rf.replicators[i] = newReplicator(rf.me, i, rf)
		}
	}
	rf.committer = newCommitter()
	defer func() { rf.replicators, rf.committer = nil, nil }()

	rgm := util.NewRoutineGroupMonitor()
	defer rgm.Done()
	stepDownSig := util.NewSignal()
	rgm.GoFunc(func(canceller util.Canceller) { rf.sendHeartbeats(canceller, stepDownSig) })

	for rf.raftState.AtomicGet() == Leader {
		select {
		case rpc := <-rf.rpcCh:
			DPrintf("[%v - %v] - received a RPC request: %v...\n", rf.me, rf.raftState.AtomicGet(), rpc.args)
			// TODO: handlers return next state, and we change the state in this loop.
			rf.processRPC(rpc)
		case msg := <-rf.appendCh:
			DPrintf("[%v - %v] - received an append msg: %v...\n", rf.me, rf.raftState.AtomicGet(), msg)
			rf.replicate(rgm, stepDownSig, msg)
		case <-stepDownSig.Received():
			DPrintf("[%v - %v] - received step down signal in leader loop...\n",
				rf.me, rf.raftState.AtomicGet())
			rf.raftState.AtomicSet(Follower)
			return
		}
	}
}
