package raft

import (
	//"fmt"
	"raft/util"
	"time"
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

func (rf *Raft) sendHeartbeats(canceller util.Canceller, stepDownSig util.Signal) {
	for {
		select {
		case <-canceller.Cancelled():
			return
		case <-stepDownSig.Received():
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
						//DPrintf("[%v - %v] - send heardbeat to %v...\n", from, rf.raftState.AtomicGet(), to)
						if rf.sendAppendEntries(to,
							&AppendEntriesArgs{
								Term:     int(rf.CurrentTerm.AtomicGet()),
								LeaderId: from,
								//LeaderCommit: int(rf.commitIndex.AtomicGet()),
								Entires: nil},
							reply) {
							if reply.Term > int(rf.CurrentTerm.AtomicGet()) {
								// Fall back to Follower
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
	rf.committedCh = make(chan struct{}, 1)
	rf.committer = newCommitter(rf.committedCh)
	rf.committer.quoromSize = rf.quorum()
	defer func() { rf.committer, rf.committedCh = nil, nil }()

	rgm := util.NewRoutineGroupMonitor()
	defer rgm.Done()
	stepDownSig := util.NewSignal()
	rgm.GoFunc(func(canceller util.Canceller) { rf.sendHeartbeats(canceller, stepDownSig) })
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
			DPrintf("[%v - %v] - received a RPC request: %v...\n", rf.me, rf.raftState.AtomicGet(), rpc.args)
			// TODO: handlers return next state, and we change the state in this loop.
			rf.processRPC(rpc)
		case msg := <-rf.appendCh:
			DPrintf("[%v - %v] - received an append msg: %v...\n", rf.me, rf.raftState.AtomicGet(), msg)
			rf.replicate(msg)
		case <-stepDownSig.Received():
			DPrintf("[%v - %v] - received step down signal in leader loop...\n",
				rf.me, rf.raftState.AtomicGet())
			rf.raftState.AtomicSet(Follower)
			return
		case <-rf.committedCh:
			DPrintf("[%v - %v] - receives commit signal, send to applyCh...\n", rf.me, rf.raftState.AtomicGet())
			rf.commitIndex.AtomicSet(int32(rf.committer.getCommitIndex()))
			es := rf.committer.getCommitted()
			for _, log := range es {
				DPrintf("[%v - %v] - send %#v to applyCh...\n", rf.me, rf.raftState.AtomicGet(), *log)
				rf.applyCh <- ApplyMsg{Index: log.Index, Command: log.Command}
				rf.lastApplied = log.Index
			}
		}
	}
}
