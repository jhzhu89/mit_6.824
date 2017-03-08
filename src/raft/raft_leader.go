package raft

import (
	"raft/util"
	"sync/atomic"
	"time"
)

func (rf *Raft) replicate(stepDownSig util.Signal, msg *AppendMsg) {
	defer close(msg.done)
	if rf.raftState.AtomicGet() != Leader {
		msg.isLeader = false
		return
	}

	// start to append log
	prevLog := rf.lastLogEntry()
	// TODO: correct the log index here.
	log := &msg.LogEntry
	log.Index = rf.lastIndex() + 1
	log.Term = rf.CurrentTerm
	rf.append(log)

	// TODO: call replicator to replicate logs.
	req := &AppendEntriesArgs{
		Term: rf.CurrentTerm, LeaderId: rf.me,
		LeaderCommit: rf.commitIndex, Entires: []*LogEntry{log},
	}
	if prevLog == nil {
		req.PrevLogIndex, req.PrevLogTerm = -1, -1
	} else {
		req.PrevLogIndex, req.PrevLogTerm = prevLog.Index, prevLog.Term
	}

	var nCommit uint32 = 1
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(from, to int) {
				reply := &AppendEntriesReply{}
				DPrintf("[%v - %v] - append logs to %v...\n", from, rf.raftState.AtomicGet(), to)
				if rf.sendAppendEntries(to, req, reply) {
					if reply.Term > rf.CurrentTerm {
						select {
						// Fall back to Follower
						case <-stepDownSig.Received():
							DPrintf("[%v - %v] - someone already sent step down signal...\n",
								rf.me, rf.raftState.AtomicGet())
							return
						default:
						}
						stepDownSig.Send()
						DPrintf("[%v - %v] - step down signal sent...\n", rf.me, rf.raftState.AtomicGet())
					} else {
						if reply.Success {
							DPrintf("[%v - %v] - successfully replicated to %v...\n", from, rf.raftState.AtomicGet(), to)
							atomic.AddUint32(&nCommit, 1)
							n := atomic.LoadUint32(&nCommit)
							if n >= uint32(rf.quorum()) {
								// send an ApplyMsg
								DPrintf("[%v - %v] - sending a message to applyCh...\n", from, rf.raftState.AtomicGet())
								// send to commit chan
								rf.commitIndex = msg.Index
							}
						}
					}
				}
			}(rf.me, i)
		}
	}

	return
}

// TODO: need to atomically acess raft states.
func (rf *Raft) sendHeartbeats(stopper util.Stopper, stepDownSig util.Signal) {
	for {
		select {
		case <-stopper.Stopped():
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
	// Tell spawned routines to stop.
	stopper, stopf := util.WithStop()
	defer stopf()
	stepDownSig := util.NewSignal()
	goFunc(func() { rf.sendHeartbeats(stopper, stepDownSig) })

	for rf.raftState.AtomicGet() == Leader {
		select {
		case rpc := <-rf.rpcCh:
			DPrintf("[%v - %v] - received a RPC request: %v...\n", rf.me, rf.raftState.AtomicGet(), rpc.args)
			rf.processRPC(rpc)
		case msg := <-rf.appendCh:
			DPrintf("[%v - %v] - received an append msg: %v...\n", rf.me, rf.raftState.AtomicGet(), msg)
			// Replicate log to followers.
			rf.replicate(stepDownSig, msg)
		case <-stepDownSig.Received():
			DPrintf("[%v - %v] - received step down signal in leader loop...\n",
				rf.me, rf.raftState.AtomicGet())
			rf.raftState.AtomicSet(Follower)
			return
		}
	}
}
