package raft

import (
	"fmt"
	"github.com/jhzhu89/log"
	"raft/util"
	"strconv"
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
							log.V(0).WithField(strconv.Itoa(rf.me), rf.raftState.AtomicGet()).
								Infoln("not leader anymore, stop send heartbeat...")
							return
						}
						reply := &AppendEntriesReply{}
						log.V(2).WithField(strconv.Itoa(rf.me), rf.raftState.AtomicGet()).
							WithField("to", to).Infoln("send heardbeat...")
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
								log.V(0).WithField(strconv.Itoa(rf.me), rf.raftState.AtomicGet()).WithField(fmt.Sprintf("%v's Term", to), reply.Term).
									WithField("my Term", rf.CurrentTerm.AtomicGet()).
									Infoln("step down signal sent...")
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
	rf.committer = newCommitter(rf.committedCh, int(rf.commitIndex)+1)
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
			log.V(0).WithField(strconv.Itoa(rf.me), rf.raftState.AtomicGet()).
				WithField("rpc", rpc.args).Infoln("received a RPC request...")
			// TODO: handlers return next state, and we change the state in this loop.
			rf.processRPC(rpc)
		case msg := <-rf.appendCh:
			log.V(0).WithField(strconv.Itoa(rf.me), rf.raftState.AtomicGet()).
				WithField("app", msg).Infoln("received an append msg...")
			rf.replicate(msg)
		case <-stepDownSig.Received():
			log.V(0).WithField(strconv.Itoa(rf.me), rf.raftState.AtomicGet()).
				Infoln("received step down signal in leader loop...")
			rf.raftState.AtomicSet(Follower)
			return
		case <-rf.committedCh:
			log.V(0).WithField(strconv.Itoa(rf.me), rf.raftState.AtomicGet()).
				Infoln("receives commit signal, send to applyCh...")
			newCommitIndex := rf.committer.getCommitIndex()
			rf.commitIndex.AtomicSet(int32(newCommitIndex))
			log.V(1).WithField(strconv.Itoa(rf.me), rf.raftState.AtomicGet()).
				WithField("lastApplied", rf.lastApplied).Infoln("before apply...")
			for i := rf.lastApplied + 1; i <= newCommitIndex; i++ {
				log := rf.getLogEntry(i)
				rf.applyCh <- ApplyMsg{Index: log.Index, Command: log.Command}
			}
			rf.lastApplied = newCommitIndex
			log.V(1).WithField(strconv.Itoa(rf.me), rf.raftState.AtomicGet()).
				WithField("lastApplied", rf.lastApplied).Infoln("after apply...")
		}
	}
}
