package raft

import (
	"fmt"
	"github.com/jhzhu89/log"
	"raft/util"
	"strconv"
)

const MaxAppend = 1024

func replicate(rf *Raft, appMsgs []*AppendMsg, term int) {
	var entries []*LogEntry
	rf.persistentState.Lock()
	for _, appMsg := range appMsgs {
		appMsg.isLeader = true
		l := &appMsg.LogEntry
		l.Index = rf.lastIndex() + 1
		l.Term = term
		if rf.appendOne(l) == false {
			panic(fmt.Sprintf("failed to append %v to raft log", l))
		}
		close(appMsg.done)
		entries = append(entries, l)
	}
	rf.committer.addLogs(entries)
	rf.persistRaftState(rf.persister)
	rf.persistentState.Unlock()

	// 2. replicate to others
	for _, repl := range rf.replicators {
		repl.Replicate()
	}
}

func leaderHandleAppendMsg(raft *Raft, ctx util.Context) {
	defer func() {
		// Reject the remaining appendMsg(s) in appendCh.
		for {
			select {
			case msg := <-raft.appendCh:
				msg.isLeader = false
				msg.Index = -1
				msg.Term = int(raft.currentTerm.AtomicGet())
				close(msg.done)
			default:
				return
			}
		}
	}()

	// the term in which I am the leader to replicate logs.
	legitimateTerm := int(raft.currentTerm.AtomicGet())
	logV2 := log.V(2).F(strconv.Itoa(raft.me), fmt.Sprintf("%v, %v",
		raft.state.AtomicGet(), legitimateTerm))
	for raft.state.AtomicGet() == Leader {
		var appMsgs []*AppendMsg
		select {
		case msg := <-raft.appendCh:
			logV2.Clone().F("app", msg).Infoln("received an append msg...")
			appMsgs = append(appMsgs, msg)
			for i := 0; i < MaxAppend-1; i++ {
				select {
				case msg := <-raft.appendCh:
					logV2.Clone().F("app", msg).Infoln("received an append msg...")
					appMsgs = append(appMsgs, msg)
				default:
					break
				}
			}
			replicate(raft, appMsgs, int(legitimateTerm))
		case <-ctx.Done():
			return
		}
	}
}

func (rf *Raft) runLeader() {
	// init resources
	rf.committedCh = make(chan struct{}, 1)
	rf.committer = newCommitter(rf.committedCh)
	rf.committer.quoromSize = rf.quorum()
	rg, donef := util.NewRoutineGroup()
	stepDown := util.NewSignal()
	rf.replicators = make(map[int]*replicator, 0)
	for i, _ := range rf.peers {
		if i != rf.me {
			rf.replicators[i] = newReplicator(rg, stepDown, rf, rf.me, i)
		}
	}

	defer func() {
		log.V(2).Fs(strconv.Itoa(rf.me), fmt.Sprintf("%v, %v",
			rf.state.AtomicGet(), rf.currentTerm.AtomicGet())).
			Infoln("start to cancel all routines...")
		donef()
		log.V(2).Fs(strconv.Itoa(rf.me), fmt.Sprintf("%v, %v",
			rf.state.AtomicGet(), rf.currentTerm.AtomicGet())).
			Infoln("finish to cancel all routines...")
		rf.replicators = nil
		rf.committer = nil
		rf.committedCh = nil
	}()

	rg.GoFunc(func(ctx util.Context) {
		applyLogEntries(ctx, rf, func() int {
			newCommitIndex := rf.committer.getCommitIndex()
			rf.commitIndex.AtomicSet(int32(newCommitIndex))
			return newCommitIndex
		})
	})
	rg.GoFunc(func(ctx util.Context) { leaderHandleAppendMsg(rf, ctx) })

	for rf.state.AtomicGet() == Leader {
		select {
		case rpc := <-rf.appEntRpcCh:
			log.V(2).Fs(strconv.Itoa(rf.me), fmt.Sprintf("%v, %v",
				rf.state.AtomicGet(), rf.currentTerm.AtomicGet()),
				"rpc", rpc.args).Infoln("received an append entries request...")
			rf.handleAppendEntries(rpc)
		case rpc := <-rf.reqVoteRpcCh:
			log.V(2).Fs(strconv.Itoa(rf.me), fmt.Sprintf("%v, %v",
				rf.state.AtomicGet(), rf.currentTerm.AtomicGet()),
				"rpc", rpc.args).Infoln("received a request vote request...")
			rf.handleRequestVote(rpc)
		case <-stepDown.Received():
			return
		case <-rf.stopCh:
			return
		}
	}
}
