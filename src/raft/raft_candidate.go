package raft

import (
	"fmt"
	"github.com/jhzhu89/log"
	"raft/util"
	"strconv"
	"time"
)

func (rf *Raft) retrySendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// retry ElectionTimeout/(RPCTimeout/3) times.
	for i := 0; i < 5; i++ {
		_reply := &RequestVoteReply{}
		if rf.sendRequestVote(server, args, _reply) {
			reply.Term = _reply.Term
			reply.VoteGranted = _reply.VoteGranted
			return true
		}
	}

	return false
}

func (rf *Raft) candidateRequestVotes(ctx util.Context, electSig util.Signal) {
	legitimateTerm := int(rf.currentTerm.AtomicGet()) // the term in which I will request votes.
	var votes uint32 = 1
	voteCh := make(chan struct{}, len(rf.peers)-1)
	// Send RequestVote RPC.
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(from, to int) {
				rf.persistentState.RLock()
				last := rf.lastLogEntry()
				var lastLogTerm, lastLogIndex int
				if last != nil {
					lastLogIndex, lastLogTerm = last.Index, last.Term
				}
				rf.persistentState.RUnlock()

				reply := &RequestVoteReply{}
				if rf.retrySendRequestVote(to,
					&RequestVoteArgs{Term: legitimateTerm, CandidateId: from,
						LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm},
					reply) {
					if reply.VoteGranted {
						log.V(1).F(strconv.Itoa(rf.me), fmt.Sprintf("%v, %v",
							rf.state.AtomicGet(), legitimateTerm)).
							F("voter", to).Infoln("got vote...")
						voteCh <- struct{}{}
					}
				} else {
					log.F(strconv.Itoa(rf.me), "unknown_state").
						F("send_to", to).Warningln("sendRequestVote RPC failed...")
				}
			}(rf.me, i)
		}
	}

	for {
		if votes >= uint32(rf.quorum()) {
			// Got enough votes.
			electSig.Send()
			return
		}
		select {
		case <-voteCh:
			votes++
		case <-ctx.Done():
			return
		}
	}
}

func (rf *Raft) runCandidate() {
	// init resources
	rf.committedCh = make(chan struct{}, 1)
	rg, donef := util.NewRoutineGroup()

	defer func() {
		donef()
		rf.committedCh = nil
	}()

	electSig := util.NewSignal()
	rf.currentTerm.AtomicAdd(1)
	rf.votedFor = rf.me

	if len(rf.peers) == 1 {
		rf.state.AtomicSet(Leader)
		return
	}

	rg.GoFunc(func(ctx util.Context) { rf.candidateRequestVotes(ctx, electSig) })
	rg.GoFunc(func(ctx util.Context) {
		applyLogEntries(ctx, rf, func() int { return int(rf.commitIndex.AtomicGet()) })
	})
	rg.GoFunc(func(ctx util.Context) { rejectAppendMsg(rf, ctx) })

	rf.electTimer = time.NewTimer(randomTimeout(ElectionTimeout))
	defer func() { rf.electTimer = nil }()
	for rf.state.AtomicGet() == Candidate {
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
		case <-electSig.Received():
			log.V(1).F(strconv.Itoa(rf.me), fmt.Sprintf("%v, %v",
				rf.state.AtomicGet(), rf.currentTerm.AtomicGet())).
				Infoln("got enough votes, promote to Leader...")
			rf.state.AtomicSet(Leader)
			return
		case <-rf.electTimer.C:
			// start next round election
			return
		case <-rf.stopCh:
			return
		}
	}
}
