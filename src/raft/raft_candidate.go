package raft

import (
	"github.com/jhzhu89/log"
	"raft/util"
	"strconv"
)

func (rf *Raft) candidateRequestVotes(ctx util.CancelContext, electSig util.Signal) {
	var votes uint32 = 1
	voteCh := make(chan struct{}, len(rf.peers)-1)
	// Send RequestVote RPC.
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(from, to int) {
				reply := &RequestVoteReply{}
				last := rf.lastLogEntry()
				var lastLogTerm, lastLogIndex int
				if last != nil {
					lastLogIndex, lastLogTerm = last.Index, last.Term
				}
				if rf.sendRequestVote(to,
					&RequestVoteArgs{Term: int(rf.CurrentTerm.AtomicGet()), CandidateId: from,
						LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm},
					reply) {
					if reply.VoteGranted {
						log.V(0).WithField(strconv.Itoa(rf.me), rf.state.AtomicGet()).
							WithField("voter", to).Infoln("got vote...")
						voteCh <- struct{}{}
					}
				} else {
					log.WithField(strconv.Itoa(rf.me), "unknown_state").
						WithField("send_to", to).Warningln("sendRequestVote RPC failed...")
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
	electSig := util.NewSignal()
	rf.CurrentTerm.AtomicAdd(1)
	rf.VotedFor = rf.me

	if len(rf.peers) == 1 {
		rf.state.AtomicSet(Leader)
		return
	}

	rg := util.NewRoutineGroup()
	rg.GoFunc(func(ctx util.CancelContext) { rf.candidateRequestVotes(ctx, electSig) })
	defer rf.committedChH(&rf.committedCh)()
	rg.GoFunc(func(ctx util.CancelContext) { applyLogEntries(ctx, rf) })
	// Start the timer
	defer rf.timerH(&rf.electTimer)()
	defer rg.Done()

	for rf.state.AtomicGet() == Candidate {
		select {
		case rpc := <-rf.rpcCh:
			rf.processRPC(rpc)
		case <-electSig.Received():
			log.V(0).WithField(strconv.Itoa(rf.me), rf.state.AtomicGet()).
				Infoln("got enough votes, promote to Leader...")
			rf.state.AtomicSet(Leader)
			return
		case <-rf.electTimer.C:
			// start next round election
			return
		}
	}
}
