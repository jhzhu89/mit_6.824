package raft

import (
	"raft/util"
)

func (rf *Raft) candidateRequestVotes(stopper util.Stopper, electSig util.Signal) {
	var votes uint32 = 1
	voteCh := make(chan struct{}, len(rf.peers)-1)
	// Send RequestVote RPC.
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(from, to int) {
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(to,
					&RequestVoteArgs{Term: rf.CurrentTerm, CandidateId: from},
					reply) {
					if reply.VoteGranted {
						voteCh <- struct{}{}
					}
				} else {
					DPrintf("[%v - %v] - sendRequestVote to peer %v RPC failed...\n",
						rf.me, rf.raftState.AtomicGet(), to)
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
		case <-stopper.Stopped():
			return
		}
	}
}

func (rf *Raft) runCandidate() {
	// Tell spawned routines to stop.
	stopper, stopf := util.WithStop()
	defer stopf()

	electSig := util.NewSignal()
	rf.CurrentTerm++
	rf.VotedFor = rf.me

	if len(rf.peers) == 1 {
		rf.raftState.AtomicSet(Leader)
		return
	}

	goFunc(func() { rf.candidateRequestVotes(stopper, electSig) })
	// Start the timer
	defer rf.timerRH(&rf.electionTimer)()

	for rf.raftState.AtomicGet() == Candidate {
		select {
		case rpc := <-rf.rpcCh:
			DPrintf("[%v - %v] - received a RPC request: %v...\n", rf.me, rf.raftState.AtomicGet(), rpc.args)
			rf.processRPC(rpc)
		case <-electSig.Received():
			DPrintf("[%v - %v] - got enough votes, promote to Leader...\n", rf.me, rf.raftState.AtomicGet())
			rf.raftState.AtomicSet(Leader)
			return
		case <-rf.electionTimer.C:
			// start next round election
			return
		}
	}
}
