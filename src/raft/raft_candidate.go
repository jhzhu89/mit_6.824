package raft

import (
	"raft/util"
)

func (rf *Raft) candidateRequestVotes(canceller util.Canceller, electSig util.Signal) {
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
						voteCh <- struct{}{}
					}
				} else {
					DPrintf("[%v - unsure] - sendRequestVote to peer %v RPC failed...\n", rf.me, to)
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
		case <-canceller.Cancelled():
			return
		}
	}
}

func (rf *Raft) runCandidate() {
	electSig := util.NewSignal()
	rf.CurrentTerm.AtomicAdd(1)
	rf.VotedFor = rf.me

	if len(rf.peers) == 1 {
		rf.raftState.AtomicSet(Leader)
		return
	}

	rgm := util.NewRoutineGroupMonitor()
	rgm.GoFunc(func(canceller util.Canceller) { rf.candidateRequestVotes(canceller, electSig) })
	defer rf.committedChRH(&rf.committedCh)()
	rgm.GoFunc(func(canceller util.Canceller) { applyLogEntries(canceller, rf) })
	// Start the timer
	defer rf.timerRH(&rf.electionTimer)()
	defer rgm.Done()

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
