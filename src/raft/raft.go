package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"labrpc"
	"raft/util"
	"time"
)

// import "bytes"
// import "encoding/gob"

const (
	None RaftState = iota
	Follower
	Candidate
	Leader
)

const ElectionTimeout = 1000 * time.Millisecond

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// The log entry.
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// RPC message.
type RPCMsg struct {
	args  interface{}
	reply interface{}
	done  chan struct{}
}

func (r RPCMsg) String() string {
	return fmt.Sprintf("{args: %v, reply: %v}", r.args, r.reply)
}

// Append message.
type AppendMsg struct {
	LogEntry
	isLeader bool
	done     chan struct{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	//mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	persistentState
	volatileState
	leaderVolatileState

	rpcCh    chan *RPCMsg    // Channel to receive RPCs.
	appendCh chan *AppendMsg // Channel to receive logs.
	applyCh  chan ApplyMsg

	electionTimer *time.Timer
	timerRH       util.ResourceHolder // Hold and start a timer.

	committedCh   chan struct{}
	committedChRH util.ResourceHolder // Create and release the channel.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return int(rf.CurrentTerm.AtomicGet()), rf.raftState.AtomicGet() == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

func (r *RequestVoteArgs) String() string {
	return fmt.Sprintf("{RequestVoteArgs - Term: %v, CandidateId: %v, LastLogIndex: %v, LastLogTerm: %v}",
		r.Term, r.CandidateId, r.LastLogIndex, r.LastLogTerm)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (r *RequestVoteReply) String() string {
	return fmt.Sprintf("{RequestVoteReply - Term: %v, VoteGranted: %v}", r.Term, r.VoteGranted)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Write message into RPC chan.
	done := make(chan struct{})
	rf.rpcCh <- &RPCMsg{args, reply, done}
	<-done
}

//
// The real handler.
//
func (rf *Raft) handleRequestVote(rpc *RPCMsg) {
	args := rpc.args.(*RequestVoteArgs)
	reply := rpc.reply.(*RequestVoteReply)
	nextState := rf.raftState.AtomicGet()
	defer func() {
		close(rpc.done)
		if rf.raftState.AtomicGet() != nextState {
			DPrintf("[%v - %v] - state change from %v to %v...\n", rf.me, rf.raftState.AtomicGet(),
				rf.raftState.AtomicGet(), nextState)
			rf.raftState.AtomicSet(nextState)
		}
	}()

	reply.VoteGranted = false
	currentTerm := int(rf.CurrentTerm.AtomicGet())
	reply.Term = currentTerm

	if args.Term < currentTerm {
		// Reject old request.
		DPrintf("[%v - %v] - reject request vote: reply: %v...\n", rf.me, rf.raftState.AtomicGet(), reply)
		return
	}

	if args.Term == currentTerm {
		if rf.VotedFor == args.CandidateId {
			// Duplicate request.
			reply.VoteGranted = true
			return
		} else if rf.VotedFor > 0 && rf.VotedFor != args.CandidateId {
			DPrintf("[%v - %v] - already voted to %v - reject request vote: reply: %v...\n",
				rf.me, rf.raftState.AtomicGet(), rf.VotedFor, reply)
			return
		}
	}

	// Have not voted yet.
	if args.Term > currentTerm {
		// update to known latest term, vote for nobody
		rf.CurrentTerm.AtomicSet(int32(args.Term))
		rf.VotedFor = -1
		rf.persistRaftState(rf.persister)
		reply.Term = args.Term
		if rf.raftState.AtomicGet() != Follower {
			nextState = Follower
			DPrintf("[%v - %v] - a larger Term seen, will fall back to Follower...\n",
				rf.me, rf.raftState.AtomicGet())
		}
	}

	if rf.raftState.AtomicGet() != Leader {
		if rf.electionTimer.Stop() {
			DPrintf("[%v - %v] - election timer stopped...\n", rf.me, rf.raftState.AtomicGet())
			defer func() {
				rf.electionTimer.Reset(randomTimeout(ElectionTimeout))
				DPrintf("[%v - %v] - election timer reset...\n", rf.me, rf.raftState.AtomicGet())
			}()
		}
	}

	// Compare logs.
	last := rf.lastLogEntry()
	if last != nil {
		if last.Term > args.LastLogTerm ||
			(last.Term == args.LastLogTerm && last.Index > args.LastLogIndex) {
			DPrintf("[%v - %v] - vote not granted: %v...\n", rf.me, rf.raftState.AtomicGet(), reply)
			DPrintf("[%v - %v] - last: %v, args: %v...\n", rf.me, rf.raftState.AtomicGet(), last, args)
			return
		}
	}

	reply.VoteGranted = true
	rf.VotedFor = args.CandidateId
	rf.persistRaftState(rf.persister)

	DPrintf("[%v - %v] - vote granted: %v...\n", rf.me, rf.raftState.AtomicGet(), reply)
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// AppendEntries RPC request.
//
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entires      []*LogEntry
	LeaderCommit int
}

func (r *AppendEntriesArgs) String() string {
	return fmt.Sprintf("{AppendEntriesArgs - Term: %v, LeaderId: %v, PrevLogIndex: %v, PrevLogTerm: %v,"+
		" LeaderCommit: %v", r.Term, r.LeaderId, r.PrevLogIndex, r.PrevLogTerm, r.LeaderCommit)
}

//
// AppendEntries RPC reply.
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func (r *AppendEntriesReply) String() string {
	return fmt.Sprintf("{AppendEntriesReply - Term: %v, Success: %v}", r.Term, r.Success)
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	done := make(chan struct{})
	rf.rpcCh <- &RPCMsg{args, reply, done}
	<-done
}

//
// The real handler.
//
func (rf *Raft) handleAppendEntries(rpc *RPCMsg) {
	args := rpc.args.(*AppendEntriesArgs)
	reply := rpc.reply.(*AppendEntriesReply)
	nextState := rf.raftState.AtomicGet()
	defer func() {
		close(rpc.done)
		if rf.raftState.AtomicGet() != nextState {
			DPrintf("[%v - %v] - state change from %v to %v...\n", rf.me, rf.raftState.AtomicGet(),
				rf.raftState.AtomicGet(), nextState)
			rf.raftState.AtomicSet(nextState)
		}
	}()

	reply.Success = false
	currentTerm := int(rf.CurrentTerm.AtomicGet())
	reply.Term = currentTerm

	if args.Term < currentTerm {
		return
	}

	if rf.raftState.AtomicGet() != Follower {
		nextState = Follower
	}

	if args.Term > currentTerm {
		rf.CurrentTerm.AtomicSet(int32(args.Term))
		reply.Term = args.Term
	}

	if rf.raftState.AtomicGet() != Leader {
		if rf.electionTimer.Stop() {
			defer rf.electionTimer.Reset(randomTimeout(ElectionTimeout))
		}
	}

	// save the log on disk and send to applyCh
	if len(args.Entires) > 0 {
		if args.PrevLogIndex > 0 {
			prevLog := rf.getLogEntry(args.PrevLogIndex)
			if prevLog == nil || prevLog.Term != args.PrevLogTerm {
				return
			}
		}

		rf.raftLog.Lock()
		rf.removeSuffix(args.Entires[0].Index)
		for _, l := range args.Entires {
			rf.append(l)
		}
		rf.raftLog.Unlock()
	}

	reply.Success = true

	commitIndex, setTo := int(rf.commitIndex.AtomicGet()), int(0)
	if commitIndex < args.LeaderCommit {
		setTo = args.LeaderCommit
		if args.LeaderCommit > rf.lastIndex() {
			rf.raftLog.Lock()
			setTo = rf.lastIndex()
			rf.raftLog.Unlock()
		}
		rf.commitIndex.AtomicSet(int32(setTo))

		select {
		case rf.committedCh <- struct{}{}:
		default:
		}
	}

	if len(args.Entires) > 0 {
		DPrintf("[%v - %v] - appendEntries: args: %v, reply: %v...\n", rf.me,
			rf.raftState.AtomicGet(), args, reply)
	}
}

//
// example code to send a AppendEntries RPC to a server.
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// RPC handler dispatcher.
//
func (rf *Raft) processRPC(rpc *RPCMsg) {
	if rpc.args == nil || rpc.reply == nil || rpc.done == nil {
		DPrintf("[%v - %v] - RPCMsg is invalid: %v...", rf.me, rf.raftState.AtomicGet(), rpc)
		return
	}

	switch rpc.args.(type) {
	case *RequestVoteArgs:
		rf.handleRequestVote(rpc)
	case *AppendEntriesArgs:
		rf.handleAppendEntries(rpc)
	default:
		DPrintf("[%v - %v] - unknown RPC message: %v...", rf.me, rf.raftState.AtomicGet(), rpc)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Send a log to the leader's appendCh
	// Your code here (2B).
	if rf.raftState.AtomicGet() != Leader {
		rf.raftLog.Lock()
		idx := rf.lastIndex()
		rf.raftLog.Unlock()
		return idx + 1, int(rf.CurrentTerm.AtomicGet()), false
	}
	msg := &AppendMsg{
		LogEntry{Command: command},
		true,
		make(chan struct{}),
	}

	rf.appendCh <- msg
	<-msg.done
	return msg.Index, msg.Term, msg.isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.VotedFor = -1
	rf.raftLog = *newRaftLog()
	rf.volatileState = volatileState{0, 0, Follower}

	rf.rpcCh = make(chan *RPCMsg)
	rf.appendCh = make(chan *AppendMsg)
	rf.applyCh = applyCh

	rf.timerRH = util.ResourceHolder(
		func(r interface{}) util.Releaser {
			t := r.(**time.Timer)
			*t = time.NewTimer(randomTimeout(ElectionTimeout))
			return func() { *t = nil }
		})
	rf.committedChRH = util.ResourceHolder(
		func(r interface{}) util.Releaser {
			ch := r.(*chan struct{})
			*ch = make(chan struct{}, 1)
			return func() { *ch = nil }
		})

	go rf.run()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) run() {
	for {
		switch rf.raftState.AtomicGet() {
		case Follower:
			DPrintf("[%v - %v] - start to run as follower...", rf.me, rf.raftState.AtomicGet())
			rf.runFollower()
		case Candidate:
			DPrintf("[%v - %v] - start to run as candidate...", rf.me, rf.raftState.AtomicGet())
			rf.runCandidate()
		case Leader:
			DPrintf("[%v - %v] - start to run as leader...", rf.me, rf.raftState.AtomicGet())
			rf.runLeader()
		default:
			DPrintf("[%v - %v] - unexpected state: %v", rf.me, rf.raftState.AtomicGet())
			return
		}
	}
}

func (rf *Raft) quorum() int {
	return len(rf.peers)/2 + 1
}
