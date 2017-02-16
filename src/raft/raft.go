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

import crand "crypto/rand"
import "fmt"

//import "sync"
import "labrpc"
import "math"
import "math/big"
import "math/rand"
import "sync/atomic"
import "time"

// import "bytes"
// import "encoding/gob"

func init() {
	// Ensure we use a high-entropy seed for the psuedo-random generator
	rand.Seed(newSeed())
}

type RaftState uint32

func (rs *RaftState) AtomicGet() RaftState {
	return RaftState(atomic.LoadUint32((*uint32)(rs)))
}

func (rs *RaftState) AtomicSet(v RaftState) {
	atomic.StoreUint32((*uint32)(rs), uint32(v))
}

func (rs RaftState) String() string {
	switch rs {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Invalid"
	}
}

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

	//
	resetElectionTimerCh chan struct{}

	// Channel to receive RPCs. Main loop executes RPC handlers sequentially.
	rpcCh chan RPCMsg
}

// Persistent state on all servers.
type persistentState struct {
	currentTerm int
	votedFor    int
	logs        []*LogEntry
}

// Volatile state on all servers.
type volatileState struct {
	commitIndex int
	lastApplied int
	raftState   RaftState
}

// Volatile state on leaders.
type leaderVolatileState struct {
	nextIndex  map[int]int // key: server id, val: the index
	matchIndex map[int]int // key: server id, val: the index
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
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

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Write message into RPC chan.
	done := make(chan struct{})
	rf.rpcCh <- RPCMsg{args, reply, done}
	<-done
}

//
// The real handler.
//
func (rf *Raft) handleRequestVote(rpc RPCMsg) {
	args := rpc.args.(*RequestVoteArgs)
	reply := rpc.reply.(*RequestVoteReply)
	defer close(rpc.done)

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		// Reject old request.
		return
	}

	// TODO
	//if args.Term == rf.currentTerm {
	//	switch {
	//	case rf.votedFor < 0:
	//	case rf.votedFor == args.CandidateId:
	//		DPrintf("[%v - %v] - duplicate RequestVote request%v...", rf.me, rf.raftState.AtomicGet(), rpc)
	//		reply.VoteGranted = true
	//		return
	//	case rf.votedFor > 0 && rf.votedFor != args.CandidateId:
	//		reply.VoteGranted = false
	//		return
	//	}
	//}

	if rf.votedFor < 0 {
		if args.LastLogIndex > rf.lastIndex() ||
			(args.LastLogIndex == rf.lastIndex() && args.Term > rf.currentTerm) {
			reply.VoteGranted = true
			rf.currentTerm = args.Term
		}
	}

	DPrintf("%v - %v", args, reply)

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
}

//
// AppendEntries RPC reply.
//
type AppendEntriesReply struct {
	// Your data here (2A).
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	done := make(chan struct{})
	rf.rpcCh <- RPCMsg{args, reply, done}
	<-done
}

//
// The real handler.
//
func (rf *Raft) handleAppendEntries(rpc RPCMsg) {
	args := rpc.args.(*AppendEntriesArgs)
	reply := rpc.reply.(*AppendEntriesReply)

	DPrintf("%v - %v", args, reply)

	close(rpc.done)
}

//
// example code to send a RequestVote RPC to a server.
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// RPC handler dispatcher.
//
func (rf *Raft) processRPC(rpc RPCMsg) {
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	rf.votedFor = -1
	rf.logs = make([]*LogEntry, 0)
	rf.raftState = Follower
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)

	rf.rpcCh = make(chan RPCMsg)

	go rf.run()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) run() {
	for {
		switch rf.raftState {
		case Follower:
			DPrintf("[node: %v] - start to run as follower...", rf.me)
			rf.runFollower()
		case Candidate:
			DPrintf("[node: %v] - start to run as candidate...", rf.me)
			rf.runCandidate()
		case Leader:
			DPrintf("[node: %v] - start to run as leader...", rf.me)
			rf.runLeader()
		default:
			DPrintf("[node: %v] - unexpected state: %v", rf.me, rf.raftState)
			return
		}
	}
}

//
// Run RPC handlers in the main loop, receive heart beats in another routine.
//
func (rf *Raft) runFollower() {
	stopCh := make(chan struct{})
	go func() {
		rf.resetElectionTimerCh = make(chan struct{})
		for {
			select {
			case <-randomTimeout(ElectionTimeout):
				stopCh <- struct{}{}
				// Stop receiving reset signals since we already timed out.
				close(rf.resetElectionTimerCh)
				return
			case <-rf.resetElectionTimerCh:
				// Once a signal received from this chan, current timer returned by randomTimeout(...)
				// is not fired; in the next loop, randomTimeout(...) generates a new timer.
				DPrintf("[node: %v] - reseting the election timer...", rf.me)
			}
		}
	}()

	for rf.raftState.AtomicGet() == Follower {
		select {
		case rpc := <-rf.rpcCh:
			DPrintf("[%v - %v] - received a RPC request: %v...\n", rf.me, rf.raftState.AtomicGet(), rpc)
			rf.processRPC(rpc)
		case <-stopCh:
			DPrintf("[%v - %v] - election timed out, promote to candidate...", rf.me, rf.raftState.AtomicGet())
			rf.raftState.AtomicSet(Candidate)
			return
		}
	}
}

func (rf *Raft) runCandidate() {
	for rf.raftState.AtomicGet() == Candidate {
		select {
		case <-time.After(time.Second * 1):
			DPrintf("[%v - %v] - election timeout...", rf.me, rf.raftState.AtomicGet())
		}
	}
}

func (rf *Raft) runLeader() {
	for rf.raftState.AtomicGet() == Leader {
		select {
		case <-time.After(time.Second * 1):
			DPrintf("[node: %v] - election timeout...", rf.me)
		}
	}
}

func (rf *Raft) lastIndex() int {
	if len(rf.logs) == 0 {
		return -1
	}

	return rf.logs[len(rf.logs)-1].Index
}

//
// Util funcs.
//
// returns an int64 from a crypto random source
// can be used to seed a source for a math/rand.
func newSeed() int64 {
	r, err := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		panic(fmt.Errorf("failed to read random bytes: %v", err))
	}
	return r.Int64()
}

// randomTimeout returns a value that is between the minVal and 2x minVal.
func randomTimeout(minVal time.Duration) <-chan time.Time {
	if minVal == 0 {
		return nil
	}
	extra := (time.Duration(rand.Int63()) % minVal)
	return time.After(minVal + extra)
}
