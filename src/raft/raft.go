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

import "bytes"
import crand "crypto/rand"
import "encoding/gob"
import "fmt"

import "sync"
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

func (r RPCMsg) String() string {
	return fmt.Sprintf("{args: %v, reply: %v}", r.args, r.reply)
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
	electionTimer *time.Timer

	// Channel to receive RPCs. Main loop executes RPC handlers sequentially.
	rpcCh chan *RPCMsg
}

type raftLog struct {
	Logs  map[int]*LogEntry
	first int
	last  int
}

func (l *raftLog) getLogEntry(index int) *LogEntry {
	return l.Logs[index]
}

func (l *raftLog) append(entry *LogEntry) bool {
	if entry.Index < 0 {
		return false
	}

	if _, ok := l.Logs[entry.Index]; ok {
		return false
	}

	if l.first < 0 || entry.Index < l.first {
		l.first = entry.Index
	}
	if entry.Index > l.last {
		l.last = entry.Index
	}

	l.Logs[entry.Index] = entry

	return true
}

func (l *raftLog) lastLogEntry() *LogEntry {
	if len(l.Logs) == 0 {
		return nil
	}

	return l.Logs[l.last]
}

func (l *raftLog) lastIndex() int {
	return l.last
}

// Persistent state on all servers.
type persistentState struct {
	CurrentTerm int
	VotedFor    int
	raftLog
}

func (p *persistentState) persistRaftState(persister *Persister) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if e := encoder.Encode(p); e != nil {
		DPrintf("fail to encode raftState")
		return
	}

	persister.SaveRaftState(buf.Bytes())
}

func (p *persistentState) readRaftState(persister *Persister) {
	var buf bytes.Buffer
	if _, e := buf.Read(persister.ReadRaftState()); e != nil {
		DPrintf("fail to read raftState")
		return
	}
	decoder := gob.NewDecoder(&buf)
	if e := decoder.Decode(p); e != nil {
		DPrintf("fail to encode raftState")
		return
	}
}

func (p *persistentState) truncateLogPrefix(i int) {
	// TODO
}

// Including ith.
func (p *persistentState) truncateLogSuffix(i int) {

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
	// Your code here (2A).
	return rf.CurrentTerm, rf.raftState.AtomicGet() == Leader
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
	defer func() {
		close(rpc.done)
	}()

	curState := rf.raftState.AtomicGet()
	reply.VoteGranted = false
	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		// Reject old request.
		DPrintf("[%v - %v] - reject request vote: reply: %v...\n", rf.me, rf.raftState.AtomicGet(), reply)
		return
	}

	if args.Term == rf.CurrentTerm {
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
	if args.Term > rf.CurrentTerm {
		// update to known latest term, vote for nobody
		rf.CurrentTerm, rf.VotedFor = args.Term, -1
		rf.persistRaftState(rf.persister)
		reply.Term = args.Term
		// fall back to Follower if not
		rf.raftState.AtomicSet(Follower)
		DPrintf("[%v - %v] - a larger Term seen, fall back to Follower...\n",
			rf.me, rf.raftState.AtomicGet())
	}

	if curState != Leader {
		// Stop the timer and defer its restart operation.
		defer rf.resetElectionTimer()()
	}

	// Compare logs.
	last := rf.lastLogEntry()
	if last != nil {
		if last.Term > args.LastLogTerm ||
			(last.Term == args.LastLogTerm && last.Index > args.LastLogIndex) {
			return
		}
	}

	reply.VoteGranted = true
	rf.VotedFor = args.CandidateId
	rf.persistRaftState(rf.persister)

	DPrintf("[%v - %v] - vote granted: %v...\n", rf.me, rf.raftState.AtomicGet(), reply)
	return
}

// Stop the election timer, and return a function which restart the timer.
// If it is already fired, return a non op.
func (rf *Raft) resetElectionTimer() func() {
	if !rf.electionTimer.Stop() {
		// Already timed out, the value in electionTimer.C will be drained in another routine.
		//<-rf.electionTimer.C
		DPrintf("[%v - %v] - election timer already fired (value not drained here)...\n",
			rf.me, rf.raftState.AtomicGet())
		// Return a non op.
		return func() {}
	}
	DPrintf("[%v - %v] - election timer stopped...\n", rf.me, rf.raftState.AtomicGet())
	return func() {
		rf.electionTimer.Reset(randomTimeout(ElectionTimeout))
		DPrintf("[%v - %v] - election timer reset...\n", rf.me, rf.raftState.AtomicGet())
	}
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
	return fmt.Sprintf("{AppendEntriesArgs - Term: %v, LeaderId: %v}", r.Term, r.LeaderId)
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
	defer close(rpc.done)
	curState := rf.raftState.AtomicGet()

	reply.Success = false
	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		return
	}

	if rf.raftState.AtomicGet() != Follower {
		rf.raftState.AtomicSet(Follower)
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		reply.Term = rf.CurrentTerm
	}

	if curState != Leader {
		defer rf.resetElectionTimer()()
	}

	reply.Success = true

	// TODO: handle left part.
	DPrintf("[%v - %v] - args: %v, reply: %v...\n", rf.me, rf.raftState.AtomicGet(), args, reply)
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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = rf.lastIndex()
	term = rf.CurrentTerm
	isLeader = rf.raftState.AtomicGet() == Leader
	// Your code here (2B).
	if !isLeader {
		return
	}

	// start to append log
	prevLog := rf.lastLogEntry()
	log := &LogEntry{Index: rf.lastIndex() + 1, Term: rf.CurrentTerm, Command: command}
	rf.append(log)
	req := &AppendEntriesArgs{
		Term: rf.CurrentTerm, LeaderId: rf.me,
		LeaderCommit: rf.commitIndex, Entires: []*LogEntry{log},
	}
	if prevLog == nil {
		req.PrevLogIndex, req.PrevLogTerm = -1, -1
	} else {
		req.PrevLogIndex, req.PrevLogTerm = prevLog.Index, prevLog.Term
	}

	func() {
		for i, _ := range rf.peers {
			if i != rf.me {
				go func(from, to int) {
					reply := &AppendEntriesReply{}
					DPrintf("[%v - %v] - append logs to %v...\n", from, rf.raftState.AtomicGet(), to)
					if rf.sendAppendEntries(to, req, reply) {
						if reply.Term > rf.CurrentTerm {
							// Fall back to Follower
							// TODO - fall back to Follower
							// DPrintf("[%v - %v] - step down signal sent...\n", rf.me, rf.raftState.AtomicGet())
						}
					}
				}(rf.me, i)
			}
		}
	}()

	return
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
	rf.raftLog = raftLog{Logs: make(map[int]*LogEntry), first: -1, last: -1}
	rf.volatileState = volatileState{-1, -1, Follower}
	rf.leaderVolatileState = leaderVolatileState{make(map[int]int), make(map[int]int)}

	rf.rpcCh = make(chan *RPCMsg)

	go rf.run()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) run() {
	for {
		switch rf.raftState {
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

// Start the election timer.
func (rf *Raft) startElectionTimer(stopCh, timedOutCh chan struct{}) {
	rf.electionTimer = time.NewTimer(randomTimeout(ElectionTimeout))
	defer func() { rf.electionTimer = nil }()

	for {
		select {
		case <-rf.electionTimer.C:
			DPrintf("[%v - %v] - election timed out (value drained)...", rf.me, rf.raftState.AtomicGet())
			// Stop receiving reset signals since we already timed out.
			close(timedOutCh)
			// Exit after parent stopped, since electionTimer could be modified in RPC handler.
			//return
		case <-stopCh:
			DPrintf("[%v - %v] - received stop signal from parent...", rf.me, rf.raftState.AtomicGet())
			return
		}
	}
}

//
// Run RPC handlers in the main loop, receive heart beats in another routine.
//
func (rf *Raft) runFollower() {
	DPrintf("[node: %v] - in runFollower()...", rf.me)
	stopCh, wg := make(chan struct{}), sync.WaitGroup{}
	defer func() {
		close(stopCh)
		wg.Wait()
	}()
	// Monitor election timer in another routine.
	timedOutCh := make(chan struct{})
	goChild(&wg, func() { rf.startElectionTimer(stopCh, timedOutCh) })

	for rf.raftState.AtomicGet() == Follower {
		select {
		case rpc := <-rf.rpcCh:
			DPrintf("[%v - %v] - received a RPC request: %v...\n", rf.me, rf.raftState.AtomicGet(), rpc.args)
			rf.processRPC(rpc)
		case <-timedOutCh:
			DPrintf("[%v - %v] - election timed out, promote to candidate...", rf.me, rf.raftState.AtomicGet())
			rf.raftState.AtomicSet(Candidate)
			return
		}
	}
}

func (rf *Raft) runCandidate() {
	// Tell spawned routines to stop.
	stopCh, wg := make(chan struct{}), sync.WaitGroup{}
	defer func() {
		close(stopCh)
		wg.Wait()
	}()
	// Monitor election timer in another routine.
	timedOutCh := make(chan struct{})
	goChild(&wg, func() { rf.startElectionTimer(stopCh, timedOutCh) })

	electedCh := make(chan struct{})
	rf.CurrentTerm++
	rf.VotedFor = rf.me

	if len(rf.peers) == 1 {
		rf.raftState.AtomicSet(Leader)
		return
	}

	go func() {
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
				close(electedCh)
				return
			}
			select {
			case <-voteCh:
				votes++
			case <-stopCh:
				return
			}
		}
	}()

	for rf.raftState.AtomicGet() == Candidate {
		select {
		case rpc := <-rf.rpcCh:
			DPrintf("[%v - %v] - received a RPC request: %v...\n", rf.me, rf.raftState.AtomicGet(), rpc.args)
			rf.processRPC(rpc)
		case <-electedCh:
			DPrintf("[%v - %v] - got enough votes, promote to Leader...\n", rf.me, rf.raftState.AtomicGet())
			rf.raftState.AtomicSet(Leader)
			return
		case <-timedOutCh:
			// start next round election
			return
		}
	}
}

func (rf *Raft) runLeader() {
	// Tell spawned routines to stop.
	stopCh := make(chan struct{})
	defer close(stopCh)
	stepDownCh := make(chan struct{}, len(rf.peers))

	// Send heart beats.
	go func() {
		for {
			select {
			case <-stopCh:
				return
			case <-time.After(randomTimeout(ElectionTimeout / 10)):
				for i, _ := range rf.peers {
					if i != rf.me {
						go func(from, to int) {
							reply := &AppendEntriesReply{}
							DPrintf("[%v - %v] - send heardbeat to %v...\n",
								from, rf.raftState.AtomicGet(), to)
							if rf.sendAppendEntries(to,
								&AppendEntriesArgs{Term: rf.CurrentTerm, LeaderId: from},
								reply) {
								if reply.Term > rf.CurrentTerm {
									// Fall back to Follower
									stepDownCh <- struct{}{}
									DPrintf("[%v - %v] - step down signal sent...\n",
										rf.me, rf.raftState.AtomicGet())
								}
							}
						}(rf.me, i)
					}
				}
			}
		}
	}()

	for rf.raftState.AtomicGet() == Leader {
		select {
		case rpc := <-rf.rpcCh:
			DPrintf("[%v - %v] - received a RPC request: %v...\n", rf.me, rf.raftState.AtomicGet(), rpc.args)
			rf.processRPC(rpc)
			//default:
			//	DPrintf("[%v - %v] - leader nothing todo...\n", rf.me, rf.raftState.AtomicGet())
		}
	}
}

func (rf *Raft) quorum() int {
	return len(rf.peers)/2 + 1
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
func randomTimeout(minVal time.Duration) time.Duration {
	if minVal == 0 {
		return 0
	}
	extra := (time.Duration(rand.Int63()) % minVal)
	return minVal + extra
}

// Run a child routine, wait it to exist.
func goChild(wg *sync.WaitGroup, f func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		f()
	}()
}
