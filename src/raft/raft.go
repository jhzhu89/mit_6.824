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
	"strconv"
	"time"

	"github.com/jhzhu89/log"
)

// import "bytes"
// import "encoding/gob"

const (
	None RaftState = iota
	Follower
	Candidate
	Leader
)

const ElectionTimeout = 667 * time.Millisecond
const RPCTimeout = ElectionTimeout / 2
const HeartbeatTimeout = ElectionTimeout / 20

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

func (a AppendMsg) String() string {
	return fmt.Sprintf("{LogEntry: %v, isLeader: %v}", a.LogEntry, a.isLeader)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	persistentState

	// Volatile state on all servers.
	commitIndex Int32 // need to read/write commitIndex atomically.
	lastApplied int
	state       RaftState

	// Volatile state on leaders.
	replicators map[int]*replicator
	committer   *committer

	appEntRpcCh chan *RPCMsg // Channel to receive RPCs.
	// appendEntriesRPC may block request vote rpc.
	// Let reqest vote rpc goes into its own channel, so that it can be processed more quickly.
	reqVoteRpcCh chan *RPCMsg

	appendCh chan *AppendMsg // Channel to receive logs.
	applyCh  chan ApplyMsg

	electTimer *time.Timer // Election timer.

	committedCh chan struct{}
	stopCh      chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return int(rf.currentTerm.AtomicGet()), rf.state.AtomicGet() == Leader
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
	// rf.persistentState.RLock()
	// rf.persistRaftState(rf.persister)
	// rf.persistentState.RUnlock()
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
	// if data == nil || len(data) < 1 { // bootstrap without any state?
	// 	return
	// }
	rf.readRaftState(rf.persister)
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
	return fmt.Sprintf("{Term: %v, CandidateId: %v, LastLogIndex: %v, LastLogTerm: %v}",
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
	return fmt.Sprintf("{Term: %v, VoteGranted: %v}", r.Term, r.VoteGranted)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Write message into RPC chan.
	done := make(chan struct{})
	rf.reqVoteRpcCh <- &RPCMsg{args, reply, done}
	<-done
}

//
// The real handler.
//
// when transisting state, change state first, then term.
func (rf *Raft) handleRequestVote(rpc *RPCMsg) {
	defer close(rpc.done)

	args := rpc.args.(*RequestVoteArgs)
	reply := rpc.reply.(*RequestVoteReply)
	logV1 := log.V(1).F(strconv.Itoa(rf.me), fmt.Sprintf("%v, %v",
		rf.state.AtomicGet(), rf.currentTerm.AtomicGet()))

	reply.VoteGranted = false
	currentTerm := int(rf.currentTerm.AtomicGet())
	reply.Term = currentTerm

	if args.Term < currentTerm {
		// Reject old request.
		logV1.Clone().F("reply", reply).Infoln("reject request vote since its term is old")
		return
	}

	if args.Term == currentTerm {
		if rf.votedFor == args.CandidateId {
			// Duplicate request.
			reply.VoteGranted = true
			return
		} else if rf.votedFor >= 0 && rf.votedFor != args.CandidateId { // server id could be 0.
			logV1.Clone().F("vote_to", rf.votedFor).F("reply", reply).
				Infoln("reject request vote since already voted...")
			return
		}
	}

	rf.persistentState.Lock()
	defer rf.persistentState.Unlock()
	// Have not voted yet.
	if args.Term > currentTerm {
		if rf.state.AtomicGet() != Follower {
			logV1.Clone().Infoln("a larger Term seen, fall back to Follower...")
			rf.state.AtomicSet(Follower)
		}
		// update to known latest term, vote for nobody
		rf.currentTerm.AtomicSet(int32(args.Term))
		rf.votedFor = -1
		rf.persistRaftState(rf.persister)
		reply.Term = args.Term
	}

	// Compare logs.
	last := rf.lastLogEntry()
	if last != nil {
		if last.Term > args.LastLogTerm ||
			(last.Term == args.LastLogTerm && last.Index > args.LastLogIndex) {
			logV1.Clone().F("reply", reply).F("last", last).
				F("args", args).Infoln("vote not granted...")
			return
		}
	}

	if rf.electTimer != nil {
		if rf.electTimer.Stop() {
			defer rf.electTimer.Reset(randomTimeout(ElectionTimeout))
		}
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.persistRaftState(rf.persister)
	logV1.Clone().F("reply", reply).Infoln("vote granted...")
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
	logV1 := log.V(1).F(strconv.Itoa(rf.me), fmt.Sprintf("%v, %v",
		rf.state.AtomicGet(), rf.currentTerm.AtomicGet()))
	done := make(chan bool)
	goFunc(func() {
		//logV1.Clone().WithField("to", server).Infoln("sending RequestVote RPC...")
		select {
		case done <- rf.peers[server].Call("Raft.RequestVote", args, reply):
		default:
		}
	})

	select {
	case ok := <-done:
		return ok
	case <-time.After(RPCTimeout / 3): // need to retry quickly
		logV1.Clone().F("to", server).Infoln("sendRequestVote timed out...")
		return false
	}
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
	return fmt.Sprintf("{Term: %v, LeaderId: %v, PrevLogIndex: %v, PrevLogTerm: %v,"+
		" LeaderCommit: %v}", r.Term, r.LeaderId, r.PrevLogIndex, r.PrevLogTerm, r.LeaderCommit)
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
	return fmt.Sprintf("{Term: %v, Success: %v}", r.Term, r.Success)
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	done := make(chan struct{})
	rf.appEntRpcCh <- &RPCMsg{args, reply, done}
	<-done
}

//
// The real handler.
//
func (rf *Raft) handleAppendEntries(rpc *RPCMsg) {
	defer close(rpc.done)
	args := rpc.args.(*AppendEntriesArgs)
	reply := rpc.reply.(*AppendEntriesReply)

	logV1 := log.V(1).F(strconv.Itoa(rf.me), fmt.Sprintf("%v, %v",
		rf.state.AtomicGet(), rf.currentTerm.AtomicGet()))
	logV2 := log.V(2).F(strconv.Itoa(rf.me), fmt.Sprintf("%v, %v",
		rf.state.AtomicGet(), rf.currentTerm.AtomicGet()))
	reply.Success = false
	currentTerm := int(rf.currentTerm.AtomicGet())
	reply.Term = currentTerm
	if args.Term < currentTerm {
		logV2.Fs("reply.Term", reply.Term, "cur.Term", currentTerm).Infoln("reject old message...")
		return
	}

	if args.Term > currentTerm {
		if rf.state.AtomicGet() != Follower {
			logV1.Clone().Infoln("a larger Term seen, will fall back to Follower...")
			rf.state.AtomicSet(Follower)
		}
		rf.currentTerm.AtomicSet(int32(args.Term))
		reply.Term = args.Term
	}

	if rf.electTimer != nil {
		if rf.electTimer.Stop() {
			defer rf.electTimer.Reset(randomTimeout(ElectionTimeout))
		}
	}

	rf.persistentState.Lock()
	defer rf.persistentState.Unlock()
	// save the log on disk and send to applyCh
	// need to check prev log even when args.Entries is empty.
	if args.PrevLogIndex > 0 {
		prevLog := rf.getLogEntry(args.PrevLogIndex)
		if prevLog == nil ||
			prevLog.Term != args.PrevLogTerm ||
			prevLog.Index != args.PrevLogIndex {
			logV2.Fs("prevLog", prevLog, "args.PrevLogTerm", args.PrevLogTerm,
				"args.PrevLogIndex", args.PrevLogIndex).Infoln("prev mismatch...")
			return
		}
	}

	var lastIndex int
	if len(args.Entires) > 0 {
		rf.appendLogs(args.Entires)
		lastIndex = rf.lastIndex()
		rf.persistRaftState(rf.persister)
	}

	reply.Success = true

	commitIndex := int(rf.commitIndex.AtomicGet())
	if commitIndex < args.LeaderCommit {
		setTo := args.LeaderCommit
		if lastIndex == 0 {
			lastIndex = rf.lastIndex()
		}
		if args.LeaderCommit > lastIndex {
			setTo = lastIndex
		}
		rf.commitIndex.AtomicSet(int32(setTo))

		select {
		case rf.committedCh <- struct{}{}:
		default:
		}
	}

	if len(args.Entires) > 0 {
		logV1.Clone().F("args", args).F("reply", reply).Infoln("appendEntries...")
	} else {
		logV2.Clone().F("args", args).F("reply", reply).Infoln("heartbeat...")
	}
}

//
// example code to send a AppendEntries RPC to a server.
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	done := make(chan bool)
	goFunc(func() {
		select {
		case done <- rf.peers[server].Call("Raft.AppendEntries", args, reply):
		default:
		}
	})

	select {
	case ok := <-done:
		return ok
	case <-time.After(RPCTimeout):
		log.V(1).F(strconv.Itoa(rf.me), fmt.Sprintf("%v, %v", rf.state.AtomicGet(),
			rf.currentTerm.AtomicGet())).F("to", server).Infoln("sendAppendEntries timed out...")
		return false
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
	if rf.state.AtomicGet() != Leader {
		return -1, int(rf.currentTerm.AtomicGet()), false
	}
	msg := &AppendMsg{
		LogEntry{Command: command},
		true, // init to true, could be set to false if rejected.
		make(chan struct{}),
	}

	rf.appendCh <- msg
	<-msg.done
	log.V(1).F(strconv.Itoa(rf.me), fmt.Sprintf("%v, %v", rf.state.AtomicGet(),
		rf.currentTerm.AtomicGet())).F("appmsg", msg).Infoln("append msg processed...")
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
	close(rf.stopCh)
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
	rf.raftLog = *newRaftLog()
	// initialize from state persisted before a crash
	rf.readRaftState(rf.persister)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower

	rf.appEntRpcCh = make(chan *RPCMsg, 1)
	rf.reqVoteRpcCh = make(chan *RPCMsg, 1)
	rf.appendCh = make(chan *AppendMsg, 1024)
	rf.applyCh = applyCh
	rf.stopCh = make(chan struct{})

	go rf.run()
	return rf
}

func (rf *Raft) run() {
	for {
		logV1 := log.V(1).F(strconv.Itoa(rf.me), fmt.Sprintf("%v, %v",
			rf.state.AtomicGet(), rf.currentTerm.AtomicGet()))
		switch rf.state.AtomicGet() {
		case Follower:
			logV1.Clone().Infoln("start to run as follower...")
			rf.runFollower()
		case Candidate:
			logV1.Clone().Infoln("start to run as candidate...")
			rf.runCandidate()
		case Leader:
			logV1.Clone().Infoln("start to run as leader...")
			rf.runLeader()
		default:
			logV1.Clone().Infoln("unexpected state...")
			return
		}

		select {
		case <-rf.stopCh:
			return
		default:
		}
	}
}

func (rf *Raft) quorum() int {
	return len(rf.peers)/2 + 1
}
