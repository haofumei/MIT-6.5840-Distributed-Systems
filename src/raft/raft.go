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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	follower  = 0
	leader    = 1
	candidate = 2
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers.
	// Updated on stable storage before responding to RPCs.
	currentTerm int               // latest term server has seen
	votedFor    int               // candidateId that received vote in current term
	log         []LogEntry        // log entries; each entry contains command for state machine,
						          // and term when entry was received by leader
						          // first index is 1

	// Volatile state on all server.
	commitIndex  int              // index of highest log entry known to be committed
	lastApplied  int              // index of highest log entry applied to state machine
	currentState int32            // follower? leader? candidate?
	voteCnt      int              // count for received votes

	// Volatile state on leaders.
	// Reinitialized after election.
	nextIndex []int               // for each server, index of the next log entry to send to that server
					              // initialized to leader last log index + 1
	matchIndex []int              // for each server, index of highest log entry known to be replicated on server
	                              // initialized to 0, increases monotonically

	// Channels
	heartbeat   chan int          // signal indicates leader is alive
	grantVote   chan int          // signal indicates electing leader
	winElection chan int          // signal indicates candidate win election
	convertToF  chan int          // signal indicates convert to follower
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.currentState == leader)

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// Invoked by candidates to gather votes.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm { // ignore lower term request
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf("%d has term %d and %d has term %d", args.CandidateId, args.Term, rf.me, rf.currentTerm)
		rf.currentTerm = args.Term
		DPrintf("%d becomes follower due to Vote", rf.me)
		rf.convertToFollower()
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId &&
		rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId

		// grant vote to candidate
		signalCh(rf.grantVote, args.CandidateId)
		DPrintf("%d grante vote", rf.me)
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
}

// Send requestVote to peers, return true if been granted vote.
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("%d send request vote to %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// filer late response 
	if args.Term != rf.currentTerm {
		return false
	}

	// if receiver's term > mine
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.convertToFollower()
		return true
	}

	if reply.VoteGranted {
		rf.voteCnt++
		if rf.voteCnt == (len(rf.peers)/2 + 1) {
			rf.winElection <- 1
		}
	}
	return true
}

// Do some preparations and start election as candidate
func (rf *Raft) startElection() {
	rf.mu.Lock()

	if rf.currentState != candidate {
		rf.mu.Unlock()
		return
	}

	rf.currentTerm++    // increment currentTerm
	rf.votedFor = rf.me // vote for self
	rf.voteCnt = 1
	DPrintf("%d start election(term: %d)", rf.me, rf.currentTerm)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	for p := range rf.peers {
		if p != rf.me {
			go func(server int) {
				rf.sendRequestVote(server, &args, &RequestVoteReply{})
			}(p)
		}
	}
}

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store, empty for heartbeat
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int             // currentTerm, for leader to update itself
	Success bool            // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// Invoked by leader to replicate log entries, also used as heartbeat.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d(state: %d) receive append from %d", rf.me, rf.currentState, args.LeaderId)
	// refuse every lower term request
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	signalCh(rf.heartbeat, args.LeaderId)

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}

	// when legitimate leader comes in
	rf.convertToFollower()

	if rf.getLastLogIndex() < args.PrevLogIndex ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
	}
}

// Send AppendEntries request to followers.
// Update leader info based on the response.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d(term: %d) is sending Append to %d", rf.me, rf.currentTerm, server)
	// filer outdated response
	if args.Term != rf.currentTerm {
		return false
	}
	// if receiver's term > mine
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.convertToFollower()
		DPrintf("%d becomes follower due to send Append", rf.me)
		return true
	}
	// index conflits, update corresponding index
	if !reply.Success {
		rf.nextIndex[server] -= 1
		return true
	}
	return true
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	if rf.currentState != leader {
		rf.mu.Unlock()
		return
	}

	argsList := make([]AppendEntriesArgs, len(rf.peers))
	for i := range rf.peers {
		if i != rf.me {
			argsList[i].Term = rf.currentTerm
			argsList[i].LeaderId = rf.me
			argsList[i].LeaderCommit = rf.commitIndex
			argsList[i].Entries = []LogEntry{}
			argsList[i].PrevLogIndex = rf.getPrevLogIndex(i)
			argsList[i].PrevLogTerm = rf.getPrevLogTerm(i)
		}
	}
	rf.mu.Unlock()

	for p := range rf.peers {
		if p != rf.me {
			go func(server int) {
				rf.sendAppendEntries(server, &argsList[server], &AppendEntriesReply{})
			}(p)
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// pause for a random amount of time between 50 and 350
		electionTimeout := 250 + (rand.Int63() % 250)
		heartBeatTimeout := 150

		rf.mu.Lock()
		switch rf.currentState {
		case follower:
			rf.mu.Unlock()
			select {
			case <-rf.heartbeat:
			case <-rf.grantVote:
			case <-time.After(time.Duration(electionTimeout) * time.Millisecond):
				rf.mu.Lock()
				rf.convertToCandidate()
				rf.mu.Unlock()
				go rf.startElection()
			}
		case leader:
			rf.mu.Unlock()
			select {
			case <-rf.convertToF:
			case <-time.After(time.Duration(heartBeatTimeout) * time.Millisecond):
				go rf.sendHeartbeat()
			}
		case candidate:
			rf.mu.Unlock()
			select {
			case <-rf.convertToF:
			case <-rf.winElection:
				rf.mu.Lock()
				rf.convertToLeader()
				rf.mu.Unlock()
				go rf.sendHeartbeat()
			case <-time.After(time.Duration(electionTimeout) * time.Millisecond):
				go rf.startElection()
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.currentState = follower
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.initChannels()
	DPrintf("Initiate %d(term: %d, state: %d)", rf.me, rf.currentTerm, rf.currentState)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}

// Helper function

// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
// If the logs end with the same term, then whichever log is longer is more up-to-date.
// Return true if candidate's log is at least up-to-date as me
// thread unsafe, need lock
func (rf *Raft) isLogUpToDate(cLastLogIndex int, cLastLogTerm int) bool {
	mLastLogIndex := rf.getLastLogIndex()
	mLastLogTerm := rf.getLastLogTerm()

	if cLastLogTerm > mLastLogTerm {
		return true
	}
	if cLastLogTerm == mLastLogTerm && cLastLogIndex >= mLastLogIndex {
		return true
	}
	return false
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// thread unsafe, need lock
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

// thread unsafe, need lock
func (rf *Raft) getLastLogTerm() int {
	return rf.log[rf.getLastLogIndex()].Term
}

// thread unsafe, need lock
func (rf *Raft) getPrevLogIndex(server int) int {
	return rf.nextIndex[server] - 1
}

// thread unsafe, need lock
func (rf *Raft) getPrevLogTerm(server int) int {
	return rf.log[rf.getPrevLogIndex(server)].Term
}

// thread unsafe, need lock
func (rf *Raft) convertToCandidate() {
	rf.currentState = candidate
	rf.winElection = make(chan int)
	rf.convertToF = make(chan int)
	DPrintf("%d becomes candidate", rf.me)
}

// thread unsafe, need lock
func (rf *Raft) convertToFollower() {
	if rf.currentState == follower {
		rf.votedFor = -1
	} else {
		rf.currentState = follower
		signalCh(rf.convertToF, 1)
		rf.votedFor = -1
		//rf.heartbeat = make(chan int)
		//rf.grantVote = make(chan int)
	}
	
}

// thread unsafe, need lock
func (rf *Raft) convertToLeader() {
	rf.currentState = leader
	rf.convertToF = make(chan int)
	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	DPrintf("%d becomes leader", rf.me)
}

// thread unsafe, need lock
func (rf *Raft) initChannels() {
	rf.heartbeat = make(chan int)
	rf.grantVote = make(chan int)
	rf.winElection = make(chan int)
	rf.convertToF = make(chan int)
}

func signalCh(ch chan int, signal int) {
	select {
	case ch <- signal:
	default:
	}
}
