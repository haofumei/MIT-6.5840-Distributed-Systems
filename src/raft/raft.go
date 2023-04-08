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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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

type snapshotOrder struct {
	server int
	id int
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
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	log         []LogEntry // log entries, first index is 1
	lastIncludedIndex int  // last log chunk ending index, initiate to -1
	lastIncludedTerm int   // term of lastIncludedIndex

	// Volatile state on all server.
	commitIndex  int   // index of highest log entry known to be committed
	lastApplied  int   // index of highest log entry applied to state machine
	currentState int32 // follower? leader? candidate?
	voteCnt      int   // count for received votes
	suspendApply bool  // temporarily stop applying command

	// Volatile state on leaders.
	// Reinitialized after election.
	nextIndex []int // for each server, index of the next log entry to send to that server
	// initialized to leader last log index + 1
	matchIndex []int // for each server, index of highest log entry known to be replicated on server
	// initialized to 0, increases monotonically

	// Channels
	heartbeat    chan bool // signal indicates leader is alive
	grantVote    chan bool // signal indicates electing leader
	winElection  chan bool // signal indicates candidate win election
	convertToF   chan bool // signal indicates convert to follower
	applyTrigger chan bool // signal indicates applier should work
	applyCh chan ApplyMsg // 
	snapshotCh chan ApplyMsg
	orderSnapshot chan snapshotOrder
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := (rf.currentState == leader)

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(snapshot []byte) {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// persistent state
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil || 
		e.Encode(rf.lastIncludedIndex) != nil || 
		e.Encode(rf.lastIncludedTerm) != nil {
		DPrintf("Write persist error")
	} else {
		raftstate := w.Bytes()
		if snapshot != nil {
			rf.persister.Save(raftstate, snapshot)
		} else {
			rf.persister.SaveRaftstate(raftstate)
		}
		
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil || 
		d.Decode(&lastIncludedIndex) != nil || 
		d.Decode(&lastIncludedTerm) != nil {
		DPrintf("Read persist error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {

	msg := ApplyMsg{
		CommandValid: false,
		SnapshotValid: true,
		Snapshot: snapshot,
		SnapshotIndex: index,
	}

	rf.mu.Lock()
	rf.suspendApply = true // suspend applying command
	msg.SnapshotTerm = rf.currentTerm // set term in mutex
	rf.trimLog(msg.SnapshotIndex) // trim the log
	rf.persist(msg.Snapshot) // persist state and snapshot
	select { // use select to avoid blocking
	case <-rf.applyCh: // if the last command has sent to applyCh
		rf.lastApplied-- // rollback lastApplied to resend		
	default:
	}
	rf.mu.Unlock()

	rf.snapshotCh <- msg 
	
	 // can not send msg to applyCh here, or it will block
	DPrintf("%d snapshot at index %d", rf.me, index)
		
}

// handle one snaphot
func (rf *Raft) snapshotDealer(applyCh chan ApplyMsg) {
	for !rf.killed() {

		msg := <- rf.snapshotCh

		applyCh <- msg // send before reset suspendApply
		
		rf.mu.Lock()
		rf.suspendApply = false // resume apply command
		rf.mu.Unlock()
		signalCh(rf.applyTrigger, true)
	}
}

// trim the log, discard entries that behind the index
func (rf *Raft) trimLog(index int) {
	
	// must store term first
	if index <= rf.getLastLogIndex() {
		rf.lastIncludedTerm = rf.getTermAt(index)
	}

	if rf.getLastLogIndex() <= index {
		rf.log = make([]LogEntry, 0)
	} else {
		newCopy := make([]LogEntry, rf.getLastLogIndex() - index)
		copy(newCopy, rf.log[rf.getCut(index + 1) : ])
		rf.log = newCopy
	}

	rf.lastIncludedIndex = index // update at last
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

	if args.Term > rf.currentTerm { // a better candidate occurs
		rf.currentTerm = args.Term
		rf.convertToFollower()
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		rf.persist(nil)

		DPrintf("%d grant vote to %d", rf.me, args.CandidateId)
		// grant vote to candidate
		signalCh(rf.grantVote, true)
	} else { // candidate's log is outdated
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
	DPrintf("%d send requestVote to %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm { // filter outdated response
		return false
	}

	if reply.Term > rf.currentTerm { // if receiver's term > mine
		rf.currentTerm = reply.Term
		rf.convertToFollower()
		rf.persist(nil)
		return true
	}

	if reply.VoteGranted {
		rf.voteCnt++
		if rf.voteCnt == (len(rf.peers)/2 + 1) {
			signalCh(rf.winElection, true)
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
	rf.persist(nil)

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
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	XTerm   int  // term of conflicting entry or last term in follower's log
	XIndex  int  // index of first entry of that term
	XLen    int  // true length of follower log
}

// Invoked by leader to replicate log entries, also used as heartbeat.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm { // refuse lower term request
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	signalCh(rf.heartbeat, true) // if args.Term >= rf.currentTerm

	// if args.Term == rf.currentTerm, we should not set votedFor = -1
	// since every term, every server should only vote for one candidate
	// and here this candidate has voted itself
	if rf.currentState != follower {
		rf.currentState = follower
		rf.persist(nil)
		signalCh(rf.convertToF, true)
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertToFollower()
		rf.persist(nil)
	}

	if rf.getLastLogIndex() < args.PrevLogIndex { // follower's log is shorter than PrevLogIndex
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XLen = rf.getUncut(len(rf.log)) 
	} else {
		if rf.getTermAt(args.PrevLogIndex) != args.PrevLogTerm { // PrevLogTerm does not match
			reply.Success = false
			reply.Term = rf.currentTerm

			reply.XTerm = rf.getTermAt(args.PrevLogIndex)
			reply.XIndex = rf.getUncut(leftBound(rf.log, reply.XTerm))
			reply.XLen = rf.getUncut(len(rf.log)) 
		} else { // PrevLogTerm match
			if len(args.Entries) > 0 { // append entries into log if not heartbeat
				rf.log = append(rf.log[: rf.getCut(args.PrevLogIndex+1)], args.Entries...)
				rf.persist(nil)
				DPrintf("%d update its log: %v", rf.me, rf.log)
			}
			
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
				DPrintf("%d commit Index update to %d", rf.me, rf.commitIndex)
				signalCh(rf.applyTrigger, true) // apply log
			}
			reply.Success = true
			reply.Term = rf.currentTerm
			reply.XLen = rf.getUncut(len(rf.log)) // always return full length to update snapshot
		}
	}
}

// Send AppendEntries request to followers.
// Update leader info based on the response.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("%d sendAppendEntries to %d with pivot(%d, %d) log: %d", rf.me, server, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	
	if !ok {
		return false
	}

	DPrintf("%d receiveAppendEntries from %d %v", rf.me, server, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm { // filer outdated response
		return false
	}

	if reply.Term > rf.currentTerm { // if receiver's term > mine
		rf.currentTerm = reply.Term
		rf.convertToFollower()
		rf.persist(nil)
		return true
	}

	needSnapshot := false
	if !reply.Success { // index conflits, update corresponding index
		if reply.XLen < args.PrevLogIndex + 1 { // follower's log is too short
			if reply.XLen >= rf.lastIncludedIndex { // no need to send snapshot
				rf.nextIndex[server] = reply.XLen
			} else { // trigger send snapshot
				needSnapshot = true
			}
		} else {
			pivot := rightBound(rf.log, reply.XTerm)
			if pivot == -1 { // No term that is lower than XTerm exist (need snapshot)
				needSnapshot = true
			} else if rf.log[pivot].Term == reply.XTerm { // leader has conflited term
				rf.nextIndex[server] = rf.getUncut(pivot + 1)
			} else { // term that is lower than XTerm exist
				rf.nextIndex[server] = reply.XIndex
			}
			
		}
	} else { // reply success
		if len(args.Entries) > 0 { // for debug
			DPrintf("%d update %d match %d and next %d", rf.me, server, rf.matchIndex[server], rf.nextIndex[server])
		} 

		rf.matchIndex[server] = max(args.PrevLogIndex + len(args.Entries), rf.matchIndex[server])
		rf.nextIndex[server] = max(args.PrevLogIndex + len(args.Entries) + 1, rf.nextIndex[server])

		if rf.tryCommit(server) {
			// apply log
			signalCh(rf.applyTrigger, true)
		}
		/*
		// Suppose leader 0 log at index 98, and follower 1 log at index 98
		// Leader now start a new agreement at log 99, and then sendAppendEntries to 1 with 99, and snapshot at 99
		// Follower 1 successfully update its log to 99, and snapshot at 99 too
		// Image at this time, 1's response to 0 lost due to network failure, which means leader can not update the nextIndex for 1
		// 0 still send 98 as prevLogIndex to 1, and 1 will reply success for this prevLogIndex
		// So I return full length of 1 for 0 to update the prevLogIndex
		if reply.XLen > rf.nextIndex[server] {
			rf.nextIndex[server] = reply.XLen
		}*/

		if reply.XLen < rf.lastIncludedIndex + 1 { 
			needSnapshot = true
		}
	}

	if needSnapshot {
		select {
		case rf.orderSnapshot <- snapshotOrder{server: server, id: 0}:
		default:
		}
	}
	return true
}

func (rf *Raft) startLogSync() {
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
			argsList[i].PrevLogIndex = rf.getPrevLogIndex(i)
			argsList[i].PrevLogTerm = rf.getPrevLogTerm(i)

			if rf.getPrevLogIndex(i) == rf.getLastLogIndex() {
				argsList[i].Entries = nil
			} else if rf.getPrevLogIndex(i) < rf.lastIncludedIndex {
				argsList[i].Entries = nil // the entries have been deleted by snapshot
				select {
				case rf.orderSnapshot <- snapshotOrder{server: i, id: 0}: 
				default:
				}
			} else {
				subEntries := rf.log[rf.getCut(rf.nextIndex[i]) :]
				argsList[i].Entries = make([]LogEntry, len(subEntries))
				copy(argsList[i].Entries, subEntries)
			}
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

/************************
** InstallSnapshot RPC **
*************************/
type InstallSnapshotArgs struct {
	Term int // leader’s term
	LeaderId int // so follower can redirect clients
	LastIncludedIndex int // the snapshot replaces all entries up through and including this index
	LastIncludedTerm int // term of lastIncludedIndex
	Offset int // byte offset where chunk is positioned in the snapshot file (not use)
	Data []byte // raw bytes of the snapshot chunk
	Done bool // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("%d start snapshot at %d", rf.me, args.LastIncludedIndex)
	rf.mu.Lock()

	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.lastIncludedIndex { 
		reply.Term = rf.currentTerm
		DPrintf("%d reject snapshot at index %d", rf.me, args.LastIncludedIndex)
		rf.mu.Unlock()
		return
	}

	rf.suspendApply = true // suspend apply command until snapshot completes

	msg := ApplyMsg{
		CommandValid: false,
		SnapshotValid: true,
		Snapshot: args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm: rf.currentTerm,
	}

	rf.trimLog(msg.SnapshotIndex) // trim the log
	rf.persist(msg.Snapshot) // persist state and snapshot
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	DPrintf("%d finish snapshot at %d term %d, commit: %d", rf.me, args.LastIncludedIndex, rf.lastIncludedTerm, rf.commitIndex)
	rf.mu.Unlock()

	rf.snapshotCh <- msg
	DPrintf("%d done snapshot at %d", rf.me, args.LastIncludedIndex)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply, orderId int) bool {
	DPrintf("%d send InstallSnapshot to %d with index %d term %d with id: %d", rf.me, server, args.LastIncludedIndex, args.LastIncludedTerm, orderId)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	if !ok {
		select { // retry and increase orderId
		case rf.orderSnapshot <- snapshotOrder{server: server, id: orderId + 1}:
			DPrintf("%d retry %d with index %d term %d with id: %d", rf.me, server, args.LastIncludedIndex, args.LastIncludedTerm, orderId + 1)
		default:
		}
		return false
	}
	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm { // filer outdated response
		return false
	}

	if reply.Term > rf.currentTerm { // if receiver's term > mine
		rf.currentTerm = reply.Term
		rf.convertToFollower()
		rf.persist(nil)
		return true
	}

	rf.nextIndex[server] = max(args.LastIncludedIndex+1, rf.nextIndex[server])
	rf.matchIndex[server] = max(args.LastIncludedIndex, rf.matchIndex[server])
	return true
}

func (rf *Raft) startInstallSnapshot() {
	var asgs *InstallSnapshotArgs
	lastIncludedIndices := make([]int, len(rf.peers))
	prevOrderId := make([]int, len(rf.peers))

	for !rf.killed() {
		order := <- rf.orderSnapshot
		DPrintf("%d receive order (%d, %d)", rf.me, order.server, order.id)
		rf.mu.Lock()

		if rf.currentState != leader {
			rf.mu.Unlock()
			return
		}

		if lastIncludedIndices[order.server] < rf.lastIncludedIndex { // new snapshot 
			lastIncludedIndices[order.server] = rf.lastIncludedIndex
			prevOrderId[order.server] = 0 // reset id for this lastIncludedIndex
		} else if prevOrderId[order.server] > order.id { // this id for this lastIncludedIndex has been sent
			rf.mu.Unlock()
			continue
		}

		asgs = &InstallSnapshotArgs {
			Term: rf.currentTerm,
			LeaderId: rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm: rf.lastIncludedTerm,
			Data: rf.persister.ReadSnapshot(),
		}
		
		prevOrderId[order.server]++
		go rf.sendInstallSnapshot(order.server, asgs, &InstallSnapshotReply{}, order.id)

		rf.mu.Unlock()
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentState != leader || rf.killed() {
		return index, term, false
	}

	rf.log = append(rf.log, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	rf.persist(nil)

	term = rf.currentTerm
	index = rf.getLastLogIndex()
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	DPrintf("%d Start agreement: index %d, log: %v", rf.me, index, rf.log[len(rf.log)-1])
	//DPrintf("%d Start agreement: index %d", rf.me, index)
	return index, term, true
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
	//DPrintf("One thread killed")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) applier(applyCh chan<- ApplyMsg) {

	for !rf.killed() {

		isCommand := <-rf.applyTrigger
		
		if isCommand { // apply log entries
			rf.mu.Lock()

			for !rf.suspendApply && rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
			
				msg := ApplyMsg {
					CommandValid: true,
					Command:      rf.log[rf.getCut(rf.lastApplied)].Command,
					CommandIndex: rf.lastApplied,
				}
				rf.mu.Unlock()
				applyCh <- msg
				rf.mu.Lock()
			}
			rf.mu.Unlock()
		} 
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// pause for a random amount of time between 50 and 350
		electionTimeout := 300 + (rand.Int63() % 300)
		heartBeatTimeout := 120

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
				rf.persist(nil)
				rf.mu.Unlock()
				go rf.startElection()
			}
		case leader:
			rf.mu.Unlock()
			select {
			case <-rf.convertToF:
			case <-time.After(time.Duration(heartBeatTimeout) * time.Millisecond):
				go rf.startLogSync()
			}
		case candidate:
			rf.mu.Unlock()
			select {
			case <-rf.convertToF:
			case <-rf.winElection:
				rf.mu.Lock()
				rf.convertToLeader()
				rf.persist(nil)
				rf.mu.Unlock()
				go rf.startInstallSnapshot()
				go rf.startLogSync()
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
	rf.lastIncludedIndex = -1 // initiate to -1 to complement before snapshot apply
	rf.currentState = follower
	rf.suspendApply = false
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.heartbeat = make(chan bool)
	rf.grantVote = make(chan bool)
	rf.winElection = make(chan bool)
	rf.convertToF = make(chan bool)
	rf.applyTrigger = make(chan bool)
	rf.applyCh = applyCh
	rf.snapshotCh = make(chan ApplyMsg)
	rf.orderSnapshot = make(chan snapshotOrder)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.lastIncludedIndex != -1 { // set up index after snapshot apply
		rf.commitIndex = rf.lastIncludedIndex 
		rf.lastApplied = rf.lastIncludedIndex 
	} 

	DPrintf("Initiate %d term %d with log: %v", rf.me, rf.currentTerm, rf.log)

	go rf.applier(applyCh)
	go rf.snapshotDealer(applyCh)

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

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// thread unsafe, need lock
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1 + rf.lastIncludedIndex + 1
}

// thread unsafe, need lock
// get trimmed log's last term
func (rf *Raft) getLastLogTerm() int {
	index := rf.getLastLogIndex()
	return rf.getTermAt(index)
}

// thread unsafe, need lock
func (rf *Raft) getPrevLogIndex(server int) int {
	return rf.nextIndex[server] - 1
}

// thread unsafe, need lock
func (rf *Raft) getPrevLogTerm(server int) int {
	index := rf.getPrevLogIndex(server)
	return rf.getTermAt(index)
}

// thread unsafe, need lock
// get corresponding cut index, or len
func (rf *Raft) getCut(index int) int {
	return index - rf.lastIncludedIndex - 1
}

// thread unsafe, need lock
// get corresponding uncut index, or len
func (rf *Raft) getUncut(index int) int {
	return index + rf.lastIncludedIndex + 1
}

// thread unsafe, need lock
// get term at index if index > lastIncludeIndex 
// otherwise, return lastIncludeTerm
func (rf *Raft) getTermAt(index int) int {
	if index > rf.lastIncludedIndex {
		return rf.log[rf.getCut(index)].Term
	} else {
		return rf.lastIncludedTerm
	}
}


// thread unsafe, need lock
func (rf *Raft) convertToCandidate() {
	rf.currentState = candidate
	rf.winElection = make(chan bool)
	rf.convertToF = make(chan bool)
}

// thread unsafe, need lock
// if it is a follower before, just reset votedFor
func (rf *Raft) convertToFollower() {
	if rf.currentState != follower {
		rf.currentState = follower
		signalCh(rf.convertToF, true)
	}
	rf.votedFor = -1
}

// thread unsafe, need lock
func (rf *Raft) convertToLeader() {
	rf.currentState = leader
	rf.convertToF = make(chan bool)
	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	DPrintf("%d becomes leader (term: %d)", rf.me, rf.currentTerm)
}

// safe tranfer signal without blocking
func signalCh(ch chan bool, sig bool) {
	select {
	case ch <- sig:
	default:
	}
}

// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
// and log[N].term == currentTerm: set commitIndex = N
func (rf *Raft) tryCommit(server int) bool {
	N := rf.matchIndex[server]
	cnt := 0

	if N <= rf.commitIndex {
		return false
	}

	for i := range rf.peers {
		if rf.matchIndex[i] >= N {
			cnt++
		}
	}

	if cnt > (len(rf.peers)/2) && rf.log[rf.getCut(N)].Term == rf.currentTerm {
		rf.commitIndex = N
		return true
	}
	return false
}

// Return index of the leftmost element in the array that
// is greater than or equal to x
func leftBound(log []LogEntry, x int) int {
	left := 0
	right := len(log)

	for left < right {
		mid := (left + right) / 2

		if log[mid].Term < x {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return left
}

// Return index of the rightmost element in the array that
// is less than or equal to x
func rightBound(log []LogEntry, x int) int {
	left := 0
	right := len(log)

	for left < right {
		mid := (left + right) / 2

		if log[mid].Term <= x {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return left - 1
}
