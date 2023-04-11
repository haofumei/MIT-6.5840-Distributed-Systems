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
	// millisecond
	electionTimeBase  = 500
	electionTimeRange = 200
	heartBeatTime     = 100
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
	id     int
	retry  bool
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent state on all servers.
	// Updated on stable storage before responding to RPCs.
	currentTerm       int        // latest term server has seen
	votedFor          int        // candidateId that received vote in current term
	log               []LogEntry // log entries, first index is 1
	lastIncludedIndex int        // last log chunk ending index, initiate to -1
	lastIncludedTerm  int        // term of lastIncludedIndex

	// Volatile state on all server.
	commitIndex  int   // index of highest log entry known to be committed
	lastApplied  int   // index of highest log entry applied to state machine
	currentState int32 // follower? leader? candidate?
	voteCnt      int   // count for received votes
	suspendApply bool  // temporarily stop applying command

	// Volatile state on leaders.
	// Reinitialized after election.
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	// Channels
	heartbeat      chan bool          // signal indicates leader is alive
	grantVote      chan bool          // signal indicates electing leader
	winElection    chan bool          // signal indicates candidate win election
	convertToF     chan bool          // signal indicates convert to follower
	applyTrigger   chan bool          // signal indicates applier should work
	startAgreement chan bool          // signal triggers appendEntries after start an agreement
	applyCh        chan ApplyMsg      // channel transfers ApplyMsg
	orderSnapshot  chan snapshotOrder // channel transfers snapshot order to startInstallSnapshot goroutine

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

// restore previously persisted raft state.
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

	rf.mu.Lock()
	rf.suspendApply = true // suspend applying command
	rf.trimLog(index)      // trim the log
	rf.persist(snapshot)   // persist state and snapshot
	select {               // use select to avoid blocking
	case <-rf.applyCh: // if the last command has sent to applyCh
		rf.lastApplied-- // rollback lastApplied to resend
	default:
	}
	rf.mu.Unlock()

	rf.applyTrigger <- false // apply snapshot trigger

	// can not send msg to applyCh here, or it will block
	DPrintf("%d snapshot at index %d", rf.me, index)

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
	signalCh(rf.startAgreement, true)
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

// this function acts at a long running goroutine
// accepts signals from applyTrigger channel
// applies the snapshots or commands with applyCh
func (rf *Raft) applier(applyCh chan<- ApplyMsg) {

	for !rf.killed() {

		isCommand := <-rf.applyTrigger

		if !isCommand { // apply snapshot
			rf.mu.Lock()
			msg := ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      rf.persister.ReadSnapshot(),
				SnapshotTerm:  rf.currentTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}

			rf.suspendApply = false // resume apply command
			rf.mu.Unlock()

			applyCh <- msg

		}

		// after applying snapshot, check if there are some commands remaining
		rf.mu.Lock()
		for !rf.suspendApply && rf.commitIndex > rf.lastApplied {
			rf.lastApplied++

			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.getCut(rf.lastApplied)].Command,
				CommandIndex: rf.lastApplied,
			}

			rf.mu.Unlock()

			applyCh <- msg

			rf.mu.Lock() // lock again after send msg to channel
		}
		rf.mu.Unlock()
	}
}

// this function acts at a long running goroutine
// if currentState is follower, execute one of below operations:
// 1. accepts heartbeat or grantVote signal to reset election timeout
// 2. converts to candidate and starts election after election timeout

// if currentState is candidate, execute one of below operations:
// 1. converts to follower if accepts signal from channel convertToF
// 2. converts to leader, and starts logSync and installSnapshot
// 3. starts a new election after election timeout

// if currentState is leader, execute one of below operations:
// 1. starts logSync if accepts signal from channel startAgreement
// 2. converts to follower if accepts signal from channel convertToF
// 3. starts logSync after heartbeat timeout
func (rf *Raft) ticker() {
	for !rf.killed() {

		electionTimeout := electionTimeBase + (rand.Int63() % electionTimeRange)
		heartBeatTimeout := heartBeatTime

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
			case <-rf.startAgreement:
				go rf.startLogSync()
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

	rf.votedFor = -1
	rf.lastIncludedIndex = -1 // initiate to -1 to complement the log index
	rf.currentState = follower
	rf.suspendApply = false
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.nextIndex = make([]int, len(rf.peers))  // initialized to leader last log index + 1
	rf.matchIndex = make([]int, len(rf.peers)) // initialized to 0, increases monotonically
	rf.heartbeat = make(chan bool)
	rf.grantVote = make(chan bool)
	rf.winElection = make(chan bool)
	rf.convertToF = make(chan bool)
	rf.applyTrigger = make(chan bool)
	rf.applyCh = applyCh
	rf.orderSnapshot = make(chan snapshotOrder)
	rf.startAgreement = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.lastIncludedIndex != -1 { // set up index after snapshot apply
		rf.commitIndex = rf.lastIncludedIndex
		rf.lastApplied = rf.lastIncludedIndex
	}

	DPrintf("Initiate %d term %d with log: %v", rf.me, rf.currentTerm, rf.log)

	go rf.applier(applyCh)

	go rf.ticker()

	return rf
}

// trim the log, discard entries that behind the index
// if index <= lastLogIndex, set lastIncludedTerm to the term at index
// else did not modify lastIncludedTerm
// but lastIncludedIndex will be reset to index
func (rf *Raft) trimLog(index int) {

	// must store term first
	if index <= rf.getLastLogIndex() {
		rf.lastIncludedTerm = rf.getTermAt(index)
	}

	if rf.getLastLogIndex() <= index {
		rf.log = make([]LogEntry, 0)
	} else {
		newCopy := make([]LogEntry, rf.getLastLogIndex()-index)
		copy(newCopy, rf.log[rf.getCut(index+1):])
		rf.log = newCopy
	}

	rf.lastIncludedIndex = index // update at last
}

/********************
** RequestVote RPC **
********************/
type RequestVoteArgs struct {
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// Invoked by candidates to gather votes.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
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

	// if votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		rf.persist(nil)

		signalCh(rf.grantVote, true)
	} else { // candidate's log is outdated
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
}

// send one requestVote RPC to the server, handle the response from this call
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
	DPrintf("%d send requestVote to %d, args: %v", rf.me, server, args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("%d reply requestVote to %d, reply: %v", server, rf.me, reply)

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

// On conversion to candidate, start election:
// 1. Increment currentTerm
// 2. Vote for self
// 3. Reset election timer
// 4. Send RequestVote RPCs to all other servers
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

/**********************
** AppendEntries RPC **
**********************/
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

	reply.XLen = rf.getUncut(len(rf.log)) // always return full length to update snapshot

	// if follower's lastLogIndex is smaller than prevLogIndex
	// or prevLogIndex is smaller than follower's lastIncludedIndex
	if rf.getLastLogIndex() < args.PrevLogIndex || rf.lastIncludedIndex > args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// if prevLogIndex exists in current log, compare the term at this index
	if rf.getTermAt(args.PrevLogIndex) != args.PrevLogTerm { // PrevLogTerm does not match
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XTerm = rf.getTermAt(args.PrevLogIndex)
		reply.XIndex = rf.getUncut(leftBound(rf.log, reply.XTerm))
	} else { // PrevLogTerm matches
		if len(args.Entries) > 0 { // append entries into log if not heartbeat
			// If an existing entry conflicts with a new one (same index but different terms),
			// delete the existing entry and all that follow it
			var i int
			needPersist := false
			begin := rf.getCut(args.PrevLogIndex) + 1
			existingEntries := rf.log[begin:]
			DPrintf("%d log: %v and append: %v", rf.me, rf.log, args.Entries)
			for i = 0; i < min(len(existingEntries), len(args.Entries)); i++ {
				if existingEntries[i].Term != args.Entries[i].Term {
					rf.log = rf.log[:begin+i]
					needPersist = true
					break
				}
			}
			if i < len(args.Entries) {
				rf.log = append(rf.log, args.Entries[i:]...)
				needPersist = true
			}
			DPrintf("%d log: %v after append", rf.me, rf.log)
			
			if needPersist {
				rf.persist(nil)
			}
		}

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
			signalCh(rf.applyTrigger, true) // apply log
		}
		reply.Success = true
		reply.Term = rf.currentTerm
	}

}

// Send one AppendEntries request to the server.
// Update leader info based on the response.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("%d sendAppendEntries to %d, args: %v", rf.me, server, args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("%d replyAppendEntries to %d, reply: %v", server, rf.me, reply)

	if !ok {
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

	needSnapshot := false
	if !reply.Success { // index conflits, update corresponding index
		if reply.XLen < args.PrevLogIndex+1 { // follower's log is too short
			if reply.XLen < rf.lastIncludedIndex+1 { // trigger send snapshot
				needSnapshot = true
			} else { // no need to send snapshot
				rf.nextIndex[server] = reply.XLen
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

		rf.matchIndex[server] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[server])
		rf.nextIndex[server] = max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[server])

		if rf.tryCommit(server) {
			// apply log
			signalCh(rf.applyTrigger, true)
		}

		if reply.XLen < rf.lastIncludedIndex+1 {
			needSnapshot = true
		}
	}

	// trigger negative(not retry) snapshot
	if needSnapshot {
		select {
		case rf.orderSnapshot <- snapshotOrder{server: server, id: -1, retry: false}:
		default:
		}
	}
	return true
}

// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; 
// repeat during idle periods to prevent election timeouts.

// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex.
// If successful: update nextIndex and matchIndex for follower;
// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
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
				// trigger retry snapshot
				select {
				case rf.orderSnapshot <- snapshotOrder{server: i, id: 0, retry: true}:
				default:
				}
			} else {
				subEntries := rf.log[rf.getCut(rf.nextIndex[i]):]
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
	Term              int    // leader’s term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Offset            int    // byte offset where chunk is positioned in the snapshot file (not use)
	Data              []byte // raw bytes of the snapshot chunk
	Done              bool   // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

// Invoked by leader to send chunks of a snapshot to a follower.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	// refuse lower term request and the installed snapshot
	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.suspendApply = true             // suspend apply command until snapshot completes
	rf.trimLog(args.LastIncludedIndex) // trim the log
	rf.persist(args.Data)              // persist state and snapshot
	rf.lastIncludedTerm = args.LastIncludedTerm 
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	rf.mu.Unlock()

	rf.applyTrigger <- false // trigger apply snapshot
}

// send one InstallSnapshot RPC to server.
// if fail to receive the response, retry a same call; 
// otherwise, update the nextIndex and matchIndex for this server.
func (rf *Raft) sendInstallSnapshot(order snapshotOrder, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	DPrintf("%d send InstallSnapshot to %d with index %d term %d with id: %d", rf.me, order.server, args.LastIncludedIndex, args.LastIncludedTerm, order.id)
	ok := rf.peers[order.server].Call("Raft.InstallSnapshot", args, reply)
	DPrintf("%d reply InstallSnapshot to %d with index %d term %d with id: %d", order.server, rf.me, args.LastIncludedIndex, args.LastIncludedTerm, order.id)
	
	if !ok {
		if order.retry {
			order.id = order.id + 1 // retry and increase orderId
			select {
			case rf.orderSnapshot <- order:
				DPrintf("%d retry %d with index %d term %d with id: %d", rf.me, order.server, args.LastIncludedIndex, args.LastIncludedTerm, order.id)
			default:
			}
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

	// avoid some delay responses rollback the log
	rf.nextIndex[order.server] = max(args.LastIncludedIndex+1, rf.nextIndex[order.server])
	rf.matchIndex[order.server] = max(args.LastIncludedIndex, rf.matchIndex[order.server])
	return true
}

// this function acts at a long running goroutine,
// accepts snapshot order from channel orderSnapshot,
// send InstallSnapshot RPC to the needed server based on the info of order.
func (rf *Raft) startInstallSnapshot() {
	var asgs *InstallSnapshotArgs
	lastIncludedIndices := make([]int, len(rf.peers))
	prevOrderId := make([]int, len(rf.peers))

	for !rf.killed() {
		order := <-rf.orderSnapshot

		rf.mu.Lock()

		if rf.currentState != leader {
			rf.mu.Unlock()
			return
		}

		// two types of order: passive(retry order) and nagetive(one time order)
		// passive: will send again when sendInstallSnapshot receives not ok, and increase order id by one
		// negative: send if sendAppendEntries triggers, not increase order id
		if lastIncludedIndices[order.server] < rf.lastIncludedIndex { // new snapshot
			lastIncludedIndices[order.server] = rf.lastIncludedIndex
			prevOrderId[order.server] = 0 // reset id for this lastIncludedIndex
		} else if order.id < prevOrderId[order.server] && order.retry { // avoid sending too much retry order
			rf.mu.Unlock()
			continue
		}

		asgs = &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.persister.ReadSnapshot(),
		}

		if order.retry { // only increase id if retry order
			prevOrderId[order.server]++
		}

		go rf.sendInstallSnapshot(order, asgs, &InstallSnapshotReply{})

		rf.mu.Unlock()
	}

}

/********************
** Helper Function **
********************/

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
// get the uncut lastLogIndex
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
// get the uncut prevLogIndex
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
// get term at index if index >= lastIncludeIndex
// otherwise, return -1
func (rf *Raft) getTermAt(index int) int {
	if index > rf.lastIncludedIndex {
		return rf.log[rf.getCut(index)].Term
	} else if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	} else {
		return -1
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

// Return index of the leftmost element in the array that is greater than or equal to x;
// if no such element exist, return the last index 
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

// Return index of the rightmost element in the array that is less than or equal to x;
// if no such element exist, return -1
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
