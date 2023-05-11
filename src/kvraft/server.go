package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Type     string // "Get", "Put" or "Append"
	Key      string // "Key" for the "Value"
	Value    string // empty for "Get"
	ClientId int64  // who assigns this Op
	SN       int    // serial number for this Op
}

type DupEntry struct { // record the executed request
	SN    int
	Value string
	Err   Err
}

type doitResult struct {
	ClientId int64  // who assigns this Op
	SN       int    // serial number for this Op
	Value    string // empty for "Get"
	Err      Err    // err message
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// Persistent state on snapshot, capitalize for encoding
	Data     map[string]string  // key/value pairs
	DupTable map[int64]DupEntry // table for duplicated check
	lastIncludedIndex int // last snapshot index

	// Volatile state on all server.
	resultCh    map[int]chan doitResult // transfer result to RPC
	snapshotTrigger chan bool // signal indicates to take a snapshot
	lastApplied int                     // lastApplied log index
	
}

// Get RPC fetches the current value for the key, or empty string for a non-existent key.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	op := Op{
		Type:     "Get",
		Key:      args.Key,
		ClientId: args.ClientId,
		SN:       args.SN,
	}

	result := kv.doit(&op)

	// Optimation: reply if it is a same op even though the leader may change
	if result.ClientId == args.ClientId && result.SN == args.SN {
		reply.Value = result.Value
		reply.Err = result.Err
	}

}

// Put replaces the value for a particular key in the database, Append appends arg to key's value
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SN:       args.SN,
	}

	result := kv.doit(&op)

	// Optimation: reply if it is a same op even though the leader may change
	if result.ClientId == args.ClientId && result.SN == args.SN {
		reply.Err = result.Err
	}
}

// handle one Op received by Get or PutAppend RPC.
// first, it performs duplicated detection. if not, it goes to next step.
// if current server is the leader, it will replicate the log through Raft, and update the key/value pairs based on the Op.
// finally, it returns response info in Op for next same Op check.
func (kv *KVServer) doit(op *Op) doitResult {
	result := doitResult{ClientId: op.ClientId, SN: op.SN}
	kv.mu.Lock()

	// the follower should have the ability to detect duplicate before redirect to leader.
	// if it is a up-to-date follower, it is safe to do so.
	// if it is a stale follower, it is still safe to do so, because:
	// 1. if it has this entry, implies its log has been updated to this request
	// 2. if it does not, it will be redirect to other up-to-date server.
	// if it is a stale leader, this request will timeout and redirect to other serser.
	if dEntry, ok := kv.DupTable[op.ClientId]; ok { // duplicated detection
		if dEntry.SN == op.SN {
			result.Value = dEntry.Value
			result.Err = OK
			kv.mu.Unlock()
			return result
		}
	}

	index, _, isLeader := kv.rf.Start(*op)

	if !isLeader { // check if it is leader
		result.Err = ErrWrongLeader
		kv.mu.Unlock()
		return result
	}

	DPrintf("%d call op: %v at index %d", kv.me, op, index)

	// must create reply channel before unlock
	ch := make(chan doitResult)
	kv.resultCh[index] = ch
	kv.mu.Unlock()

	select {
	case result = <-ch:
	case <-time.After(time.Duration(ResponseTimeout) * time.Millisecond):
		result.Err = ErrWrongLeader // if we don't get a reponse in time, leader may be dead
	}

	go func() { // unblock applier
		<-ch
	}()

	return result
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.snapshotTrigger = make(chan bool)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.Data = make(map[string]string)
	kv.resultCh = make(map[int]chan doitResult)
	kv.DupTable = make(map[int64]DupEntry)

	kv.ingestSnap(persister.ReadSnapshot())

	go kv.snapshoter(persister, maxraftstate)
	go kv.applier()
	

	return kv
}

// this function acts at a long running goroutine,
// accepts ApplyMsg from Raft through applyCh.
// if it is a command, it will update the state of storage, and check the necessity to take a snapshot.
// if it is a snapshot, it will install the snapshot.
func (kv *KVServer) applier() {

	for m := range kv.applyCh {

		if m.CommandValid {
			DPrintf("%d apply command: %v at %d", kv.me, m.Command, m.CommandIndex)
			kv.ingestCommand(m.CommandIndex, m.Command)
		} else if m.SnapshotValid && kv.lastApplied < m.SnapshotIndex { // no need lock lastApplied here
			DPrintf("%d apply snapshot at %d and lastApplied: %d", kv.me, m.SnapshotIndex, kv.lastApplied)
			kv.ingestSnap(m.Snapshot)
		}
	}
}

func (kv *KVServer) snapshoter(persister *raft.Persister, maxraftstate int) {
	if maxraftstate == -1 {
		return
	}

	for !kv.killed() {
		select {
		case <-kv.snapshotTrigger:
		case <-time.After(SnapshotTimeout * time.Millisecond):
		}
		if kv.killed() {
			return
		}
		
		if persister.RaftStateSize() > maxraftstate {
			kv.printPersist(persister.ReadRaftState())
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			
			kv.mu.Lock()
			kv.lastIncludedIndex = kv.lastApplied
			index := kv.lastIncludedIndex
			if e.Encode(kv.Data) != nil ||
				e.Encode(kv.DupTable) != nil ||
				e.Encode(kv.lastIncludedIndex) != nil {
				log.Fatalf("snapshot encode error")
			}
			DPrintf("%d snapshot at %d and size: %d", kv.me, kv.lastApplied, persister.RaftStateSize())
			kv.mu.Unlock()

			kv.rf.Snapshot(index, w.Bytes())
		}
	}

}

// ingest one command, and update the state of storage.
// transfer back the result by OpCh.
func (kv *KVServer) ingestCommand(index int, command interface{}) {
	op := command.(Op)
	result := doitResult{ClientId: op.ClientId, SN: op.SN}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.lastApplied = index // update lastApplied index

	// if a duplicate request arrives before the original executes
	// don't execute if table says already seen
	if dEntry, ok := kv.DupTable[op.ClientId]; ok && dEntry.SN >= op.SN {
		// it is safe to ignore the lower SN request,
		// since the sender has received the result for this SN,
		// and has sent the higher SN for another request.
		if dEntry.SN == op.SN {
			result.Err = dEntry.Err
			result.Value = dEntry.Value
		}
	} else {
		switch op.Type {
		case "Get":
			value, ok := kv.Data[op.Key]
			if ok {
				result.Value = value
				result.Err = OK
			} else {
				result.Err = ErrNoKey
			}
		case "Put":
			kv.Data[op.Key] = op.Value
			result.Err = OK
		case "Append":
			kv.Data[op.Key] += op.Value
			result.Err = OK
		default:
			panic(op)
		}

		kv.DupTable[result.ClientId] = DupEntry{
			SN:    result.SN,
			Value: result.Value,
			Err:   result.Err,
		}
	}

	// send the result back if this server has channel
	// no matter whether it is a duplicated or new request to avoid resource leaks
	if ch, ok := kv.resultCh[index]; ok {
		ch <- result
		delete(kv.resultCh, index)
	}
	if (kv.lastApplied - kv.lastIncludedIndex) >= MaxNumOfCommandsWithoutSnapshot {
		select {
		case kv.snapshotTrigger <- true:
		default:
		}
		
	}
}

// install the snapshot.
func (kv *KVServer) ingestSnap(snapshot []byte) {
	if len(snapshot) == 0 {
		return // ignore empty snapshot
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var data map[string]string
	var dupTable map[int64]DupEntry
	var lastIncludedIndex int
	if d.Decode(&data) != nil ||
		d.Decode(&dupTable) != nil ||
		d.Decode(&lastIncludedIndex) != nil {
		log.Fatalf("snapshot decode error")
	}

	kv.mu.Lock()
	kv.Data = data
	kv.DupTable = dupTable
	kv.lastIncludedIndex = lastIncludedIndex
	kv.lastApplied = lastIncludedIndex
	kv.mu.Unlock()
}

// debug print
func (kv *KVServer) printPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var log []raft.LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		DPrintf("Read persist error")
	} else {
		DPrintf("%d currentTerm: %d, log length: %d, lastInclude: %d", kv.me, currentTerm, len(log), lastIncludedIndex)
	}
}
