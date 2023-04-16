package kvraft

import (
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
	Type string // "Get", "Put" or "Append"
	Key string // "Key" for the "Value"
	Value string // empty for "Get"
	Err Err
	ClientId int64 // who assigns this Op
	SN int // serial number for this Op
}

type dupEntry struct { // record the executed request
	SN int 
	Value string
	Err Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	data map[string]string
	opCh map[int]chan Op
	dupTable map[int64]dupEntry
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	op := Op {
		Type: "Get",
		Key: args.Key,
		ClientId: args.ClientId,
		SN: args.SN,
	}
	DPrintf("%d call op: %v", kv.me, op)
	
	newOp := kv.doit(&op)

	if newOp.ClientId == op.ClientId && newOp.SN == op.SN {
		reply.Value = newOp.Value
		reply.Err = newOp.Err
	}
	
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	
	op := Op {
		Type: args.Op,
		Key: args.Key,
		Value: args.Value,
		ClientId: args.ClientId,
		SN: args.SN,
	}
	DPrintf("%d call op: %v", kv.me, op)

	newOp := kv.doit(&op)

	if newOp.ClientId == op.ClientId && newOp.SN == op.SN {
		reply.Err = newOp.Err
	}
}

func (kv *KVServer) doit(op *Op) Op {
	kv.mu.Lock()

	// the follower should have the ability to detect duplicate before redirect to leader.
	// if it is a up-to-date follower, it is safe to do so.
	// if it is a stale follower, it is still safe to do so, because:
	// 1. if it has this entry, implies its log has been updated to this request
	// 2. if it does not, it will be redirect to other up-to-date server.
	// if it is a stale leader, this request will timeout and redirect to other serser.
	if dEntry, ok := kv.dupTable[op.ClientId]; ok { // duplicate detection
		if dEntry.SN == op.SN {
			op.Value = dEntry.Value
			op.Err = OK
			kv.mu.Unlock()
			return *op
		} 
	}

	index, term, isLeader := kv.rf.Start(*op)

	if term == 0 {
		op.Err = ErrInitElection
		kv.mu.Unlock()
		return *op
	}

	if !isLeader { // check if it is leader
		op.Err = ErrWrongLeader
		kv.mu.Unlock()
		return *op
	} 

	// must create reply channel before unlock
	ch := make(chan Op)
	kv.opCh[index] = ch
	kv.mu.Unlock()

	select {
	case result := <-ch:
		return result
	case <-time.After(time.Duration(ResponseTimeout) * time.Millisecond):
		op.Err = ErrWrongLeader // if we don't get a reponse in time, leader may be dead
	}	

	kv.mu.Lock()
	delete(kv.opCh, index)
	kv.mu.Unlock()
	return *op
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
	// Your code here, if desired.
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
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.opCh = make(map[int]chan Op)
	kv.dupTable = make(map[int64]dupEntry)

	go kv.applier()

	return kv
}

func (kv *KVServer) applier() {

	for m := range kv.applyCh {
		DPrintf("%d apply command: %v", kv.me, m)

		if (kv.killed()) {
			DPrintf("kill applier")
			return
		}

		if m.CommandValid {
			kv.ingestCommand(m.CommandIndex, m.Command)
		}

	}
}

func (kv *KVServer) ingestCommand(index int, command interface{}) {
	op := command.(Op)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// if a duplicate request arrives before the original executes
	// don't execute if table says already seen
	if dEntry, ok := kv.dupTable[op.ClientId]; ok && dEntry.SN >= op.SN { 
		return
	}

	switch op.Type {
	case "Get":
		value, ok := kv.data[op.Key]
		if ok {
			kv.dupTable[op.ClientId] = dupEntry{
				SN: op.SN,
				Value: value,
				Err: OK,
			}

			if ch, ok := kv.opCh[index]; ok { 
				op.Value = value
				op.Err = OK
				ch <- op
			}
		} else {
			kv.dupTable[op.ClientId] = dupEntry{
				SN: op.SN,
				Err: ErrNoKey,
			}

			if ch, ok := kv.opCh[index]; ok { // if it is leader
				op.Err = ErrNoKey
				ch <- op
			} 
		}
	case "Put":
		kv.data[op.Key] = op.Value
		kv.dupTable[op.ClientId] = dupEntry{
			SN: op.SN,
			Err: OK,
		}
		
		if ch, ok := kv.opCh[index]; ok { // if it is leader
			op.Err = OK
			ch <- op
		}
	case "Append":
		kv.data[op.Key] += op.Value
		kv.dupTable[op.ClientId] = dupEntry{
			SN: op.SN,
			Err: OK,
		}
	
		if ch, ok := kv.opCh[index]; ok { // if it is leader
			op.Err = OK
			ch <- op
		}
	default:
	}
	DPrintf("%d current dupTable: %v", kv.me, kv.dupTable)
}

