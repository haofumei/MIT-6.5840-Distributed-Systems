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
	ClientId int64 // who assigns this Op
	SN int // serial number for this Op
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	data map[string]string
	replyCh map[int]chan interface{}
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	op := Op {
		Type: "Get",
		Key: args.Key,
		ClientId: args.ClientId,
		SN: args.SN,
	}

	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	} 
	DPrintf("leader %d begin op: %v", kv.me, op)

	// must create reply channel before unlock
	ch := make(chan interface{})
	kv.replyCh[index] = ch
	kv.mu.Unlock()

	select {
	case result := <-ch:
		*reply = result.(GetReply)
	case <-time.After(time.Duration(ResponseTimeout) * time.Millisecond):
		if currentTerm, _ := kv.rf.GetState(); currentTerm != term {
			reply.Err = ErrWrongLeader
		}
	}	
	DPrintf("args: %v, reply: %v", args, reply)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op {
		Type: args.Op,
		Key: args.Key,
		Value: args.Value,
		ClientId: args.ClientId,
		SN: args.SN,
	}

	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	} 
	DPrintf("leader %d begin op: %v", kv.me, op)

	ch := make(chan interface{})
	kv.replyCh[index] = ch
	kv.mu.Unlock()

	select {
	case result := <-ch:
		*reply = result.(PutAppendReply)
	case <-time.After(time.Duration(ResponseTimeout) * time.Millisecond):
		if currentTerm, _ := kv.rf.GetState(); currentTerm != term {
			reply.Err = ErrWrongLeader
		}
	}	
	DPrintf("args: %v, reply: %v", args, reply)
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
	kv.replyCh = make(map[int]chan interface{})

	go kv.applier()

	return kv
}

func (kv *KVServer) applier() {

	for m := range kv.applyCh {
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

	switch op.Type {
	case "Get":
		value, ok := kv.data[op.Key]
		if ok {
			if ch, ok := kv.replyCh[index]; ok {
				ch <- GetReply{Value: value, Err: OK}
			}
		} else {
			if ch, ok := kv.replyCh[index]; ok {
				ch <- GetReply{Err: ErrNoKey}
			} 
		}
	case "Put":
		kv.data[op.Key] = op.Value
		if ch, ok := kv.replyCh[index]; ok {
			ch <- PutAppendReply{Err: OK}
		}
	case "Append":
		kv.data[op.Key] += op.Value
		if ch, ok := kv.replyCh[index]; ok {
			ch <- PutAppendReply{Err: OK}
		}
	default:
	}
}