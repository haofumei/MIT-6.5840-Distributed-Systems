package shardkv


import (
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
	"6.5840/labgob"
	"sync"
	"sync/atomic"
	"log"
	"time"
	"bytes"
	
)
 

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	ClientId int64 // who assigns this Op
	SN int // serial number for this Op
	Playload interface{}
}

type ClientPlayload struct {
	Type string // "Get", "Put" or "Append"
	Key string // "Key" for the "Value"
	Value string // empty for "Get"
	Shard int // shard responsible for this Op
}

type ServerPlayload struct {
	Type string // "Migration"
	Num int // config num
	Servers []string // migrate from these servers
	Sids []int // migrating shard indexes
	Data []map[string]string // shard data replicated by leader
}

type DupEntry struct { // record the executed request
	SN int 
	Value string
	Err Err
}

type doitResult struct {
	ClientId int64 // who assigns this Op
	SN int // serial number for this Op
	Value string // empty for "Get"
	Err Err // err message
}

type Shard struct {
	Status ShardStatus
	Data map[string]string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	sm       	 *shardctrler.Clerk
	dead         int32 // set by killed()
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Persistent state on snapshot, capitalize for encoding
	Shards []Shard // shard -> data
	DupTable map[int64]DupEntry // table for duplicated check

	// Volatile state on all server.
	resultCh map[int]chan doitResult // transfer result to RPC
	lastApplied int // lastApplied log index
	config   shardctrler.Config // current config

	// Channels
	pollTrigger chan bool 
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(ClientPlayload{})
	labgob.Register(ServerPlayload{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.sm = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.pollTrigger = make(chan bool)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.resultCh = make(map[int]chan doitResult)
	kv.DupTable = make(map[int64]DupEntry)
	kv.Shards = make([]Shard, shardctrler.NShards)
	for i, _ := range kv.Shards {
		kv.Shards[i].Status = ShardOK
		kv.Shards[i].Data = make(map[string]string)
	}

	kv.ingestSnap(persister.ReadSnapshot())

	go kv.pollTicker(kv.pollTrigger)
	go kv.applier(persister, maxraftstate)

	return kv
}

// handle one Op received by Get or PutAppend RPC.
// first, it performs duplicated detection. if not, it goes to next step.
// if current server is the leader, it will replicate the log through Raft, and update the key/value pairs based on the Op.
// finally, it returns response info in Op for next same Op check.
func (kv *ShardKV) doit(op *Op) doitResult {
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
	
	// check if the replica group is responsible or ready for this op
	if pl, ok := op.Playload.(ClientPlayload); ok {
		if sid := pl.Shard; kv.config.Shards[sid] != kv.gid || kv.Shards[sid].Status != ShardOK {
			result.Err = ErrWrongGroup
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

	DPrintf("(%d: %d) call op: %v at index %d", kv.gid, kv.me, op, index)

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

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {

	op := Op {
		ClientId: args.ClientId,
		SN: args.SN,
		
	}
	op.Playload = ClientPlayload {
		Type: "Get",
		Key: args.Key,
		Shard: args.Shard,
	}
	
	result := kv.doit(&op)

	// Optimation: reply if it is a same op even though the leader may change
	if result.ClientId == args.ClientId && result.SN == args.SN {
		reply.Value = result.Value
		reply.Err = result.Err
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	op := Op {
		ClientId: args.ClientId,
		SN: args.SN,
	}
	op.Playload = ClientPlayload {
		Type: args.Op,
		Key: args.Key,
		Value: args.Value,
		Shard: args.Shard,
	}

	result := kv.doit(&op)

	// Optimation: reply if it is a same op even though the leader may change
	if result.ClientId == args.ClientId && result.SN == args.SN {
		reply.Err = result.Err
	}
}

// ingest one command, and update the state of storage.
// transfer back the result by OpCh.
func (kv *ShardKV) ingestCommand(index int, command interface{}) {
	op := command.(Op)
	result := doitResult{ClientId: op.ClientId, SN: op.SN, Err: OK}

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
	}

	switch pl := op.Playload.(type) {
	case ClientPlayload:
		switch pl.Type {
		case "Get":
			value, ok := kv.Shards[pl.Shard].Data[pl.Key]
			if ok {
				result.Value = value
			} else {
				result.Err = ErrNoKey 
			}
		case "Put":
			kv.Shards[pl.Shard].Data[pl.Key] = pl.Value
		case "Append":
			kv.Shards[pl.Shard].Data[pl.Key] += pl.Value
		default:
			panic(op)
		}
	case ServerPlayload:
		switch pl.Type {
		case "MigrationOut":
			if _, isLeader := kv.rf.GetState(); isLeader {
				go kv.sendShardMigration(pl.Servers, pl.Sids, pl.Num)
			}
			return // no need to test duplication
		case "MigrationIn":
			DPrintf("(%d:%d) install: %v", kv.gid, kv.me, pl)
			for _, sid := range pl.Sids {
				kv.Shards[sid].Data = copyOfData(pl.Data[sid])
				kv.Shards[sid].Status = ShardOK
			}
		default:
			panic(op)
		}
	default:
		panic(op)
	}

	kv.DupTable[result.ClientId] = DupEntry{
		SN: result.SN,
		Value: result.Value,
		Err: result.Err,
	}
	DPrintf("(%d:%d) update dup: %v", kv.gid, kv.me, kv.DupTable)
	
	// send the result back if this server has channel 
	// no matter whether it is a duplicated or new request to avoid resource leaks
	// however, for example, when server 1 was partitioned and start a request for client 1 with SN 1
	// when server 1 come back and apply other log (client 2 with SN 1) with same log index
	// should check if it is the right result received by this channel
	if ch, ok := kv.resultCh[index]; ok { 
		ch <- result
	}
	delete(kv.resultCh, index)
}

// install the snapshot.
func (kv *ShardKV) ingestSnap(snapshot []byte) {
	if len(snapshot) == 0 {
		return // ignore empty snapshot
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var shards []Shard
	var dupTable map[int64]DupEntry
	if d.Decode(&shards) != nil ||
		d.Decode(&dupTable) != nil {
		log.Fatalf("snapshot decode error")
	}

	kv.mu.Lock()
	kv.Shards = shards
	kv.DupTable = dupTable
	kv.mu.Unlock()
}

// this function acts at a long running goroutine,
// accepts ApplyMsg from Raft through applyCh.
// if it is a command, it will update the state of storage, and check the necessity to take a snapshot.
// if it is a snapshot, it will install the snapshot.
func (kv *ShardKV) applier(persister *raft.Persister, maxraftstate int) {

	for m := range kv.applyCh {

		if m.CommandValid {
			DPrintf("(%d: %d) apply command: %v at %d", kv.gid, kv.me, m.Command, m.CommandIndex)
			kv.ingestCommand(m.CommandIndex, m.Command)
			
			if maxraftstate != -1 && (m.CommandIndex % SnapCheckpoint == 0) { 
				if persister.RaftStateSize() > maxraftstate {
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					kv.mu.Lock()
					if e.Encode(kv.Shards) != nil ||
						e.Encode(kv.DupTable) != nil {
						log.Fatalf("snapshot encode error")
					}
					kv.mu.Unlock()
					DPrintf("%d snapshot at %d", kv.me, m.CommandIndex)
					kv.rf.Snapshot(m.CommandIndex, w.Bytes())
				}
			}
		} else if m.SnapshotValid && kv.lastApplied < m.SnapshotIndex { // no need lock here
			DPrintf("%d apply snapshot at %d and lastApplied: %d", kv.me, m.SnapshotIndex, kv.lastApplied)
			kv.ingestSnap(m.Snapshot)
		}
	}
}

func (kv *ShardKV) pollTicker(pollTrigger chan bool) {
	for !kv.killed() {

		select {
		case <-pollTrigger:
		case <-time.After(time.Duration(PollInterval) * time.Millisecond):
		}

		kv.mu.Lock()
		
		// process re-configurations one at a time, in order.
		newConfig := kv.sm.Query(kv.config.Num + 1)

		if kv.config.Num == newConfig.Num { // same config
			kv.mu.Unlock()
			continue
		}

		if _, isLeader := kv.rf.GetState(); isLeader { // only leader start migration
			DPrintf("(%d:%d) last C: %v and new C: %v",kv.gid, kv.me, kv.config, newConfig)
			shardsOut := make(map[int][]int) // gid -> shards
			for i := 0; i < shardctrler.NShards; i++ {
				if kv.config.Shards[i] == newConfig.Shards[i] {
					continue
				}
				if kv.config.Shards[i] == kv.gid { // record the shards that need to migrate out
					shardsOut[newConfig.Shards[i]] = append(shardsOut[newConfig.Shards[i]], i)
				}
				if newConfig.Shards[i] == kv.gid && kv.config.Shards[i] != 0 { // halt the shards that need to migrate in
					kv.Shards[i].Status = ShardMigrating
				}
			}
			for gid, sids := range shardsOut { // migrate "Sids" to "Servers"
				op := Op{}
				op.Playload = ServerPlayload{
					Type: "MigrationOut",
					Num: newConfig.Num,
					Sids: sids, 
					Servers: newConfig.Groups[gid], 
				}
				kv.rf.Start(op)
			}
			DPrintf("(%d:%d) current shard: %v after poll",kv.gid, kv.me, kv.Shards)
		}

		kv.config = newConfig
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) ShardMigration(args *ShardMigrationArgs, reply *ShardMigrationReply) {

	kv.mu.Lock()

	if args.Num < kv.config.Num {
		reply.Err = ErrOutdatedConfig
		kv.mu.Unlock()
		return 
	}

	if args.Num > kv.config.Num {
		signalCh(kv.pollTrigger, true)
		reply.Err = ErrUpdatingConfig
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op {
		ClientId: args.ClientId,
		SN: args.SN,
	}
	op.Playload = ServerPlayload {
		Type: "MigrationIn",
		Num: args.Num,
		Sids: args.Sids,
		Data: args.Data,
	}
	DPrintf("(%d:%d) receive migration op:%v", kv.gid, kv.me, op)
	result := kv.doit(&op)
	DPrintf("(%d:%d) finish doit op:%v", kv.gid, kv.me, op)
	// Optimation: reply if it is a same op even though the leader may change
	if result.ClientId == args.ClientId && result.SN == args.SN {
		reply.Err = result.Err
	}
}

func (kv *ShardKV) sendShardMigration(servers []string, sids []int, num int) {
	
	kv.mu.Lock()

	data := make([]map[string]string, shardctrler.NShards)
	for _, sid := range sids {
		data[sid] = copyOfData(kv.Shards[sid].Data)
	}
	
	args := ShardMigrationArgs{
		Num: num,
		Sids: sids, 
		Data: data,
		ClientId: int64(kv.gid),
		SN: num, // use config Num as Serial number here
	}
	reply := ShardMigrationReply{}

	kv.mu.Unlock()
	DPrintf("(%d:%d) send migration with args: %v", kv.gid, kv.me, args)
	for {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])		
			ok := srv.Call("ShardKV.ShardMigration", &args, &reply)
			if ok && (reply.Err == OK || reply.Err == ErrOutdatedConfig) {
				break
			}
		}
		if reply.Err == OK {
			break
		}
		if reply.Err == ErrOutdatedConfig {
			signalCh(kv.pollTrigger, true)
			break
		}
		time.Sleep(PollInterval * time.Millisecond)
	}
	
}

func signalCh(ch chan bool, val bool) {
	select {
	case ch <- val:
	default:
	}
}

func copyOfData(data map[string]string) map[string]string {
	result := make(map[string]string)
	for k, v := range data {
		result[k] = v
	}
	return result
}

