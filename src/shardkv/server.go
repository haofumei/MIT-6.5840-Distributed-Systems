package shardkv

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Playload interface{}
}

type ClientOp struct {
	Type     string // "Get", "Put" or "Append"
	Key      string // "Key" for the "Value"
	Value    string // empty for "Get"
	ClientId int64  // who assigns this Op
	SN       int    // serial number for this Op
	SID      int    // shard id responsible for this Op
}

type Migration struct {
	Type     string // "In" or "Done"
	GID      int64  // who assigns this Op
	Num      int    // serial number for this Op
	SIDs     []int
	Data     []map[string]string // shard data replicated by leader
	DupTable []map[int64]DupEntry
}

type DupEntry struct { // record the executed request
	SN    int
	Value string
	Err   Err
}

type opResult struct {
	ClientId int64  // who assigns this result
	SN       int    // serial number for this result
	SID      int    // shard id responsible for this result
	Value    string // empty for "Get"
	Err      Err    // err message
}

type Shard struct {
	Status ShardStatus
	Data   map[string]string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	sm           *shardctrler.Clerk
	dead         int32 // set by killed()
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Persistent state on snapshot, capitalize for encoding
	Shards            []Shard              // shard -> data
	DupTables         []map[int64]DupEntry // table for duplicated check
	Config            shardctrler.Config   // current config
	lastIncludedIndex int                  // last snapshot index

	// Volatile state on all server.
	resultCh            map[int]chan opResult // transfer result to RPC
	lastApplied         int                   // lastApplied log index
	lastMigrationNum int

	// Channels
	pollTrigger      chan bool
	migrationTrigger chan bool
	snapshotTrigger  chan bool
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
	labgob.Register(ClientOp{})
	labgob.Register(Migration{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(DupEntry{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.sm = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.pollTrigger = make(chan bool)
	kv.migrationTrigger = make(chan bool)
	kv.snapshotTrigger = make(chan bool)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.resultCh = make(map[int]chan opResult)

	kv.DupTables = make([]map[int64]DupEntry, shardctrler.NShards)
	for i, _ := range kv.DupTables {
		kv.DupTables[i] = make(map[int64]DupEntry)
	}

	kv.Shards = make([]Shard, shardctrler.NShards)
	for i, _ := range kv.Shards {
		kv.Shards[i].Status = ShardOK
		kv.Shards[i].Data = make(map[string]string)
	}

	kv.ingestSnap(persister.ReadSnapshot())

	go kv.snapshoter(persister, maxraftstate)
	go kv.applier(kv.applyCh)
	go kv.migrationer(kv.migrationTrigger)
	go kv.pollTicker(kv.pollTrigger)

	signalCh(kv.pollTrigger, true)

	return kv
}

// handle one Op received by Get or PutAppend RPC.
// first, it performs duplicated detection. if not, it goes to next step.
// if current server is the leader, it will replicate the log through Raft, and update the key/value pairs based on the Op.
// finally, it returns response info in Op for next same Op check.
func (kv *ShardKV) doit(op *Op) opResult {
	result := opResult{}

	kv.mu.Lock()

	if pl, ok := op.Playload.(ClientOp); ok { // client op
		result.ClientId = pl.ClientId
		result.SN = pl.SN
		result.SID = pl.SID
		// the follower should have the ability to detect duplicate before redirect to leader.
		// if it is a up-to-date follower, it is safe to do so.
		// if it is a stale follower, it is still safe to do so, because:
		// 1. if it has this entry, implies its log has been updated to this request
		// 2. if it does not, it will be redirect to other up-to-date server.
		// if it is a stale leader, this request will timeout and redirect to other serser.
		if dEntry, ok := kv.DupTables[pl.SID][pl.ClientId]; ok { // duplicated detection
			if dEntry.SN == pl.SN {
				result.Value = dEntry.Value
				result.Err = OK
				kv.mu.Unlock()
				return result
			}
		}

		// check if the replica group is responsible or ready for this client op
		if kv.Config.Shards[pl.SID] != kv.gid || kv.Shards[pl.SID].Status != ShardOK {
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

	DPrintf("(%d:%d) call op: %v at index %d", kv.gid, kv.me, op, index)

	// must create reply channel before unlock
	ch := make(chan opResult)
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

	clientOp := ClientOp{
		Type:     "Get",
		Key:      args.Key,
		ClientId: args.ClientId,
		SN:       args.SN,
		SID:      args.SID,
	}

	result := kv.doit(&Op{Playload: clientOp})

	// Optimation: reply if it is a same op even though the leader may change
	if result.SID == args.SID && result.ClientId == args.ClientId && result.SN == args.SN {
		reply.Value = result.Value
		reply.Err = result.Err
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	clientOp := ClientOp{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SN:       args.SN,
		SID:      args.SID,
	}
	result := kv.doit(&Op{Playload: clientOp})

	// Optimation: reply if it is a same op even though the leader may change
	if result.SID == args.SID && result.ClientId == args.ClientId && result.SN == args.SN {
		reply.Err = result.Err
	}
}

// ingest one command, and update the state of storage.
// transfer back the result by OpCh.
func (kv *ShardKV) ingestCommand(index int, command interface{}) {
	op := command.(Op)
	result := opResult{Err: OK}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer func() {
		// send the result back if this server has channel
		// no matter whether it is a duplicated or new request to avoid resource leaks
		// however, for example, when server 1 was partitioned and start a request for client 1 with SN 1
		// when server 1 come back and apply other log (client 2 with SN 1) with same log index
		// should check if it is the right result received by this channel
		if ch, ok := kv.resultCh[index]; ok {
			ch <- result
			delete(kv.resultCh, index)
		}
		if (kv.lastApplied - kv.lastIncludedIndex) >= SnapCheckpoint {
			select {
			case kv.snapshotTrigger <- true:
			default:
			}

		}
	}()

	kv.lastApplied = index // update lastApplied index

	switch pl := op.Playload.(type) {
	case ClientOp:
		result.ClientId = pl.ClientId
		result.SN = pl.SN
		result.SID = pl.SID
		// if a duplicate request arrives before the original executes
		// don't execute if table says already seen
		if dEntry, ok := kv.DupTables[pl.SID][pl.ClientId]; ok && dEntry.SN >= pl.SN {
			// it is safe to ignore the lower SN request,
			// since the sender has received the result for this SN,
			// and has sent the higher SN for another request.
			if dEntry.SN == pl.SN {
				result.Err = dEntry.Err
				result.Value = dEntry.Value
			}
		} else { // new request
			if kv.Config.Shards[pl.SID] != kv.gid || kv.Shards[pl.SID].Status != ShardOK {
				result.Err = ErrWrongGroup
				DPrintf("(%d:%d) dangerous request: %v", kv.gid, kv.me, op)
				return
			}

			switch pl.Type {
			case "Get":
				value, ok := kv.Shards[pl.SID].Data[pl.Key]
				if ok {
					result.Value = value
				} else {
					result.Err = ErrNoKey
				}
			case "Put":
				kv.Shards[pl.SID].Data[pl.Key] = pl.Value
			case "Append":
				kv.Shards[pl.SID].Data[pl.Key] += pl.Value
			default:
				panic(op)
			}
		}

		kv.DupTables[pl.SID][result.ClientId] = DupEntry{
			SN:    result.SN,
			Value: result.Value,
			Err:   result.Err,
		}
	case shardctrler.Config:
		if needMigration := kv.applyConfig(pl); needMigration {
			signalCh(kv.migrationTrigger, true)
		}
	case Migration:
		result.ClientId = pl.GID
		result.SN = pl.Num

		if pl.Num != kv.Config.Num { // can't apply this migration
			return
		}

		if pl.Type == "Done" {
			for i := range kv.Shards {
				if kv.Shards[i].Status == ShardMigrationOut {
					kv.Shards[i].Status = ShardOK
				}
			}
			if in, out := kv.checkAllShardsStatus(); !in && !out { // all shards are ok, try if there is any new config
				signalCh(kv.pollTrigger, true)
			}
			return
		}

		for _, sid := range pl.SIDs {
			if kv.Shards[sid].Status == ShardMigrationIn {
				kv.DupTables[sid] = copyOfDupTalbe(pl.DupTable[sid])
				kv.Shards[sid].Data = copyOfData(pl.Data[sid])
				kv.Shards[sid].Status = ShardOK
				DPrintf("(%d:%d) install %d migration: %v", kv.gid, kv.me, sid, op)
			}
			if in, out := kv.checkAllShardsStatus(); !in && !out { // all shards are ok, try if there is any new config
				signalCh(kv.pollTrigger, true)
			}
		}
	default:
		panic(op)
	}
}

// prepare for migration by update Config and shard status, halt the shards that need migration,
// return a map(gid->shards) that indicates the shards need to migrate out
func (kv *ShardKV) applyConfig(newConfig shardctrler.Config) bool {
	if newConfig.Num <= kv.Config.Num {
		return false
	}

	in, out := kv.checkAllShardsStatus()
	if out {
		return true
	}
	if in {
		return false
	}

	needMigrationOut := false
	DPrintf("(%d:%d) new config: %v, last: %v", kv.gid, kv.me, newConfig, kv.Config)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.Shards[i].Status = ShardOK // reset all the shards' status
		if kv.Config.Shards[i] == newConfig.Shards[i] {
			continue
		}
		if kv.Config.Shards[i] == kv.gid { // halt the shards and check whether it need to migrate out
			kv.Shards[i].Status = ShardMigrationOut
			needMigrationOut = true
		}
		if newConfig.Shards[i] == kv.gid && kv.Config.Shards[i] != 0 { // halt the shards that need to migrate in
			kv.Shards[i].Status = ShardMigrationIn
		}
	}
	kv.Config = newConfig

	return needMigrationOut
}

// install the snapshot.
func (kv *ShardKV) ingestSnap(snapshot []byte) {
	if len(snapshot) == 0 {
		return // ignore empty snapshot
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var shards []Shard
	var dupTables []map[int64]DupEntry
	var config shardctrler.Config
	var lastIncludedIndex int
	if d.Decode(&shards) != nil ||
		d.Decode(&dupTables) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&lastIncludedIndex) != nil {
		log.Fatalf("snapshot decode error")
	}
	DPrintf("(%d:%d) decode config: %v, shards: %v", kv.gid, kv.me, config, shards)
	kv.mu.Lock()
	kv.Shards = shards
	kv.DupTables = dupTables
	kv.Config = config
	kv.lastIncludedIndex = lastIncludedIndex
	kv.lastApplied = lastIncludedIndex
	kv.mu.Unlock()
	DPrintf("(%d:%d) finish decode", kv.gid, kv.me)
}

// this function acts at a long running goroutine,
// accepts ApplyMsg from Raft through applyCh.
// if it is a command, it will update the state of storage, and check the necessity to take a snapshot.
// if it is a snapshot, it will install the snapshot.
func (kv *ShardKV) applier(applyCh chan raft.ApplyMsg) {

	for m := range applyCh {

		if m.CommandValid {
			DPrintf("(%d:%d) apply command: %v at %d", kv.gid, kv.me, m.Command, m.CommandIndex)
			kv.ingestCommand(m.CommandIndex, m.Command)
		} else if m.SnapshotValid && kv.lastApplied < m.SnapshotIndex { // no need lock here
			DPrintf("(%d:%d) apply snapshot at %d and lastApplied: %d", kv.gid, kv.me, m.SnapshotIndex, kv.lastApplied)
			kv.ingestSnap(m.Snapshot)
		}
	}
}

func (kv *ShardKV) snapshoter(persister *raft.Persister, maxraftstate int) {
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
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)

			kv.mu.Lock()
			kv.lastIncludedIndex = kv.lastApplied
			index := kv.lastIncludedIndex
			if e.Encode(kv.Shards) != nil ||
				e.Encode(kv.DupTables) != nil ||
				e.Encode(kv.Config) != nil ||
				e.Encode(kv.lastIncludedIndex) != nil {
				log.Fatalf("snapshot encode error")
			}
			DPrintf("(%d:%d) snapshot at %d and size: %d", kv.gid, kv.me, kv.lastApplied, persister.RaftStateSize())
			kv.mu.Unlock()

			kv.rf.Snapshot(index, w.Bytes())
		}
	}

}

func (kv *ShardKV) pollTicker(pollTrigger chan bool) {
	for !kv.killed() {

		select {
		case <-pollTrigger:
		case <-time.After(time.Duration(PollInterval) * time.Millisecond):
		}

		if kv.killed() {
			return
		}

		kv.mu.Lock()

		// process re-configurations one at a time, in order.
		newConfig := kv.sm.Query(kv.Config.Num + 1)

		if newConfig.Num == kv.Config.Num {
			kv.mu.Unlock()
			continue
		}

		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.rf.Start(Op{Playload: newConfig})
		}

		kv.mu.Unlock()
	}
}

func (kv *ShardKV) ShardMigration(args *ShardMigrationArgs, reply *ShardMigrationReply) {

	kv.mu.Lock()
	reply.Num = kv.Config.Num

	if args.Num > kv.Config.Num {
		signalCh(kv.pollTrigger, true)
		reply.Err = ErrUpdatingConfig
		kv.mu.Unlock()
		return
	}

	if args.Num < kv.Config.Num {
		reply.Err = ErrOutdatedConfig
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	migration := Migration{
		Type:     "In",
		GID:      args.GID,
		Num:      args.Num,
		SIDs:     args.SIDs,
		Data:     args.Data,
		DupTable: args.DupTables,
	}

	result := kv.doit(&Op{Playload: migration})
	// Optimation: reply if it is a same op even though the leader may change
	if result.ClientId == args.GID && result.SN == args.Num {
		reply.Err = result.Err
		DPrintf("(%d:%d) receive migration:%v", kv.gid, kv.me, migration)
	}
}

func (kv *ShardKV) migrationer(migrationTrigger chan bool) {
	for !kv.killed() {

		<-migrationTrigger

		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}

		kv.mu.Lock()
		if kv.lastMigrationNum != kv.Config.Num { // avoid creating too many goroutines
			kv.lastMigrationNum = kv.Config.Num
			go kv.startMigrationOut(kv.lastMigrationNum)
			DPrintf("(%d:%d) start migration out for num: %d", kv.gid, kv.me, kv.lastMigrationNum)
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) startMigrationOut(num int) {
	shardsOut := make(map[int][]int) // gid -> shards
	kv.mu.Lock()
	for i, shard := range kv.Shards {
		if shard.Status == ShardMigrationOut {
			shardsOut[kv.Config.Shards[i]] = append(shardsOut[kv.Config.Shards[i]], i)
		}
	}
	kv.mu.Unlock()

	for {

		if _, isLeader := kv.rf.GetState(); !isLeader {
			return
		}

		kv.mu.Lock()

		if num != kv.Config.Num {
			kv.mu.Unlock()
			return
		}

		if len(shardsOut) == 0 {
			migrationDone := Migration{
				Type: "Done",
				Num:  kv.Config.Num,
			}
			kv.rf.Start(Op{Playload: migrationDone})
			DPrintf("(%d:%d) migration out for num: %d finish", kv.gid, kv.me, kv.lastMigrationNum)
			kv.mu.Unlock()
			return
		}

		for gid, sids := range shardsOut {
			servers := kv.Config.Groups[gid]
			data := make([]map[string]string, shardctrler.NShards)
			dupTables := make([]map[int64]DupEntry, shardctrler.NShards)
			for _, sid := range sids {
				data[sid] = copyOfData(kv.Shards[sid].Data)
				dupTables[sid] = copyOfDupTalbe(kv.DupTables[sid])
			}
			args := ShardMigrationArgs{
				GID:       int64(kv.gid),
				Num:       kv.Config.Num, // use config Num as Serial number here
				SIDs:      sids,
				Data:      data,
				DupTables: dupTables,
			}
			go func(gid int, servers []string, args *ShardMigrationArgs) {
				reply := ShardMigrationReply{}
				if ok := kv.sendShardMigration(servers, args, &reply); ok {
					delete(shardsOut, gid)
				}
			}(gid, servers, &args)
		}

		kv.mu.Unlock()
		time.Sleep(time.Duration(MigrationInterval) * time.Millisecond)
	}
}

func (kv *ShardKV) sendShardMigration(servers []string, args *ShardMigrationArgs, reply *ShardMigrationReply) bool {

	if _, isLeader := kv.rf.GetState(); !isLeader {
		return false
	}

	DPrintf("(%d:%d) send migration with args: %v", kv.gid, kv.me, args)
	for si := 0; si < len(servers); si++ {
		srv := kv.make_end(servers[si])
		ok := srv.Call("ShardKV.ShardMigration", args, reply)
		DPrintf("(%d:%d) get reply: %v from: %d args: %v", kv.gid, kv.me, reply, si, args)
		if ok && (reply.Err == OK || reply.Err == ErrOutdatedConfig) {
			kv.mu.Lock()
			if kv.Config.Num == args.Num {
				kv.mu.Unlock()
				return true
			}
		}
	}
	return false
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

func copyOfDupTalbe(dupTable map[int64]DupEntry) map[int64]DupEntry {
	result := make(map[int64]DupEntry)
	for k, v := range dupTable {
		result[k] = v
	}
	return result
}

// check if all shards are OK
// thread-unsafe, need lock
func (kv *ShardKV) checkAllShardsStatus() (bool, bool) {
	in, out := false, false
	for _, shard := range kv.Shards {
		if shard.Status == ShardMigrationIn {
			in = true
		}
		if shard.Status == ShardMigrationOut {
			out = true
		}

	}
	return in, out
}
