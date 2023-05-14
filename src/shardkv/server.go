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

type Op struct { // op pass to Raft as command
	Playload interface{}
}

type ClientOp struct { // client request's playload
	Type     string // "Get", "Put" or "Append"
	Key      string // "Key" for the "Value"
	Value    string // empty for "Get"
	ClientId int64  // who assigns this Op
	SN       int    // serial number for this Op
	SID      int    // shard id responsible for this Op
}

type MigrationIn struct { // migrationIn's playload, indicates migration begin
	Num    int            // config num for this migration
	GID    int64          // who provides this migration
	Shards map[int]*Shard // shard data replicated by leader
}

type MigrationOut struct { // migrationOut's playload, indicates migration end
	Num  int   // config num for this migration
	SIDs []int // shards that have finished migration
}

type DupEntry struct { // record the executed client request
	SN    int    // serial number
	Value string // empty for "Get"
	Err   Err    // err message
}

type opResult struct { // op executed result
	ClientId int64  // who assigns this op
	SN       int    // serial number for this op
	SID      int    // shard id responsible for this op
	Value    string // empty for "Get"
	Err      Err    // err message
}

type Shard struct { // single shard data
	Status   ShardStatus        // current shard status
	Data     map[string]string  // Key/Value data
	DupTable map[int64]DupEntry // duplicated table for this shard
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
	Shards            map[int]*Shard     // sid -> shard
	Config            shardctrler.Config // current config
	lastIncludedIndex int                // last snapshot index

	// Volatile state on all server.
	resultCh         map[int]chan opResult // transfer result to RPC
	lastApplied      int                   // lastApplied log index
	lastMigrationNum int                   // last migration config number
	pollTrigger      chan bool             // signal ch for fetching new config
	migrationTrigger chan bool             // signal ch for starting migration
	snapshotTrigger  chan bool             // signal ch for taking a snapshot
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

// test whether the service has been killed
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
	labgob.Register(MigrationIn{})
	labgob.Register(MigrationOut{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(DupEntry{})

	kv := new(ShardKV)
	kv.me = me
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.sm = shardctrler.MakeClerk(kv.ctrlers)
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.pollTrigger = make(chan bool)
	kv.migrationTrigger = make(chan bool)
	kv.snapshotTrigger = make(chan bool)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.resultCh = make(map[int]chan opResult)
	kv.Shards = make(map[int]*Shard)

	kv.ingestSnap(persister.ReadSnapshot())

	go kv.snapshoter(persister, maxraftstate)
	go kv.applier(kv.applyCh)
	go kv.migrationer(kv.migrationTrigger)
	go kv.pollTicker(kv.pollTrigger)

	signalCh(kv.pollTrigger, true)

	return kv
}

// handle one Op received by Get, PutAppend or ShardMigration RPC.
// first, it performs duplicated detection. if not, it goes to next step.
// if current server is the leader, it will replicate the log through Raft, and update the key/value pairs based on the Op.
// finally, it returns response info of Op for the same Op check.
func (kv *ShardKV) doit(op *Op) opResult {
	result := opResult{}

	kv.mu.Lock()

	if pl, ok := op.Playload.(ClientOp); ok { // client op
		result.ClientId = pl.ClientId
		result.SN = pl.SN
		result.SID = pl.SID

		// check if the replica group is responsible or ready for this client op
		if kv.Config.Shards[pl.SID] != kv.gid || kv.Shards[pl.SID].Status != ShardOK {
			result.Err = ErrWrongGroup
			kv.mu.Unlock()
			return result
		}

		// the follower should have the ability to detect duplicate before redirect to leader.
		// if it is a up-to-date follower, it is safe to do so.
		// if it is a stale follower, it is still safe to do so, because:
		// 1. if it has this entry, implies its log has been updated to this request
		// 2. if it does not, it will be redirect to other up-to-date server.
		// if it is a stale leader, this request will timeout and redirect to other serser.
		if dEntry, ok := kv.Shards[pl.SID].DupTable[pl.ClientId]; ok { // duplicated detection
			if dEntry.SN == pl.SN {
				result.Value = dEntry.Value
				result.Err = OK
				kv.mu.Unlock()
				return result
			}
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
// transfer back the result by opResult Channel.
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

		if kv.Config.Shards[pl.SID] != kv.gid || kv.Shards[pl.SID].Status != ShardOK {
			result.Err = ErrWrongGroup
			return
		}
		// if a duplicate request arrives before the original executes
		// don't execute if table says already seen
		if dEntry, ok := kv.Shards[pl.SID].DupTable[pl.ClientId]; ok && dEntry.SN >= pl.SN {
			// it is safe to ignore the lower SN request,
			// since the sender has received the result for this SN,
			// and has sent the higher SN for another request.
			if dEntry.SN == pl.SN {
				result.Err = dEntry.Err
				result.Value = dEntry.Value
			}
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

		kv.Shards[pl.SID].DupTable[result.ClientId] = DupEntry{
			SN:    result.SN,
			Value: result.Value,
			Err:   result.Err,
		}
	case shardctrler.Config:
		if needMigration := kv.applyConfig(pl); needMigration {
			signalCh(kv.migrationTrigger, true)
		}
	case MigrationIn:
		result.ClientId = pl.GID
		result.SN = pl.Num

		if pl.Num != kv.Config.Num { // can't apply this migration
			return
		}

		for sid, shard := range pl.Shards {
			if kv.Shards[sid].Status == ShardMigrationIn {
				kv.Shards[sid] = copyOfShard(shard)
				kv.Shards[sid].Status = ShardOK
				DPrintf("(%d:%d) install %d migration: %v", kv.gid, kv.me, sid, op)
				DPrintf("(%d:%d) current shards: %v", kv.gid, kv.me, kv.Shards[sid])
			}
		}
		if in, out := kv.checkAllShardsStatus(); len(in) == 0 && len(out) == 0 { // all shards are ok, try if there is any new config
			signalCh(kv.pollTrigger, true)
		}
	case MigrationOut:
		if pl.Num == kv.Config.Num {
			for _, i := range pl.SIDs {
				delete(kv.Shards, i)
			}
		}
		if in, out := kv.checkAllShardsStatus(); len(in) == 0 && len(out) == 0 { // all shards are ok, try if there is any new config
			signalCh(kv.pollTrigger, true)
		}
	default:
		panic(op)
	}
}

// apply the config received by applier.
// if the config is applicable, set up the shards that need to migrate in or out.
func (kv *ShardKV) applyConfig(newConfig shardctrler.Config) bool {
	if newConfig.Num <= kv.Config.Num {
		return false
	}

	in, out := kv.checkAllShardsStatus()
	if len(out) > 0 { // current config need migration out
		return true
	}
	if len(in) > 0 { // current config need migration in
		return false
	}

	needMigration := false
	DPrintf("(%d:%d) new config: %v, last: %v", kv.gid, kv.me, newConfig, kv.Config)
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.Config.Shards[i] == newConfig.Shards[i] {
			continue
		}
		if kv.Config.Shards[i] == kv.gid { // halt the shards and check whether it need to migrate out
			kv.Shards[i].Status = ShardMigrationOut
			needMigration = true
		}
		if newConfig.Shards[i] == kv.gid { // halt the shards that need to migrate in
			kv.Shards[i] = &Shard{
				Status:   ShardMigrationIn,
				Data:     make(map[string]string),
				DupTable: make(map[int64]DupEntry),
			}
			if kv.Config.Shards[i] == 0 {
				kv.Shards[i].Status = ShardOK
			}
		}
	}
	kv.Config = newConfig

	return needMigration
}

// install the snapshot.
func (kv *ShardKV) ingestSnap(snapshot []byte) {
	if len(snapshot) == 0 {
		return // ignore empty snapshot
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var shards map[int]*Shard
	var config shardctrler.Config
	var lastIncludedIndex int
	if d.Decode(&shards) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&lastIncludedIndex) != nil {
		log.Fatalf("snapshot decode error")
	}
	DPrintf("(%d:%d) decode config: %v, shards: %v", kv.gid, kv.me, config, shards)
	kv.mu.Lock()
	kv.Shards = shards
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

// this function acts at a long running goroutine,
// take a snapshot periodically or triggered by signal.
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

// this function acts at a long running goroutine,
// fetch the new config periodically or triggered by signal.
// if it is a new config (larger config number), start a new agreement through Raft.
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

// ShardMigration handler.
// if the migration can be applied(same config number), accepts it.
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

	migrationIn := MigrationIn{
		GID:    args.GID,
		Num:    args.Num,
		Shards: args.Shards,
	}

	result := kv.doit(&Op{Playload: migrationIn})
	// Optimation: reply if it is a same op even though the leader may change
	if result.ClientId == args.GID && result.SN == args.Num {
		reply.Err = result.Err
		DPrintf("(%d:%d) receive migration:%v", kv.gid, kv.me, migrationIn)
	}
}

// this function acts at a long running goroutine,
// if accepts signal from migrationTrigger channel, start migration out for current config.
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
		}
		kv.mu.Unlock()
	}
}

// start migraion out for a config number,
// it will keep migrating out every MigrationInterval until finished.
func (kv *ShardKV) startMigrationOut(num int) {
	defer func() {
		DPrintf("(%d:%d) exit migration out for num: %d", kv.gid, kv.me, num)
	}()

	for !kv.killed() {

		if _, isLeader := kv.rf.GetState(); !isLeader {
			return
		}

		kv.mu.Lock()

		_, out := kv.checkAllShardsStatus()

		if num != kv.Config.Num || len(out) == 0 {
			kv.mu.Unlock()
			return
		}

		// divide the migration out shards into gids
		shardsOut := make(map[int][]int) // gid -> sids
		for _, sid := range out {
			gid := kv.Config.Shards[sid]
			shardsOut[gid] = append(shardsOut[gid], sid)
		}

		DPrintf("(%d:%d) start migration out for num: %d, shardsOut: %v", kv.gid, kv.me, num, shardsOut)
		for gid, sids := range shardsOut {
			servers := kv.Config.Groups[gid]
			args := kv.buildShardMigrationArgs(sids)
			reply := &ShardMigrationReply{}
			go kv.sendShardMigration(servers, args, reply)
		}

		kv.mu.Unlock()
		time.Sleep(MigrationInterval * time.Millisecond)
	}
}

// call the ShardMigration handler,
// if the reply is OK or ErrOutdatedConfig, start the MigrationOut op,
// indicates this migration out has finished.
func (kv *ShardKV) sendShardMigration(servers []string, args *ShardMigrationArgs, reply *ShardMigrationReply) {

	for si := 0; si < len(servers); si++ {
		srv := kv.make_end(servers[si])
		ok := srv.Call("ShardKV.ShardMigration", args, reply)
		if ok && (reply.Err == OK || reply.Err == ErrOutdatedConfig) {
			kv.mu.Lock()
			if kv.Config.Num == args.Num {
				migrationOut := MigrationOut{
					Num:  args.Num,
					SIDs: args.SIDs,
				}
				kv.mu.Unlock()
				kv.rf.Start(Op{Playload: migrationOut})
				DPrintf("(%d:%d) done migration out for sids: %v(num:%d) ", kv.gid, kv.me, args.SIDs, args.Num)
				break
			}
			kv.mu.Unlock()
		}
	}
}

// build shard migration args for a group of shards
// thread-unsafe, need lock
func (kv *ShardKV) buildShardMigrationArgs(sids []int) *ShardMigrationArgs {
	args := ShardMigrationArgs{
		GID:    int64(kv.gid),
		Num:    kv.Config.Num, // use config Num as Serial number here
		SIDs:   sids,
		Shards: make(map[int]*Shard),
	}

	for _, sid := range sids {
		args.Shards[sid] = copyOfShard(kv.Shards[sid])
	}
	return &args
}

// transfer the signal without blocking
func signalCh(ch chan bool, val bool) {
	select {
	case ch <- val:
	default:
	}
}

// return the copy of a single shard by pointer
// thread-unsafe, need lock
func copyOfShard(shard *Shard) *Shard {
	result := Shard{
		Status:   shard.Status,
		Data:     make(map[string]string),
		DupTable: make(map[int64]DupEntry),
	}
	for k, v := range shard.Data {
		result.Data[k] = v
	}
	for k, v := range shard.DupTable {
		result.DupTable[k] = v
	}
	return &result
}

// check if all shards are OK
// return the shards that need to migration in and out
// thread-unsafe, need lock
func (kv *ShardKV) checkAllShardsStatus() ([]int, []int) {
	in, out := make([]int, 0), make([]int, 0)
	for i, shard := range kv.Shards {
		if shard.Status == ShardMigrationIn {
			in = append(in, i)
		}
		if shard.Status == ShardMigrationOut {
			out = append(out, i)
		}
	}
	return in, out
}
