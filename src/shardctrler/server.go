package shardctrler

import (
	"log"
	"sort"
	"sync"
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
	Type string // "Join", "Leave", "Move" or "Query"
	Servers map[int][]string // Join arg
	GIDs []int // Leave arg
	Shard int // Move arg
	GID   int // Move arg
	Num int // Query arg
	ClientId int64 // who assigns this Op
	SN int // serial number for this Op
}

type DupEntry struct { // record the executed request
	SN int 
	Err Err
	Config Config
}

type doitResult struct {
	ClientId int64 // who assigns this Op
	SN int // serial number for this Op
	Err Err
	Config Config
}

// The shardctrler manages a sequence of numbered configurations. 
// Each configuration describes a set of replica groups and an assignment of shards to replica groups. 
// Whenever this assignment needs to change, the shard controller creates a new configuration with the new assignment. 
// Key/value clients and servers contact the shardctrler when they want to know the current (or a past) configuration.
type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs []Config // indexed by config num
	DupTable map[int64]DupEntry // table for duplicated check

	// Volatile state on all server.
	resultCh map[int]chan doitResult // transfer result to RPC
	lastApplied int // lastApplied log index
}

// handle one Op received by Join, Leave, Move, or Query RPC.
// first, it performs duplicated detection. if not, it goes to next step.
// if current server is the leader, it will replicate the log through Raft, and execute the Op.
// finally, it returns response info in Op for the same Op check.
func (sc *ShardCtrler) doit(op *Op) doitResult {
	result := doitResult{ClientId: op.ClientId, SN: op.SN}
	sc.mu.Lock()

	// the follower should have the ability to detect duplicate before redirect to leader.
	// if it is a up-to-date follower, it is safe to do so.
	// if it is a stale follower, it is still safe to do so, because:
	// 1. if it has this entry, implies its log has been updated to this request
	// 2. if it does not, it will be redirect to other up-to-date server.
	// if it is a stale leader, this request will timeout and redirect to other serser.
	if dEntry, ok := sc.DupTable[op.ClientId]; ok { // duplicated detection
		if dEntry.SN == op.SN {
			result.Err = OK
			result.Config = sc.DupTable[op.ClientId].Config
			sc.mu.Unlock()
			return result
		} 
	}

	index, _, isLeader := sc.rf.Start(*op)

	if !isLeader { // check if it is leader
		result.Err = ErrWrongLeader
		sc.mu.Unlock()
		return result
	} 

	// must create reply channel before unlock
	ch := make(chan doitResult)
	sc.resultCh[index] = ch
	sc.mu.Unlock()

	DPrintf("%d start doit: %v", sc.me, op)
	select {
	case result = <-ch:
	case <-time.After(time.Duration(ResponseTimeout) * time.Millisecond):
		result.Err = ErrWrongLeader // if we don't get a reponse in time, leader may be dead
	}

	go func() { // unblock applier
		<-ch
	}()

	DPrintf("%d receive result: %v for op: %v", sc.me, result, op)
	return result
}

// The Join RPC is used by an administrator to add new replica groups. 
// Its argument is a set of mappings from unique, non-zero replica group identifiers (GIDs) to lists of server names. 
// The shardctrler should react by creating a new configuration that includes the new replica groups. 
// The new configuration should divide the shards as evenly as possible among the full set of groups, 
// and should move as few shards as possible to achieve that goal. 
// The shardctrler should allow re-use of a GID if it's not part of the current configuration 
// (i.e. a GID should be allowed to Join, then Leave, then Join again).
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	
	op := Op {
		Type: "Join",
		Servers: args.Servers,
		ClientId: args.ClientId,
		SN: args.SN,
	}
	
	result := sc.doit(&op)

	if result.ClientId == args.ClientId && result.SN == args.SN {
		reply.Err = result.Err
	}
}

// The Leave RPC's argument is a list of GIDs of previously joined groups. 
// The shardctrler should create a new configuration that does not include those groups, 
// and that assigns those groups' shards to the remaining groups. 
// The new configuration should divide the shards as evenly as possible among the groups, 
// and should move as few shards as possible to achieve that goal.
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op {
		Type: "Leave",
		GIDs: args.GIDs,
		ClientId: args.ClientId,
		SN: args.SN,
	}
	
	result := sc.doit(&op)

	if result.ClientId == args.ClientId && result.SN == args.SN {
		reply.Err = result.Err
	}
}

// The Move RPC's arguments are a shard number and a GID. 
// The shardctrler should create a new configuration in which the shard is assigned to the group.
// The purpose of Move is to allow us to test your software. 
// A Join or Leave following a Move will likely un-do the Move, since Join and Leave re-balance.
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op {
		Type: "Move",
		Shard: args.Shard,
		GID: args.GID,
		ClientId: args.ClientId,
		SN: args.SN,
	}
	
	result := sc.doit(&op)

	if result.ClientId == args.ClientId && result.SN == args.SN {
		reply.Err = result.Err
	}
}

// The Query RPC's argument is a configuration number. 
// The shardctrler replies with the configuration that has that number. 
// If the number is -1 or bigger than the biggest known configuration number, 
// the shardctrler should reply with the latest configuration. 
// The result of Query(-1) should reflect every Join, Leave, 
// or Move RPC that the shardctrler finished handling before it received the Query(-1) RPC.
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op {
		Type: "Query",
		Num: args.Num,
		ClientId: args.ClientId,
		SN: args.SN,
	}

	result := sc.doit(&op)

	if result.ClientId == args.ClientId && result.SN == args.SN {
		reply.Err = result.Err
		reply.Config = result.Config
	}
}


// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.DupTable = make(map[int64]DupEntry)
	sc.resultCh = make(map[int]chan doitResult)

	go sc.applier(persister)

	return sc
}

// this function acts at a long running goroutine,
// accepts ApplyMsg from Raft through applyCh.
// if it is a command, it will update the state of storage, and check the necessity to take a snapshot.
// if it is a snapshot, it will install the snapshot.
func (sc *ShardCtrler) applier(persister *raft.Persister) {

	for m := range sc.applyCh {

		if m.CommandValid {
			DPrintf("%d appliy command %v", sc.me, m)
			sc.ingestCommand(m.CommandIndex, m.Command)
		}
	}
}

func (sc *ShardCtrler) ingestCommand(index int, command interface{}) {
	op := command.(Op)
	result := doitResult{ClientId: op.ClientId, SN: op.SN, Err: OK}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.lastApplied = index // update lastApplied index

	// if a duplicate request arrives before the original executes
	// don't execute if table says already seen
	if dEntry, ok := sc.DupTable[op.ClientId]; ok && dEntry.SN >= op.SN { 
		// it is safe to ignore the lower SN request,
		// since the sender has received the result for this SN, 
		// and has sent the higher SN for another request.
		if dEntry.SN == op.SN { 
			result.Config = dEntry.Config
		}
	} else {

		switch op.Type {
		case "Join":
			newConfig := sc.createConfig()
			for k, v := range op.Servers { // add new replica groups
				newConfig.Groups[k] = v
			}
			sc.configs = append(sc.configs, newConfig)
			sc.shard()
		case "Leave":
			newConfig := sc.createConfig()
			for _, v := range op.GIDs {
				delete(newConfig.Groups, v)	// remove replica groups
			}
			sc.configs = append(sc.configs, newConfig)
			sc.shard()
		case "Query":
			latestNum := sc.configs[len(sc.configs) - 1].Num
			if op.Num == -1 || op.Num >= latestNum {
				result.Config = sc.configs[len(sc.configs) - 1]
			} else {
				result.Config = sc.configs[op.Num]
			}
		case "Move":
			newConfig := sc.createConfig()
			lastConfig := sc.configs[len(sc.configs) - 1]
			copy(newConfig.Shards[:], lastConfig.Shards[:])
			newConfig.Shards[op.Shard] = op.GID
			sc.configs = append(sc.configs, newConfig)
		default:
			log.Fatal(op)
		}

		sc.DupTable[op.ClientId] = DupEntry{ // record the result
			SN: result.SN,
			Err: result.Err,
			Config: result.Config,
		}
	}

	// send the result back if this server has channel 
	// no matter whether it is a duplicated or new request to avoid resource leaks
	if ch, ok := sc.resultCh[index]; ok { 
		//DPrintf("%d current cofig: %v", sc.me, sc.configs)
		ch <- result
	}

	delete(sc.resultCh, index)
}

// create a new configuration that includes the same replica groups as last config.
// thread unsafe, need lock.
func (sc *ShardCtrler) createConfig() Config {
	lastConfig := sc.configs[len(sc.configs) - 1]
	newConfig := Config{ Num: lastConfig.Num + 1, Groups: make(map[int][]string) }

	for k, v := range lastConfig.Groups {
		newConfig.Groups[k] = v
	}
	return newConfig
}

// shard the new configuration.
// divide the shards as evenly as possible among the groups, 
// and move as few shards as possible to achieve that goal.
// thread unsafe, need lock.
func (sc *ShardCtrler) shard() {
	newConfig := &sc.configs[len(sc.configs) - 1]
	lastConfig := sc.configs[len(sc.configs) - 2]
	// need to sort the gids here, image when the number of gids > shards
	// the smaller gids will have higher priority to be assigned a shard after increasing sort
	newGIDs := make([]int, 0)
	for k, _ := range newConfig.Groups {
		newGIDs = append(newGIDs, k)
 	}
	sort.Ints(newGIDs)

	groups := len(newConfig.Groups)
	if groups == 0 { // all grouds have been removed
		return 
	} 
	
	quotient := NShards / groups
	remainder := NShards % groups

	cnt := map[int]int{} // gip -> num of shards
	// first, count the groups that exist in both last shards and new config
	for _, gid := range lastConfig.Shards {
		if _, ok := newConfig.Groups[gid]; !ok {
			continue
		}
		// only count the gid that hasn't been recorded
		if _, ok := cnt[gid]; !ok { 
			if remainder > 0 {
				cnt[gid] = quotient + 1
				remainder--
			} else {
				cnt[gid] = quotient
			}
		}
	}

	// second, count the groups that only exist in new config
	for _, gid := range newGIDs {
		// only count the gid that hasn't been recorded
		if _, ok := cnt[gid]; !ok { 
			if remainder > 0 {
				cnt[gid] = quotient + 1
				remainder--
			} else {
				cnt[gid] = quotient
			}
		}
	}

	DPrintf("%d cnt: %v", sc.me, cnt)
	restIndex := make([]int, 0) // record the unassigned index
	// move as few shards as possible
	for i, v := range lastConfig.Shards {
		if n, ok := cnt[v]; ok && n > 0 {  
			newConfig.Shards[i] = v
			cnt[v]--
			if cnt[v] == 0 {
				delete(cnt, v)
			}
		} else {
			restIndex = append(restIndex, i)
		}
	}
	
	// since the order of element is undeterministic in map
	// we need to convert it to a sorted array first
	restGIDs := make([]int, 0)
	for k, v := range cnt {
		for i := 0; i < v; i++ {
			restGIDs = append(restGIDs, k)
		}
 	}
	sort.Ints(restGIDs)

	// change the shard at unassigned index
	i := 0
	for _, gid := range restGIDs {
		newConfig.Shards[restIndex[i]] = gid
		i++
	}
	DPrintf("%d shard: %v", sc.me, newConfig.Shards)
}

