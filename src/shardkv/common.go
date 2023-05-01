package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOutdatedConfig = "ErrOutdatedConfig"
	ErrUpdatingConfig = "ErrUpdatingConfig"
	ResponseTimeout = 1000
	// maxraftstate(1000) equals approximated 16 logs,
	// so I choose 10 here for avoding confilts.
	SnapCheckpoint = 10

	PollInterval = 100 // poll the shardctrler to learn about new configurations.
	ShardOK = "ShardOK"
	ShardMigrationOut = "ShardMigrationOut"
	ShardMigrationIn = "ShardMigrationIn"
)

type Err string
type ShardStatus string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	ClientId int64
	SN int
	Shard int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	ClientId int64
	SN int
	Shard int
}

type GetReply struct {
	Err   Err
	Value string
}

type ShardMigrationArgs struct {
	Num int
	Sids []int
	Data []map[string]string
	ClientId int64
	SN int
}

type ShardMigrationReply struct {
	Err Err
}