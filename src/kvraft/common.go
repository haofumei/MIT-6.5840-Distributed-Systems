package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ResponseTimeout = 1000
	SnapshotTimeout = 100
	MaxNumOfCommandsWithoutSnapshot = 20 
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	ClientId int64
	SN int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	ClientId int64
	SN int
}

type GetReply struct {
	Err   Err
	Value string
}
