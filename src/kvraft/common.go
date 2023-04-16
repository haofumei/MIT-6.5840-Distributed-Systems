package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrInitElection = "ErrInitElection"
	ResponseTimeout = 500
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
