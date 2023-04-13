package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	clientId int64
	leaderId int // current leader
	SN int // serial number
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.leaderId = 0
	ck.SN = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs {
		Key: key, 
		ClientId: ck.clientId, 
		SN: ck.SN,
	}
	reply := GetReply{}

	n := len(ck.servers)
	i := ck.leaderId
	for {
		ok := ck.servers[i%n].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			ck.leaderId = i
			break
		}
		i++
	}
	
	ck.SN++
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs {
		Key: key, 
		Value: value, 
		Op: op, 
		ClientId: ck.clientId, 
		SN: ck.SN,
	}
	reply := PutAppendReply{}

	n := len(ck.servers)
	i := ck.leaderId
	for {
		ok := ck.servers[i%n].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			ck.leaderId = i
			break
		}
		i++ 
	}
	ck.SN++
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
