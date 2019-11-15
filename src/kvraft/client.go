package raftkv

import (
	"labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	leader  int //缓存leader
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
	return ck
}

//
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
//
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{
		Key:       key,
		requestId: nrand(),
	}
	reply := &GetReply{}
	n := len(ck.servers)
	for i := 0; i < n; i++ {
		ok := ck.servers[(i + ck.leader) % n].Call("KVServer.Get", args, reply)
		if !ok || reply.WrongLeader {
			continue
		}
	}
	// You will have to modify this function.
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		requestId: nrand(),
	}
	reply := &PutAppendReply{
	}
	n := len(ck.servers)
	for i := 0; i < n; i++ {
		ok := ck.servers[(i + ck.leader) % n].Call("KVServer.Get", args, reply)
		if !ok || reply.WrongLeader {
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
