package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
	client int64
	id     int
	mu     sync.Mutex
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
	ck.client = nrand()
	ck.id = 0
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

	// You will have to modify this function.
  var args *GetArgs = &GetArgs{}
  ck.mu.Lock()
  args.Client = ck.client
  args.Id = ck.id
  ck.id++
  ck.mu.Unlock()
	args.Key = key
for{
	  var reply *GetReply = &GetReply{}
	  ok := ck.servers[ck.leader].Call("KVServer.Get", args, reply)
	  if ok && !reply.WrongLeader {
	    if reply.Err == OK {
	      return reply.Value
	    }else {
	      return ""
	    }
	  }else{
	    ck.leader = (ck.leader + 1) % len(ck.servers)
	  }
	}
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
	var args * PutAppendArgs = &PutAppendArgs{}
	ck.mu.Lock()
  args.Client = ck.client
  args.Id = ck.id
  ck.id++
	args.Key = key
	args.Value = value
	args.Op = op
	ck.mu.Unlock()
	for{
	  var reply *PutAppendReply = &PutAppendReply{}
	  ok := ck.servers[ck.leader].Call("KVServer.PutAppend", args, reply)
	  if ok && !reply.WrongLeader{
	    break
	  }else {
	    ck.leader = (ck.leader + 1) % len(ck.servers)
	  }
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
