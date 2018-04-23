package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync/atomic"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
	client int64
	id     int64
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
	DPrintf("Client: Start Get.....\n")
  var args *GetArgs = &GetArgs{}
  args.Client = ck.client
  args.Id = atomic.AddInt64(&ck.id, 1)
	args.Key = key
for{
	  var reply *GetReply = &GetReply{}
	  ok := ck.servers[ck.leader].Call("KVServer.Get", args, reply)
	  if ok && !reply.WrongLeader {
	    if reply.Err == OK {
//	      DPrintf("Client: leader=%d, Get  %s=%s\n", ck.leader, key, reply.Value)
	      return reply.Value
	    }else {
	      DPrintf("Client: Get nothing\n")
	      return ""
	    }
	  }else{
	//    DPrintf("Client: Get wrongleader %d, %t, %t\n", ck.leader, ok, reply.WrongLeader)
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
	DPrintf("Client: Start PutAppend.....\n")
	var args * PutAppendArgs = &PutAppendArgs{}
  args.Client = ck.client
  args.Id = atomic.AddInt64(&ck.id, 1)
	args.Key = key
	args.Value = value
	args.Op = op
	for{
	  var reply *PutAppendReply = &PutAppendReply{}
	  ok := ck.servers[ck.leader].Call("KVServer.PutAppend", args, reply)
	  if ok && !reply.WrongLeader{
	  //  DPrintf("Client: leader=%d, Put %s = %s\n", ck.leader, key, value)
	    break
	  }else {
	    //DPrintf("Client: Put wrongleader %d, %t, %t\n", ck.leader, ok, reply.WrongLeader)
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
