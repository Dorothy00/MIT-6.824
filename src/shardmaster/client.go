package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	mu     sync.Mutex
	client int64
	id     int
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
	// Your code here.
	ck.client = nrand()
	ck.id = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
  ck.mu.Lock()
  ck.id++
  ck.mu.Unlock()
	args.Num = num
	args.Client = ck.client
	args.RequestId = ck.id
	DPrintf("Client start query.......\n" )
	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			DPrintf("client: ok=%t, wrongLeader=%t\n", ok, reply.WrongLeader)
			if ok && reply.WrongLeader == false {
	      for k, v := range reply.Config.Shards {
	        DPrintf("Client: %d->%d\n", k, v)
	      }
	      DPrintf("Client end query........\n" )
				return reply.Config
			}else {
		    DPrintf("Client query wrongleader %d ..\n", i)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
  ck.mu.Lock()
  ck.id++
  ck.mu.Unlock()
	args.Servers = servers
	args.Client = ck.client
	args.RequestId = ck.id
	DPrintf("Client start Join...\n" )

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
	      DPrintf("Client Finish Join...\n")
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
  ck.mu.Lock()
  ck.id++
  ck.mu.Unlock()
	args.GIDs = gids
	args.Client = ck.client
	args.RequestId = ck.id

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
  ck.mu.Lock()
  ck.id++
  ck.mu.Unlock()
	args.Shard = shard
	args.GID = gid
	args.Client = ck.client
	args.RequestId = ck.id

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
