package shardkv


import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "encoding/gob"
import "time"

const(
  GET = 1
  PUT = 2
  APPEND = 3
)

const(
  KILL = 1
  RUN = 2
)


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Kind int
	Key string
	Value string
	ClientId int64
	RequestId int
	Err Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data [shardmaster.NShards]map[string]string
	pendingOps map[int]chan Op
	dup map[int64]int
	mck *shardmaster.Clerk
	config shardmaster.Config
	state int
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var op Op
	op.Kind = GET
	op.ClientId = args.ClientId
	op.RequestId = args.RequestId
	op.Key = args.Key

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
	  reply.WrongLeader = true
	  return
	}
	ch, ok := kv.pendingOps[index]
	if !ok {
	  ch = make(chan Op)
	  kv.pendingOps[index] = ch
	}
	select {
	  case msg := <- ch:
	    reply.WrongLeader = false
	    reply.Err = msg.Err
	    reply.Value = msg.Value
	  case <- time.After(250 * time.Millisecond):
	    reply.WrongLeader = true
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var op Op
	if args.Op == "Put" {
	  op.Kind = PUT
	}else {
	  op.Kind = APPEND
	}
	op.ClientId = args.ClientId
	op.RequestId = args.RequestId
	op.Key = args.Key
	op.Value = args.Value

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
	  reply.WrongLeader = true
	  return
	}
	ch, ok := kv.pendingOps[index]
	if !ok {
	  ch = make(chan Op)
	  kv.pendingOps[index] = ch
	}
	select {
	  case msg := <- ch:
	    reply.WrongLeader = false
	    reply.Err = msg.Err
	  case <- time.After(250* time.Millisecond):
	    reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.state = KILL
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	for i := 0; i < shardmaster.NShards; i++ {
	  kv.data[i] = make(map[string]string)
	}
	kv.pendingOps = make(map[int]chan Op)
	kv.dup = make(map[int64]int)

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.config = kv.mck.Query(-1)
	kv.state = RUN

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

  go kv.UpdateConfig()
   go kv.Run()

	return kv
}

func (kv *ShardKV) UpdateConfig() {
  for {
    if kv.state == KILL {
      return
    }
    if _, isLeader := kv.rf.GetState(); isLeader {
      kv.config = kv.mck.Query(-1)
    }
    time.Sleep(100 * time.Millisecond)
  }
}

func (kv *ShardKV) Run() {
  for {
    if kv.state == KILL {
      return
    }
    select {
      case msg := <- kv.applyCh:
        kv.mu.Lock()
        op := msg.Command.(Op)
        if !kv.IsDup(op.ClientId, op.RequestId){
          shard := key2shard(op.Key)
          if kv.config.Shards[shard] != kv.gid {
            op.Err = ErrWrongGroup
          }else {
            if op.Kind == PUT {
              kv.data[shard][op.Key] = op.Value
              op.Err = OK
            }else if op.Kind == APPEND {
              kv.data[shard][op.Key] = kv.data[shard][op.Key] + op.Value
              op.Err = OK
            }else {
              v, ok := kv.data[shard][op.Key]
              if ok {
                op.Value = v
                op.Err = OK
              }else {
                op.Err = ErrNoKey
              }
            }
            kv.dup[op.ClientId] = op.RequestId
          }
        }
        ch, ok := kv.pendingOps[msg.Index]
        if ok {
          ch <- op
        }
        kv.mu.Unlock()
    }
  }
}

func (kv *ShardKV) IsDup(ClientId int64, RequestId int) bool {
  id, ok := kv.dup[ClientId]
  if !ok {
    return false
  }
  return id >= RequestId
}
