package raftkv

import (
	"labgob"
	"labrpc"
	"raft"
	"sync"
	"time"
)


const(
  GET = 1
  PUT = 2
  APPEND = 3
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Kind int
	Key string
	Value string
	Client int64
	Id   int64
}


type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data    map[string]string
	pendingOps  map[int]chan Op
	dup     map[int64]int64
	kill  bool
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var op Op
	op.Kind = GET
	op.Key = args.Key
	op.Client = args.Client
	op.Id = args.Id
  DPrintf("Server: get, client=%d, id=%d\n", args.Client, args.Id)
  result := kv.AppendEntry(op)
  reply.WrongLeader = !result
  DPrintf("Server: WrongLeader %d: %t---------------------------------------\n", kv.me,reply.WrongLeader)
  if result {
    if value, ok := kv.data[op.Key]; ok {
      reply.Value = value
      reply.Err = OK
    }else {
      reply.Err = ErrNoKey
    }
  }
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
  DPrintf("Server: PutAppend, client=%d, id=%d, key=%s, value=%s\n", args.Client, args.Id, args.Key, args.Value)
	var op Op
	if args.Op == "Put" {
	  op.Kind = PUT
	}else if args.Op == "Append" {
	  op.Kind = APPEND
	}
	op.Client = args.Client
	op.Id = args.Id
  op.Key = args.Key
  op.Value = args.Value
  result := kv.AppendEntry(op)

  if result {
   // DPrintf("Server: PutAppend, key=%s, value=%s\n", op.Key, kv.data[op.Key])
    reply.WrongLeader = false
    reply.Err = OK
  }else {
    reply.WrongLeader = true
  }
  DPrintf("Server: WrongLeader %d: %t---------------------------------------\n", kv.me,reply.WrongLeader)
}

func (kv *KVServer) AppendEntry(entry Op) bool {
  index, _, isLeader := kv.rf.Start(entry)
  if !isLeader {
    return false
  }
  kv.mu.Lock()
  ch, ok := kv.pendingOps[index]
  if !ok {
    ch = make(chan Op, 1)
    kv.pendingOps[index] = ch
  }
  kv.mu.Unlock()
  var kind string
  if entry.Kind == GET {
    kind = "GET"
  }else if entry.Kind == APPEND {
    kind = "APPEND"
  }else{
    kind = "PUT"
  }
  select{
    case op := <- ch:
      DPrintf("pend mu 3-----client=%d, id=%d, key=%s---------------------------\n", op.Client, op.Id, op.Key)
      kv.mu.Lock()
     /* for k, v := range kv.data {
        DPrintf("Append Entry %s, key=%s, value=%s\n", kind, k,v)
      }*/
      delete(kv.pendingOps, index)
      kv.mu.Unlock()
      DPrintf("pend mu 4---------------------\n")
      return  op.Client== entry.Client && op.Id == entry.Id
    case <-time.After(1000 * time.Millisecond):
      DPrintf("Append Entry %d timeout\n", kind)
      return false
  }
  return false
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.kill = true
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	DPrintf("start server: %d\n", me)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.pendingOps = make(map[int]chan Op)
	kv.dup = make(map[int64]int64)
	kv.kill = false
	go kv.ReceiveMsg()

	return kv
}

func (kv *KVServer)ReceiveMsg(){
  for{
    if kv.kill {
      return
    }
    select{
      case msg := <- kv.applyCh:
        var op Op
        op = msg.Command.(Op)
/*        if kv.IsDup(&op){
          kv.pendingOps[msg.Index] <- op
          return
        }*/
        kv.mu.Lock()
        if !kv.IsDup(&op){
          if op.Kind == PUT {
            kv.data[op.Key] = op.Value
            DPrintf("receive msg put, key=%s, value=%s\n", op.Key, op.Value)
          }else if op.Kind == APPEND {
            kv.data[op.Key] = kv.data[op.Key] + op.Value
            DPrintf("receive msg append, key=%s, value=%s\n", op.Key, kv.data[op.Key])
          }else if op.Kind == GET {
            DPrintf("receive msg get, key=%s, value=%s\n", op.Key, kv.data[op.Key])
          }
          kv.dup[op.Client] = op.Id
        }
        ch, ok := kv.pendingOps[msg.Index]
        if ok {
          ch <- op
        }
        kv.mu.Unlock()
    }
  }
}

func (kv *KVServer) IsDup(op *Op) bool {
  v, ok := kv.dup[op.Client]
  if ok {
    return v >= op.Id
  }
  return false
}
