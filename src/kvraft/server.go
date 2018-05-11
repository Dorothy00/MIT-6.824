package raftkv

import (
  "labgob"
  "labrpc"
  "raft"
  "sync"
  "time"
  "bytes"
  "encoding/gob"
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
  Id   int
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
  dup     map[int64]int
  kill  bool
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
  // Your code here.
  var op Op
  op.Kind = GET
  op.Key = args.Key
  op.Client = args.Client
  op.Id = args.Id
  result := kv.AppendEntry(op)
  reply.WrongLeader = !result
  if result {
    kv.mu.Lock()
    if value, ok := kv.data[op.Key]; ok {
      reply.Value = value
      reply.Err = OK
    } else {
      reply.Err = ErrNoKey
    }
    kv.mu.Unlock()
  }
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
  // Your code here.
  var op Op
  if args.Op == "Put" {
    op.Kind = PUT
  }  else if args.Op == "Append" {
    op.Kind = APPEND
  }
  op.Client = args.Client
  op.Id = args.Id
  op.Key = args.Key
  op.Value = args.Value
  result := kv.AppendEntry(op)

  if result {
    reply.WrongLeader = false
    reply.Err = OK
  } else {
    reply.WrongLeader = true
  }
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
  select{
    case op := <- ch:
      kv.mu.Lock()
      delete(kv.pendingOps, index)
      kv.mu.Unlock()
      return  op.Client== entry.Client && op.Id == entry.Id
    case <-time.After(800 * time.Millisecond):
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

  kv.applyCh = make(chan raft.ApplyMsg, 1)
  kv.rf = raft.Make(servers, me, persister, kv.applyCh)

  // You may need initialization code here.
  kv.data = make(map[string]string)
  kv.pendingOps = make(map[int]chan Op)
  kv.dup = make(map[int64]int)
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
        if msg.UseSnapshot {
          var LastIncludeIndex int
          var LastIncludeTerm int
          r := bytes.NewBuffer(msg.Snapshot)
          d := gob.NewDecoder(r)
          kv.mu.Lock()
          d.Decode(&LastIncludeIndex)
          d.Decode(&LastIncludeTerm)
          kv.data = make(map[string]string)
          kv.dup = make(map[int64]int)
          d.Decode(&kv.data)
          d.Decode(&kv.dup)
          kv.mu.Unlock()
        } else {
          var op Op
          op = msg.Command.(Op)
          kv.mu.Lock()
          if !kv.IsDup(&op){
            if op.Kind == PUT {
              kv.data[op.Key] = op.Value
            } else if op.Kind == APPEND {
              kv.data[op.Key] = kv.data[op.Key] + op.Value
            } else {
            }
              kv.dup[op.Client] = op.Id
          }
          ch, ok := kv.pendingOps[msg.Index]
          if ok {
            ch <- op
          }

          if kv.maxraftstate != -1 && kv.rf.GetPersistSize() > kv.maxraftstate {
            w := new(bytes.Buffer)
            e := gob.NewEncoder(w)
            e.Encode(kv.data)
            e.Encode(kv.dup)
            data := w.Bytes()
            go kv.rf.StartSnapshot(data, msg.Index)
          }
          kv.mu.Unlock()
        }
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
