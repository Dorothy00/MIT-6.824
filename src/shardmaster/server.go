package shardmaster


import "raft"
import "labrpc"
import "sync"
import "encoding/gob"
import "time"


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	pendingOps map[int] chan Op
	dup map[int64]int
	kill chan bool

  cfgNum int
	configs []Config // indexed by config num
}

const(
  JOIN = 1
  LEAVE = 2
  MOVE = 3
  QUERY = 4
)


type Op struct {
	// Your data here.
	Kind int
	Args interface{}
	Reply interface{}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	var op Op
	op.Kind = JOIN
	op.Args = *args
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
	  reply.WrongLeader = true
	}
	DPrintf("sm %d join, op index is %d ,is Leader %t\n", sm.me, index, isLeader)
 ch, ok := sm.pendingOps[index]
 if !ok {
  ch = make(chan Op)
  sm.pendingOps[index] = ch
 }
 select {
  case msg := <- ch:
    DPrintf("sm %d receive Join result, \n", sm.me)
    if recArgs, ok := msg.Args.(JoinArgs); !ok {
      reply.WrongLeader = true
    }else {
      reply.WrongLeader = !(args.Client == recArgs.Client && args.RequestId == recArgs.RequestId)
    }
  case <- time.After(800 * time.Millisecond):
    reply.WrongLeader = true
 }
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	var op Op
	op.Kind = LEAVE
	op.Args = *args
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
	  reply.WrongLeader = true
	}
 ch, ok := sm.pendingOps[index]
 if !ok {
  ch = make(chan Op)
  sm.pendingOps[index] = ch
 }
 select {
  case msg := <- ch:
    if recArgs, ok := msg.Args.(LeaveArgs); !ok {
      reply.WrongLeader = true
    }else {
      reply.WrongLeader = !(args.Client == recArgs.Client && args.RequestId == recArgs.RequestId)
    }
  case <- time.After(800 * time.Millisecond):
    reply.WrongLeader = true
 }
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	var op Op
	op.Kind = MOVE
	op.Args = *args
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
	  reply.WrongLeader = true
	}
 ch, ok := sm.pendingOps[index]
 if !ok {
  ch = make(chan Op)
  sm.pendingOps[index] = ch
 }
 select {
  case msg := <- ch:
    if recArgs, ok := msg.Args.(MoveArgs); !ok {
      reply.WrongLeader = true
    }else {
      reply.WrongLeader = !(args.Client == recArgs.Client && args.RequestId == recArgs.RequestId)
    }
  case <- time.After(800 * time.Millisecond):
    reply.WrongLeader = true
 }
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	var op Op
	op.Kind = QUERY
	op.Args = *args
	index, _, isLeader := sm.rf.Start(op)
	DPrintf("sm %d query , op index is %d ,is Leader %t\n", sm.me, index, isLeader)
	if !isLeader {
	  reply.WrongLeader = true
	}
 ch, ok := sm.pendingOps[index]
 if !ok {
  ch = make(chan Op)
  sm.pendingOps[index] = ch
 }
 select {
  case msg := <- ch:
    DPrintf("sm %d receive query result, \n", sm.me)
    reply.WrongLeader = true
    recArgs, ok := msg.Args.(QueryArgs)
    DPrintf("sm %d, args client=%d requestId=%d, recv client=%d requestId=%d\n", sm.me, args.Client, args.RequestId, recArgs.Client, recArgs.RequestId)
    if ok && (args.Client == recArgs.Client && args.RequestId == recArgs.RequestId) {
      r, ok := msg.Reply.(QueryReply)
      DPrintf("sm %d. ok=%t\n", sm.me, ok)
      if ok {
        reply.WrongLeader = false
        reply.Config = r.Config
      }
    }
  case <- time.After(1000 * time.Millisecond):
    DPrintf("sm %d query timeout..\n", sm.me)
    reply.WrongLeader = true
 }
 DPrintf("sm %d, wrongLeader=%t\n", sm.me, reply.WrongLeader)
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	sm.kill <- true
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.cfgNum = 0
  sm.pendingOps = make(map[int] chan Op)
  sm.dup = make(map[int64]int)
  sm.kill = make(chan bool)
  gob.Register(JoinArgs{})
  gob.Register(LeaveArgs{})
  gob.Register(MoveArgs{})
  gob.Register(QueryArgs{})
  gob.Register(JoinReply{})
  gob.Register(LeaveReply{})
  gob.Register(MoveReply{})
  gob.Register(QueryReply{})
  go sm.Run()

	return sm
}

func (sm *ShardMaster) Run() {
  for {
    select {
      case msg := <- sm.applyCh:
        DPrintf("sm %d receve applyCh index %d from raft\n", sm.me, msg.Index)
          op := msg.Command.(Op)
          sm.Apply(op, msg.Index)
      case <- sm.kill:
          return
    }
  }
}

func (sm *ShardMaster) IsDup(clientId int64, requestId int) bool {
  id, ok := sm.dup[clientId]
  if !ok {
    return false
  }
  return id >= requestId
}

func (sm *ShardMaster) nextConfig() *Config {
  var c Config
  c.Num = sm.cfgNum + 1
  c.Shards = sm.configs[sm.cfgNum].Shards
  c.Groups = map[int][]string{}
  for k, v := range sm.configs[sm.cfgNum].Groups {
    c.Groups[k] = v
  }
  sm.cfgNum++
  sm.configs = append(sm.configs, c)
  return &sm.configs[sm.cfgNum]
}

func (sm *ShardMaster) Apply(op Op, index int) {
  DPrintf("sm %d apply index %d, type %T\n", sm.me, index, op.Args)
  sm.mu.Lock()
  defer sm.mu.Unlock()
  switch op.Args.(type) {
    case JoinArgs:
      DPrintf("sm %d join apply, index %d\n", sm.me, index)
      args := op.Args.(JoinArgs)
      if !sm.IsDup(args.Client, args.RequestId) {
        sm.JoinApply(&args)
        ch , ok := sm.pendingOps[index]
        if ok {
          ch <- op
        }
      }
    case LeaveArgs:
      DPrintf("sm %d leave apply, index %d\n", sm.me, index)
      args := op.Args.(LeaveArgs)
      if !sm.IsDup(args.Client, args.RequestId) {
        sm.LeaveApply(&args)
        ch , ok := sm.pendingOps[index]
        if ok {
          ch <- op
        }
      }
    case MoveArgs:
      DPrintf("sm %d move apply, index %d\n", sm.me, index)
      args  := op.Args.(MoveArgs)
      if !sm.IsDup(args.Client, args.RequestId) {
        sm.MoveApply(&args)
        ch , ok := sm.pendingOps[index]
        if ok {
          ch <- op
        }
      }
    case QueryArgs:
      DPrintf("sm %d query apply, index %d\n", sm.me, index)
      args  := op.Args.(QueryArgs)
      if !sm.IsDup(args.Client, args.RequestId) {
        var reply QueryReply
        if args.Num == -1 || args.Num > sm.cfgNum {
          DPrintf("sm %d: query reply cfgNum %d, confgNum %d\n", sm.me, sm.cfgNum, len(sm.configs))
          reply.Config = sm.configs[sm.cfgNum]
        for k, v := range sm.configs[sm.cfgNum].Shards {
          DPrintf("sm %d: query reply %d=%d-------\n", sm.me, k, v)
        }
        }else {
          DPrintf("sm %d: query reply cfgNum %d, confgNum %d-----------\n", sm.me, sm.cfgNum, len(sm.configs))
          reply.Config = sm.configs[args.Num]
        }
        for k, v := range reply.Config.Shards {
          DPrintf("sm %d: query reply %d=%d\n", sm.me, k, v)
        }
        op.Reply = reply
        ch , ok := sm.pendingOps[index]
        if ok {
          ch <- op
        }
      }
  }
}

func (sm *ShardMaster) JoinApply(args *JoinArgs) {
  config := sm.nextConfig()
  for s, g := range config.Shards {
    DPrintf("sm %d shards, %d->%d\n", sm.me, s, g)
  }
  for k, v := range args.Servers {
    if _, ok := config.Groups[k]; !ok {
      config.Groups[k] = v
      sm.ReBalance(config, JOIN, k)
    }
  }
  for s, g := range sm.configs[sm.cfgNum].Shards {
    DPrintf("sm %d after join shards, %d->%d\n", sm.me, s, g)
  }

}
func (sm *ShardMaster) LeaveApply(args *LeaveArgs) {
  config := sm.nextConfig()
  for _, GID := range args.GIDs {
    delete(config.Groups, GID)
    sm.ReBalance(config, LEAVE, GID)
  }
}
func (sm *ShardMaster) MoveApply(args *MoveArgs) {
  config := sm.nextConfig()
  config.Shards[args.Shard] = args.GID
}

func (sm *ShardMaster) CountShards(cfg *Config) map[int][]int {
  shardsCount := map[int][]int{}
  for k := range cfg.Groups{
    shardsCount[k] = []int{}
  }
  for k, v := range  cfg.Shards {
    shardsCount[v] = append(shardsCount[v], k)
  }
  return shardsCount
}

func(sm *ShardMaster) GetMaxGidByShard(shardsCount map[int][]int) int {
  max := -1
  var gid int
  for k := range shardsCount {
    if max < len(shardsCount[k]) {
      max = len(shardsCount[k])
      gid = k
    }
  }
  return gid
}

func(sm *ShardMaster) GetMinGidByShard(shardsCount map[int][]int) int {
  min := -1
  var gid int
  for k := range shardsCount {
    if min == -1 || min > len(shardsCount[k]) {
      min = len(shardsCount[k])
      gid = k
    }
  }
  return gid
}

func (sm *ShardMaster) ReBalance(cfg *Config, Kind int, gid int) {
  shardsCount := sm.CountShards(cfg)
  switch Kind {
    case JOIN:
      meanNum := NShards / len(cfg.Groups)
      for i := 0; i < meanNum; i++ {
        maxGid := sm.GetMaxGidByShard(shardsCount)
        if len(shardsCount[maxGid]) == 0 {
          return
        }
        cfg.Shards[shardsCount[maxGid][0]] = gid
        shardsCount[maxGid] = shardsCount[maxGid][1:]
      }
    case LEAVE:
      shardsArray := shardsCount[gid]
      delete(shardsCount, gid)
      for _, v := range shardsArray {
        minGid := sm.GetMinGidByShard(shardsCount)
        cfg.Shards[v] = minGid
        shardsCount[minGid] = append(shardsCount[minGid], v)
      }
  }
  for k, v := range sm.configs[sm.cfgNum].Shards {
    DPrintf("sm %d, after rebalance %d->%d\n", sm.me, k, v)
  }
}
