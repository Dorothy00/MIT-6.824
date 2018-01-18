package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "time"
import "math/rand"

// import "bytes"
// import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const(
  FOLLOWER = "follower"
  CANDIDATE = "candidate"
  LEADER = "leader"
  KILL = "killed"
)

const(
  ELECTIONTIMEOUT = 300
  HEARTBEATINTERVAL = 120
)
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
 currentTerm int
 voteFor int
 log []LogEntry

 commitIndex int
 lastApplied int

 nextIndex []int
 matchIndex []int

 state string
 voteNum int

 heartCh chan bool
 voteCh chan bool
 electCh chan bool
 applyCh chan ApplyMsg
 commitCh chan bool

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

type AppendEntries struct {
  Term int
  LeaderId int
  PrevLogIndex int
  PrevLogTerm int
  LeaderCommit int
  Entries []LogEntry
}

type AppendEntriesReply struct {
  Term int
  Success bool
}

type LogEntry struct {
  Command interface{}
  Term int
}

func (rf *Raft)getLastLogIndex() int{
  return len(rf.log) - 1
}

func (rf *Raft)getLastLogTerm() int{
  return rf.log[len(rf.log) - 1].Term
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	Granted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//fmt.Printf("=======RequestVote Handler======\n")
	reply.Granted = false
	//fmt.Printf("args.Term: %d, rf.currentTerm: %d\n", args.Term, rf.currentTerm)
	if args.Term < rf.currentTerm{
	  reply.Term = rf.currentTerm
	  return
	}
	if args.Term > rf.currentTerm{
	  rf.state = FOLLOWER
	  rf.voteFor = -1
	}
	rf.currentTerm = args.Term
//	fmt.Printf("rf.VoteFor: %d, args.CandidateId: %d, isUpToDate: %t\n", rf.voteFor, args.CandidateId, rf.isUpToDate(args.LastLogIndex, args.LastLogTerm))
  if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && rf.isUpToDate(args.LastLogIndex, args.LastLogTerm){
    reply.Granted = true
    rf.voteFor = args.CandidateId
    rf.voteCh <- true
    rf.state = FOLLOWER
    //fmt.Printf("peer %d vote for %d\n", rf.me, args.CandidateId)
  }
  reply.Term = rf.currentTerm
	//fmt.Printf("=======RequestVote Handler end=======\n")
}

func (rf *Raft) isUpToDate(lastLogIndex int, lastLogTerm int) bool{
  if lastLogTerm != rf.getLastLogTerm(){
    return lastLogTerm > rf.getLastLogTerm()
  }else{
    return lastLogIndex >= rf.getLastLogIndex()
  }
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
  //fmt.Printf("sendRequestVote to peer %d, result is %t\n", server, ok)
	if ok{
	  if rf.state != CANDIDATE{
	    return ok
	  }
	  //fmt.Printf("args.Term: %d, rf.currentTerm: %d\n", args.Term, rf.currentTerm)
	  if args.Term != rf.currentTerm{
	    return ok
	  }
	  if reply.Term > rf.currentTerm{
	    rf.currentTerm = reply.Term
	    rf.state = FOLLOWER
	    rf.voteFor = -1
	  }
	  if reply.Granted{
	    rf.voteNum++;
	    if 2 * rf.voteNum > len(rf.peers){
	      rf.state = LEADER
	      rf.electCh <- true
	    }
	  }
	}
	return ok
}

func (rf *Raft) sendRequestVoteBroadcast(){
 // fmt.Printf("peer %d sendRequestVoteBroadCast\n", rf.me)
  var args *RequestVoteArgs = &RequestVoteArgs{}
  args.Term = rf.currentTerm
  args.CandidateId = rf.me
  args.LastLogIndex = rf.getLastLogIndex()
  args.LastLogTerm = rf.getLastLogTerm()
  //fmt.Printf("peers num is %d\n", len(rf.peers))
  for i := 0; i < len(rf.peers); i++{
    if i == rf.me{
      continue
    }
    go func(server int){
      var reply *RequestVoteReply = &RequestVoteReply{}
      rf.sendRequestVote(server, args, reply)
    }(i)
  }
}

func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
  //fmt.Printf("peer %d receive heartbeat\n", rf.me)
 if args.Term < rf.currentTerm{
    reply.Term = rf.currentTerm
    reply.Success = false
 } else{
    rf.currentTerm = args.Term
    reply.Term = rf.currentTerm
    reply.Success = true
    rf.heartCh <- true
 }
}

func (rf *Raft) sendHeartBeat(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
	  if rf.currentTerm < reply.Term{
	    rf.mu.Lock()
	    rf.state = FOLLOWER
	    rf.currentTerm = reply.Term
	    rf.mu.Unlock()
	  }
	}
	return ok
}

func (rf *Raft) sendHeartBeatBroadcast(){
//  fmt.Printf("peer %d send HeadtBeatBroadcast\n", rf.me)
  var args *AppendEntries= &AppendEntries{}
  args.Term = rf.currentTerm
  args.LeaderId = rf.me
  for i := 0; i < len(rf.peers); i++{
    if rf.state != LEADER{
      break
    }
    if i == rf.me {
      continue
    }
    go func(server int){
      //fmt.Printf("peer %d send heartbeat to peer %d\n", rf.me, server)
      var reply *AppendEntriesReply = &AppendEntriesReply{}
      rf.sendHeartBeat(server, args, reply)
    }(i)
  }
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	rf.state = KILL
	rf.mu.Unlock()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	//fmt.Printf("make peer %d\n", me)

	// Your initialization code here (2A, 2B, 2C).
  rf.currentTerm = 0
  rf.voteFor = -1
  rf.log = append(rf.log, LogEntry{Term: 0})

  rf.commitIndex = 0
  rf.lastApplied = 0

  rf.nextIndex = nil
  rf.matchIndex = nil

  rf.state = FOLLOWER
  rf.voteNum = 0

  rf.heartCh = make(chan bool)
  rf.voteCh = make(chan bool)
  rf.commitCh = make(chan bool)
  rf.electCh = make(chan bool)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	 go rf.run()


	return rf
}

func (rf *Raft) run(){
 // fmt.Printf("run peer %d\n", rf.me)
  for{
    rf.mu.Lock()
    currState := rf.state
    rf.mu.Unlock()
    switch currState{
      case LEADER:
          rf.sendHeartBeatBroadcast()
          time.Sleep(time.Millisecond * time.Duration(HEARTBEATINTERVAL))
             //todo 发送心跳包
      case CANDIDATE:
        rf.mu.Lock()
        rf.voteNum = 1
        rf.voteFor = rf.me
        rf.currentTerm++
        rf.mu.Unlock()
        // todo request vote
        rf.sendRequestVoteBroadcast()
        select{
          case <-rf.heartCh:
          case <-rf.electCh:
            rf.mu.Lock()
            rf.state = LEADER
            rf.nextIndex = make([]int, len(rf.peers))
            for i := 0; i < len(rf.peers); i++{
              rf.nextIndex[i] = rf.getLastLogIndex() + 1
            }
            rf.matchIndex = make([]int, len(rf.peers))
            rf.mu.Unlock()
          case <- time.After(time.Millisecond * time.Duration(ELECTIONTIMEOUT + rand.Intn(200))):
        }
      case FOLLOWER:
        select{
        case <-rf.heartCh:
        case <-rf.voteCh:
          //todo 选举
        case <- time.After(time.Millisecond * time.Duration(ELECTIONTIMEOUT + rand.Intn(200))):
          rf.mu.Lock()
          rf.state = CANDIDATE
          rf.mu.Unlock()
        }
      case KILL:
        return
    }
  }

}

