package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "time"
import "math/rand"

import "bytes"
import "encoding/gob"

//import "fmt"

//
// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
	LEADER    = "leader"
	KILL      = "killed"
)

const (
	ELECTIONTIMEOUT   = 300
	HEARTBEATINTERVAL = 100
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
	CurrentTerm int
	VoteFor     int
	Log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state   string
	voteNum int

	heartCh  chan bool
	voteCh   chan bool
	electCh  chan bool
	applyCh  chan ApplyMsg
	commitCh chan bool
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.CurrentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
}

func (rf *Raft) GetPersistSize() int {
	return rf.persister.RaftStateSize()
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VoteFor)
	d.Decode(&rf.Log)
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

type AppendEntries struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	NextTryIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

func (rf *Raft) getLastLogIndex() int {
	return rf.Log[len(rf.Log)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.Log[len(rf.Log)-1].Term
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term    int
	Granted bool
}

func (rf *Raft) TruncateLogs(lastIncludeIndex int, lastIncludeTerm int) {
	index := -1
	first := LogEntry{Index: lastIncludeIndex, Term: lastIncludeTerm}
	for i := len(rf.Log) - 1; i >= 0; i-- {
		if rf.Log[i].Index == lastIncludeIndex && rf.Log[i].Term == lastIncludeTerm {
			index = i
			break
		}
	}
	if index < 0 {
		rf.Log = []LogEntry{first}
	} else {
		rf.Log = append([]LogEntry{first}, rf.Log[index+1:]...)
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		reply.Term = args.Term
		return
	}

	select {
	case rf.heartCh <- true:
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.state = FOLLOWER
		rf.VoteFor = -1
	}
	rf.persister.SaveSnapshot(args.Data)
	rf.TruncateLogs(args.LastIncludeIndex, args.LastIncludeTerm)
	rf.lastApplied = args.LastIncludeIndex
	rf.commitIndex = args.LastIncludeIndex
	rf.persist()

	msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}
	go func() {
		select {
		case rf.applyCh <- msg:
		}
	}()
}

func (rf *Raft) SendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok {
		if rf.CurrentTerm < reply.Term {
			rf.mu.Lock()
			rf.CurrentTerm = reply.Term
			rf.state = FOLLOWER
			rf.VoteFor = -1
			rf.mu.Unlock()
			return
		}
		rf.nextIndex[server] = args.LastIncludeIndex + 1
		rf.matchIndex[server] = args.LastIncludeIndex
	}
}

func (rf *Raft) StartSnapshot(snapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	firstIndex := 0
	if len(rf.Log) > 0 {
		firstIndex = rf.Log[0].Index
	}
	lastIndex := rf.getLastLogIndex()
	if index < firstIndex || index > lastIndex {
		return
	}
	lastIncludeIndex := index
	lastIncludeTerm := rf.Log[index-firstIndex].Term
	first := LogEntry{Index: index, Term: rf.Log[index-firstIndex].Term}
	rf.Log = append([]LogEntry{first}, rf.Log[index+1-firstIndex:]...)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(lastIncludeIndex)
	e.Encode(lastIncludeTerm)
	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persist()
	rf.persister.SaveSnapshot(data)
}

func (rf *Raft) readSnapshot(data []byte) {

	rf.readPersist(rf.persister.ReadRaftState())

	if len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var LastIncludedIndex int
	var LastIncludedTerm int
	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)

	rf.mu.Lock()
	rf.commitIndex = LastIncludedIndex
	rf.lastApplied = LastIncludedIndex
	rf.TruncateLogs(LastIncludedIndex, LastIncludedTerm)
	rf.mu.Unlock()

	msg := ApplyMsg{
		UseSnapshot: true,
		Snapshot:    data,
	}

	go func() {
		select {
		case rf.applyCh <- msg:
		}
	}()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Granted = false
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.state = FOLLOWER
		rf.VoteFor = -1
	}
	rf.CurrentTerm = args.Term
	if (rf.VoteFor == -1 || rf.VoteFor == args.CandidateId) && rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.Granted = true
		rf.VoteFor = args.CandidateId
		rf.voteCh <- true
		rf.state = FOLLOWER
	}
	reply.Term = rf.CurrentTerm
}

func (rf *Raft) isUpToDate(lastLogIndex int, lastLogTerm int) bool {
	if lastLogTerm != rf.getLastLogTerm() {
		return lastLogTerm > rf.getLastLogTerm()
	} else {
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
	if ok {
		if rf.state != CANDIDATE {
			return ok
		}
		if args.Term != rf.CurrentTerm {
			return ok
		}
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.state = FOLLOWER
			rf.VoteFor = -1
			rf.persist()
		}
		if reply.Granted {
			rf.voteNum++
			if 2*rf.voteNum > len(rf.peers) && rf.state != LEADER {
				rf.state = LEADER
				rf.electCh <- true
			}
		}
	}
	return ok
}

func (rf *Raft) sendRequestVoteBroadcast() {
	var args *RequestVoteArgs = &RequestVoteArgs{}
	args.Term = rf.CurrentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastLogIndex()
	args.LastLogTerm = rf.getLastLogTerm()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			var reply *RequestVoteReply = &RequestVoteReply{}
			rf.sendRequestVote(server, args, reply)
		}(i)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	defer rf.persist()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}
	rf.CurrentTerm = args.Term
	rf.state = FOLLOWER
	reply.Term = rf.CurrentTerm
	reply.Success = false
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.NextTryIndex = rf.getLastLogIndex() + 1
		return
	}
	firstIndex := 0
	if len(rf.Log) > 0 {
		firstIndex = rf.Log[0].Index
	}
	if args.PrevLogIndex >= firstIndex {
		term := rf.Log[args.PrevLogIndex-firstIndex].Term
		if term != args.PrevLogTerm {
			for i := args.PrevLogIndex - 1; i >= firstIndex; i-- {
				if rf.Log[i-firstIndex].Term != term {
					reply.NextTryIndex = i + 1
					break
				}
			}
			return
		}
		rf.Log = append(rf.Log[:args.PrevLogIndex+1-firstIndex], args.Entries...)
		reply.Success = true
		rf.persist()
	}

	select {
	case rf.heartCh <- true:
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex > rf.getLastLogIndex() {
			rf.commitIndex = rf.getLastLogIndex()
		}
		select {
		case rf.commitCh <- true:
		}
	}

}

func (rf *Raft) sendHeartBeat(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		if rf.state != LEADER || args.Term != rf.CurrentTerm {
			return ok
		}
		if rf.CurrentTerm < reply.Term {
			rf.mu.Lock()
			rf.state = FOLLOWER
			rf.CurrentTerm = reply.Term
			rf.persist()
			rf.mu.Unlock()
		} else {
			if reply.Success {
				if len(args.Entries) > 0 {
					rf.nextIndex[server] = rf.nextIndex[server] + len(args.Entries)
					rf.matchIndex[server] = rf.nextIndex[server] - 1
				}

			} else {
				rf.nextIndex[server] = reply.NextTryIndex
			}
		}
	}
	rf.persist()
	return ok
}

func (rf *Raft) sendHeartBeatBroadcast() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return
	}

	firstIndex := 0
	if len(rf.Log) > 0 {
		firstIndex = rf.Log[0].Index
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if rf.nextIndex[i] > firstIndex {
			var args *AppendEntries = &AppendEntries{}
			args.Term = rf.CurrentTerm
			args.LeaderId = rf.me
			args.LeaderCommit = rf.commitIndex
			lastLogIndex := rf.getLastLogIndex()
			if rf.nextIndex[i] < 1 {
				rf.nextIndex[i] = 1
			}
			if rf.nextIndex[i] > lastLogIndex {
				rf.nextIndex[i] = lastLogIndex + 1
			}
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.Log[args.PrevLogIndex-firstIndex].Term
			if lastLogIndex >= rf.nextIndex[i] {
				var Logs []LogEntry = rf.Log[rf.nextIndex[i]-firstIndex:]
				args.Entries = make([]LogEntry, len(Logs))
				copy(args.Entries, Logs)
			}
			go func(server int, args *AppendEntries) {
				var reply *AppendEntriesReply = &AppendEntriesReply{}
				rf.sendHeartBeat(server, args, reply)
			}(i, args)
		} else {
			// todo
			args := &InstallSnapshotArgs{}
			args.Term = rf.CurrentTerm
			args.LeaderId = rf.me
			args.Data = rf.persister.ReadSnapshot()
			args.LastIncludeIndex = rf.Log[0].Index
			args.LastIncludeTerm = rf.Log[0].Term
			//r := bytes.NewBuffer(rf.persister.ReadSnapshot())
			//d := gob.NewDecoder(r)
			//d.Decode(&args.LastIncludeIndex)
			//d.Decode(&args.LastIncludeTerm)
			//d.Decode(&args.Data)
			go func(server int, args *InstallSnapshotArgs) {
				reply := &InstallSnapshotReply{}
				rf.SendSnapshot(server, args, reply)
			}(i, args)
		}
	}
	commitIndex := rf.commitIndex
	lastIndex := rf.getLastLogIndex()
	for i := commitIndex + 1; i <= lastIndex; i++ {
		count := 1
		for j := 0; j < len(rf.peers); j++ {
			if rf.matchIndex[j] >= i && j != rf.me && rf.CurrentTerm == rf.Log[i-firstIndex].Term {
				count++
			}
		}
		if 2*count > len(rf.peers) {
			commitIndex = i
		}
	}
	if rf.commitIndex != commitIndex && rf.Log[commitIndex-firstIndex].Term == rf.CurrentTerm {
		rf.commitIndex = commitIndex
		select {
		case rf.commitCh <- true:
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
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
	isLeader := (rf.state == LEADER)
	DPrintf("raft %d receive operate, is leader %t\n", rf.me, rf.state == LEADER)

	// Your code here (2B).
	if rf.state == LEADER {
		rf.mu.Lock()
		index = rf.getLastLogIndex() + 1
		term = rf.CurrentTerm
		rf.Log = append(rf.Log, LogEntry{Command: command, Term: term, Index: index})
		DPrintf("")
		rf.persist()
		rf.mu.Unlock()
	}
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
	rf.state = KILL
	rf.persist()
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
	// Your initialization code here (2A, 2B, 2C).
	rf.CurrentTerm = 0
	rf.VoteFor = -1
	rf.Log = append(rf.Log, LogEntry{Term: 0, Index: 0})

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
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	go rf.run()
	go rf.commitLogs()

	return rf
}

func (rf *Raft) run() {
	for {
		//rf.mu.Lock()
		currState := rf.state
		//rf.mu.Unlock()
		switch currState {
		case LEADER:
			rf.sendHeartBeatBroadcast()
			time.Sleep(time.Millisecond * time.Duration(HEARTBEATINTERVAL))
		case CANDIDATE:
			rf.mu.Lock()
			rf.voteNum = 1
			rf.VoteFor = rf.me
			rf.CurrentTerm++
			rf.persist()
			rf.mu.Unlock()
			// todo request vote
			rf.sendRequestVoteBroadcast()
			select {
			case <-rf.heartCh:
			case <-rf.electCh:
				rf.mu.Lock()
				rf.state = LEADER
				DPrintf("raft %d become leader!\n", rf.me)
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = rf.getLastLogIndex() + 1
					rf.matchIndex[i] = 0
				}
				rf.mu.Unlock()
			case <-time.After(time.Millisecond * time.Duration(ELECTIONTIMEOUT+rand.Intn(200))):
			}
		case FOLLOWER:
			select {
			case <-rf.heartCh:
			case <-rf.voteCh:
				//todo 选举
			case <-time.After(time.Millisecond * time.Duration(ELECTIONTIMEOUT+rand.Intn(200))):
				rf.mu.Lock()
				rf.state = CANDIDATE
				rf.mu.Unlock()
			}
		case KILL:
			{
				return
			}
		}
	}

}

func (rf *Raft) commitLogs() {
	for {
		if rf.state == KILL {
			return
		}
		select {
		case <-rf.commitCh:
			rf.mu.Lock()
			firstIndex := 0
			if len(rf.Log) > 0 {
				firstIndex = rf.Log[0].Index
			}
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msg := ApplyMsg{Index: i, Command: rf.Log[i-firstIndex].Command}
				select {
				case rf.applyCh <- msg:
				}
				rf.lastApplied = i
			}
			rf.persist()
			rf.mu.Unlock()
		}
	}
}
