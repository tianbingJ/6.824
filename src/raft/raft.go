package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

type State int

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower";
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown State"
	}
}

const (
	Follower State = iota
	Candidate
	Leader
)

const CommitBuffer = 1000000

const (
	ElectionTimeoutMin = 150 * time.Millisecond
	ElectionTimeoutMax = 300 * time.Millisecond
	HeartBeatInterval  = 100 * time.Millisecond
)

const NonVotes = -1

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	syncCond  *sync.Cond
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state         State
	elecTimer     *time.Timer
	lastHeartBeat time.Time

	//persistent state
	currentTerm int
	votedFor    int

	//volatile state
	commitIndex int
	commitC     chan struct{}
	lastApplied int

	//volatile state on leaders
	nextIndex  []int
	matchIndex []int
	logs       []Entry
}

/*
entry log
*/
type Entry struct {
	Term    int
	Command interface{}
}

/*
 */
type AppendEntriesArgs struct {
	Term         int //当前任期
	LeaderId     int //leader在peer中的位置
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.state == Leader
	return term, isLeader
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

func (args RequestVoteArgs) String() string {
	return fmt.Sprintf("Term:%d, CandidaetId:%d, LastLogIndex:%d, LastLogTerm:%d", args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (reply RequestVoteReply) String() string {
	return fmt.Sprintf("Term: %d, VoteGranted:%v", reply.Term, reply.VoteGranted)
}

/*
 */
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	start := time.Now()
	//// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	curTerm := rf.currentTerm

	granted := false
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		rf.switchToFollower()
	}
	//TODO 需要判断log是不是最新的
	if args.Term == rf.currentTerm && (rf.votedFor == NonVotes || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		granted = true
		rf.updateHeartBeat() //重置选举时间
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = granted
	DPrintf("node:%v vote for node:%v oldTerm:%v Term:%v Granted:%v VotedFor:%v\n", rf.me, args.CandidateId, curTerm, args.Term, reply.VoteGranted, rf.votedFor)
	end := time.Now()
	if end.Sub(start) > time.Millisecond*1000 {
		DPrintf("Request cost too much")
	}
}

/**
更新更高的term
*/
func (rf *Raft) updateTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = NonVotes
}

/**
当收到比自己大的term时，转为Follower,开始心跳检测
在个方法需要在临界区内调用。
*/
func (rf *Raft) switchToFollower() {
	if rf.state == Follower {
		return
	}
	DPrintf("node: %v switch to Follower from %v", rf.me, rf.state)
	if rf.state == Leader {
		go rf.checkElecTimeout()
	}
	rf.state = Follower
}

func (rf *Raft) switchToLeader() {
	if rf.state == Leader {
		return
	}
	DPrintf("node: %v switch to Leader from %v", rf.me, rf.state)
	rf.state = Leader
	rf.commitIndex = -1
	for i := 0; i < len(rf.peers); i ++ {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = -1
	}
	go rf.fireHeartBeats()
	go rf.startReplicationEntry(rf.currentTerm)
	go rf.startUpdateCommitIndex(rf.currentTerm)
}

/**
heart beat and new entry request

*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	start := time.Now()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if rf.state != Follower {
		rf.updateTerm(args.Term)
		rf.switchToFollower()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	rf.updateHeartBeat() //合法请求，重置心跳时间
	end := time.Now()
	if end.Sub(start) > time.Millisecond*1000 {
		DPrintf("Request cost too much")
	}
	//DPrintf("")
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
	//tS := time.Now()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//tE := time.Now()
	//if tE.Sub(tS) > time.Millisecond*5 {
	//	DPrintf("Warning!!! call RequestVote cost %v", tE.Sub(tS))
	//}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//tS := time.Now()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//tE := time.Now()
	//if tE.Sub(tS) > time.Millisecond*5 {
	//	DPrintf("Warning!!! node:%v call AppendEntries to node:%v  cost %v ", args.LeaderId, server, tE.Sub(tS))
	//}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isLeader = rf.GetState()
	if !isLeader {
		return
	}
	entry := Entry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, entry)
	rf.syncCond.Broadcast()
	return index, term, isLeader
}

/*
try to update commitIndex when replicate log success
 */
func (rf *Raft) startUpdateCommitIndex(term int) {
	for range rf.commitC {
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		n := len(rf.peers)
		matchIndexes := make([]int, n)
		copy(matchIndexes, rf.matchIndex)
		matchIndexes[rf.me] = len(rf.logs)
		sort.Ints(matchIndexes)

		majorityMatchIndex := matchIndexes[(n - 1 /2)]
		if rf.currentTerm == rf.logs[majorityMatchIndex].Term && majorityMatchIndex > rf.commitIndex {
			rf.commitIndex = majorityMatchIndex
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startReplicationEntry(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.replicateToServ(term, i)
		}
	}
}

/*
replicate log to peer serv and wait for rf.syncCond for new entry
*/
func (rf *Raft) replicateToServ(term int, serv int) {
	for {
		rf.mu.Lock()
		if term != rf.currentTerm {
			return
		}
		for rf.nextIndex[serv] >= len(rf.logs) {
			rf.syncCond.Wait()
		}
		nextLogIndex := rf.nextIndex[serv]
		preLogIndex := nextLogIndex - 1
		preLogTerm := 0
		entries := make([]Entry, 0)
		if preLogIndex >= 0 {
			preLogTerm = rf.logs[len(rf.logs)-1].Term
			entries = append(entries, rf.logs[preLogIndex])
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: preLogIndex,
			PrevLogTerm:  preLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		rf.mu.Unlock() //release lock here, wait rpc return
		ok := rf.sendAppendEntries(serv, &args, &reply)
		if !ok {
			continue
		}
		rf.mu.Lock()
		if term != rf.currentTerm {
			break
		}
		if reply.Success {
			if nextLogIndex + 1 > rf.nextIndex[serv] {
				rf.nextIndex[serv] = nextLogIndex + 1
			}
			if nextLogIndex > rf.matchIndex[serv] {
				rf.matchIndex[serv] = nextLogIndex
			}
			rf.commitC <- struct{}{}
		} else {
			if reply.Term > rf.currentTerm {
				rf.switchToFollower()
				return
			}
			//not match, decrease nextIndex
			rf.nextIndex[serv] --
		}
		rf.mu.Unlock()
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

/*
检查选举超时，如果超时，发起选举
当Follower超过一段时间没有收到心跳包时，当前Raft节点发起选举
*/
func (rf *Raft) checkElecTimeout() {
	rf.mu.Lock()
	electTimeOut := rf.randomElecTime()
	rf.resetElecTimer(electTimeOut)
	ch := rf.elecTimer.C
	rf.mu.Unlock()
	//wait to time out
	_, ok := <-ch
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	d := time.Since(rf.lastHeartBeat)
	if rf.state == Leader {
		return
	}

	if d > electTimeOut {
		rf.currentTerm = rf.currentTerm + 1
		rf.state = Candidate
		rf.votedFor = rf.me
		//开始选举
		go rf.startElection(rf.currentTerm)
		DPrintf("node:%d start election, Term %v:  time:%v", rf.me, rf.currentTerm, electTimeOut)
	}
	//开始新的心跳检测
	go rf.checkElecTimeout()
}

func (rf *Raft) resetElecTimer(duration time.Duration) {
	rf.elecTimer = time.NewTimer(duration)
}

/**
当收到合法RPC时，Follower需要更新心跳时间和选举超时时间
TODO 考虑timer的使用,目前的使用方式不是很优雅
*/
func (rf *Raft) updateHeartBeat() {
	rf.lastHeartBeat = time.Now()
	rf.resetElecTimer(rf.randomElecTime())
}

func (rf *Raft) randomElecTime() time.Duration {
	interval := int64(ElectionTimeoutMax - ElectionTimeoutMin)
	elecTime := time.Duration(rand.Int63n(interval)) + ElectionTimeoutMin
	return elecTime
}

/**
停止选举超时检测
*/
func (rf *Raft) stopElecTimer() {
	rf.elecTimer = nil
}

/**
开始选举
选举方法只需要等待足够数量的node返回选举结果即可，不能等待所有RPC请求都返回之后再统计,部分请求超时会影响选举。
*/
func (rf *Raft) startElection(electTerm int) {
	var votes int32 = 1
	var waitGroup sync.WaitGroup
	n := len(rf.peers)
	waitGroup.Add(n - 1)
	for i := 0; i < n; i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			args := RequestVoteArgs{
				Term:         electTerm,
				CandidateId:  rf.me,
				LastLogIndex: 0, //log对应的index TODO
				LastLogTerm:  0, //log对应的term TODO
			}
			var reply RequestVoteReply
			ok := rf.sendRequestVote(i, &args, &reply)
			waitGroup.Done()
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.VoteGranted {
					atomic.AddInt32(&votes, 1)
				}
				if reply.Term > rf.currentTerm {
					DPrintf("electFailed:request Term %v, reply.Term: %v\n", args.Term, reply.Term)
					rf.updateTerm(reply.Term)
					rf.switchToFollower()
					return
				}
				if rf.state != Candidate || rf.currentTerm != electTerm {
					if rf.state != Leader {
						DPrintf("elect failed: state or term changed, state:%v, term:%v, elect term:%v", rf.state, rf.currentTerm, electTerm)
					}
					return
				}
				if votes > int32(n/2) {
					rf.switchToLeader()
				}
			}
		}(i)
	}
	waitGroup.Wait()
}

/*
leader发出心跳包
*/
func (rf *Raft) fireHeartBeats() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//no leader anymore, stop fire heart beats
	if rf.state != Leader {
		return
	}
	//DPrintf("node %v fire heart Beats state:%v   term:%v", rf.me, rf.state, rf.currentTerm)
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: 0, //TODkO
		PrevLogTerm:  rf.currentTerm,
		Entries:      nil,
		LeaderCommit: 0,
	}
	for i := 0; i < len(rf.peers); i++ {
		go func(serv int) {
			if serv != rf.me {
				reply := AppendEntriesReply{
				}
				rf.sendAppendEntries(serv, &args, &reply)
				if !reply.Success {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.updateTerm(reply.Term)
						rf.switchToFollower()
					}
				}
			}

		}(i)
	}
	<-time.After(HeartBeatInterval)
	go rf.fireHeartBeats()
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

/*
 */
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.syncCond = sync.NewCond(&rf.mu)

	// Your initialization code here (2A, 2B, 2C).
	rf.readPersist(persister.ReadRaftState())
	rf.switchToFollower()
	rf.state = Follower
	rf.lastHeartBeat = time.Now()
	rf.votedFor = NonVotes

	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.commitIndex = -1
	rf.commitC = make(chan struct{}, CommitBuffer)
	go rf.checkElecTimeout()

	// initialize from state persisted before a crash
	return rf
}
