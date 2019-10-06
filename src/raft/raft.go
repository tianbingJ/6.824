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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

type State int

func (s State) String() string{
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
	Follower   State = iota
	Candidate
	Leader
)

const (
	ElectionTimeoutMin = 150 * time.Millisecond
	ElectionTimeoutMax = 300 * time.Millisecond
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
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
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
	lastApplied int

	//volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

/*
entry log
*/
type Entry struct {
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
	var isleader bool

	// Your code here (2A).
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

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

/*
 */
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply = &RequestVoteReply{}
	granted := false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		DPrintf("vote reply from node: %v, for node: %v, args:%v   reply:%v\n", rf.me, args.CandidateId, args, reply)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.state != Follower {
			rf.switchToFollower(args.Term)
		}
	}
	//TODO 需要判断log是不是最新的
	if rf.votedFor == NonVotes || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		granted = true
		rf.updateHeartBeat() //重置选举时间
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = granted
	DPrintf("vote reply from node: %v,for node:%v,  args:%v   reply:%v\n", rf.me, args.CandidateId, args, reply)
}

/**
当收到比自己大的term时，转为Follower,开始心跳检测
原子性由调用该方法的地方实现。
*/
func (rf *Raft) switchToFollower(term int) {
	if rf.state == Follower {
		return
	}
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = NonVotes
	rf.checkElecTimeout()
}

/**
心跳检测和接受日志
*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if reply.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	rf.updateHeartBeat() //合法请求，重置心跳时间
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
}

/*
检查选举超时，如果超时，发起选举
当Follower超过一段时间没有收到心跳包时，当前Raft节点发起选举
*/
func (rf *Raft) checkElecTimeout() {
	elecTime := rf.randomElecTime()
	rf.resetElecTimer(elecTime)
	ch := rf.elecTimer.C
	_ = <-ch
	d := time.Since(rf.lastHeartBeat)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		return
	}

	if rf.state == Follower && d > elecTime {
		rf.currentTerm = rf.currentTerm + 1
		rf.state = Candidate
		rf.votedFor = rf.me
		//开始选举
		go rf.startElection(rf.currentTerm)
	}
	//开始新的心跳检测
	go rf.checkElecTimeout()
}

func (rf *Raft) resetElecTimer(duration time.Duration) {
	rf.elecTimer.Reset(duration)
}

/**
当收到合法RPC时，Follower需要更新心跳时间和选举超时时间
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
	rf.elecTimer.Stop()
}

/**
开始选举
这个方法可以优化，没有必要等待所有的投票全部返回即可统计结果。
*/
func (rf *Raft) startElection(newTerm int) {
	DPrintf("node:%d start election, Term %v: ", rf.me, newTerm)
	var votes int32 = 1
	var waitGroup sync.WaitGroup
	n := len(rf.peers)
	waitGroup.Add(n - 1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			args := RequestVoteArgs{
				Term:         newTerm,
				CandidateId:  rf.me,
				LastLogIndex: 0,
				LastLogTerm:  rf.currentTerm, //log对应的term
			}
			var reply RequestVoteReply
			ok := rf.sendRequestVote(i, &args, &reply)
			if ok {
				if reply.VoteGranted {
					atomic.AddInt32(&votes, 1)
				}
				if reply.Term > rf.currentTerm {
					rf.switchToFollower(reply.Term)
				}
			}
			waitGroup.Done()
			DPrintf("rpc result, node: %d election, Term %v: response from node %d, %v", rf.me, newTerm, i, reply)
		}(i)
	}
	waitGroup.Wait()
	DPrintf("total result: node %d election, Term %v: votes:%v", rf.me, newTerm, votes)
	//成为leader
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Candidate {
		return
	}
	if rf.currentTerm != newTerm {
		return
	}
	if votes > int32(n/2) {
		rf.switchToLeader()
	}
}

func (rf *Raft) switchToLeader() {
	rf.state = Leader
	rf.stopElecTimer()
}

/*
leader发出心跳包
TODO 实现
 */
func (rf *Raft) heartBeat() {

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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.state = Follower
	rf.votedFor = NonVotes
	rf.elecTimer = time.NewTimer(rf.randomElecTime())
	go rf.checkElecTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
