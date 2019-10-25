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
	"bytes"
	"fmt"
	"labgob"
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
	HeartBeatInterval  = 50 * time.Millisecond
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
type RaftMu struct {
	mu     sync.Mutex
	t      time.Time
	action string
}

func (mu *RaftMu) Lock(action string) {
	//begin := time.Now()
	mu.mu.Lock()
	mu.t = time.Now()
	mu.action = action
	//DPrintf("[Lock]%s get the lock, cost:%v", action, mu.t.Sub(begin))
}

func (mu *RaftMu) Unlock() {
	//now := time.Now()
	//DPrintf("[Locks]action release %s Lock cost %v", mu.action, now.Sub(mu.t))
	mu.mu.Unlock()
}

type Raft struct {
	//mu        sync.Mutex // Lock to protect shared access to this peer's state
	mu        RaftMu // Lock to protect shared access to this peer's state
	syncCond  *sync.Cond
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	applyCh   chan ApplyMsg

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
	commitIndex         int
	commitC             chan struct{}
	lastApplied         int
	lastSendCommitIndex int

	//volatile state on leaders
	nextIndex          []int
	matchIndex         []int
	followerMatchIndex int //follower match with leader
	logs               []Entry
	killed             bool
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
	rf.mu.Lock("GetState")
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	//Your code here (2C).
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.logs = make([]Entry, 0) //start index is 1
		rf.logs = append(rf.logs, Entry{Term: 0})
		rf.currentTerm = 0
		rf.votedFor = NonVotes
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var logs []Entry;
	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		panic("decode error")
	} else {
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.logs = logs
	}
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
	//// Your code here (2A, 2B).
	rf.mu.Lock("RequestVote")
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

	if args.Term == rf.currentTerm && (rf.votedFor == NonVotes || rf.votedFor == args.CandidateId) && rf.candidateLogUpToDate(args) {
		rf.votedFor = args.CandidateId
		granted = true
		rf.updateHeartBeat() //重置选举时间
	}
	rf.persist()
	reply.Term = rf.currentTerm
	reply.VoteGranted = granted
	DPrintf("[RequestVotet]node:%d vote for node:%v oldTerm:%v Term:%v Granted:%v VotedFor:%v\n", rf.me, args.CandidateId, curTerm, args.Term, reply.VoteGranted, rf.votedFor)
}

func (rf *Raft) candidateLogUpToDate(args *RequestVoteArgs) bool {
	latestTerm := rf.logs[len(rf.logs)-1 ].Term
	return args.LastLogTerm > latestTerm || args.LastLogTerm == latestTerm && args.LastLogIndex >= len(rf.logs)-1
}

/**
更新更高的term
*/
func (rf *Raft) updateTerm(term int) {
	if term > rf.currentTerm {
		DPrintf("[updateTerm] node %v update term to %d from %d", rf.me, term, rf.currentTerm)
		rf.currentTerm = term
		rf.votedFor = NonVotes
	}
}

/**
当收到比自己大的term时，转为Follower,开始心跳检测
在个方法需要在临界区内调用。
*/
func (rf *Raft) switchToFollower() {
	DPrintf("node: %v switch to Follower from %v", rf.me, rf.state)
	if rf.state == Leader {
		go rf.checkElecTimeout()
	}
	rf.followerMatchIndex = 0
	rf.state = Follower
}

func (rf *Raft) switchToLeader() {
	DPrintf("term %d node: %v switch to Leader from %v", rf.currentTerm, rf.me, rf.state)
	if rf.state == Leader {
		return
	}
	rf.state = Leader
	rf.commitIndex = 0
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = 0
	}
	DPrintf("[switchToLeader] term %d leader infos: logs %v  next index %v  match index %v", rf.currentTerm, rf.logs, rf.nextIndex, rf.matchIndex)
	go rf.fireHeartBeats(rf.currentTerm)
	go rf.startReplicateEntry(rf.currentTerm)
	go rf.startUpdateCommitIndex(rf.currentTerm)
}

/**
heart beat and new entry request
*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock("AppendEntries")
	defer rf.mu.Unlock()
	//DPrintf("node %d from term %v, receive AppendEntries...", rf.me, args.Term)
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		DPrintf("[AppendEntries]request term too small  args:%v, current term:%d result:%v", args, rf.currentTerm, reply)
		return
	}

	stateChanged := false
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		rf.switchToFollower()
		stateChanged = true
	}

	rf.updateHeartBeat() //合法请求(可以认为存在leader)，重置心跳时间
	//log not match, delete stale logs
	if args.PrevLogIndex >= len(rf.logs) {
		return
	}
	if args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
		DPrintf("[AppendEntries]log dismatch current node %d args:%v, logs:%d reply:%v", rf.me, args, rf.logs, reply)
		validLogBoundery := min(args.PrevLogIndex, len(rf.logs))
		rf.logs = rf.logs[0:validLogBoundery]
		return
	}

	//不能直接写成rf.logs = append(rf.logs, args.Entries...)
	//如果raft节点有很多过期的log，比leader要长。前缀能跟leader match，但后缀直接append会导致log不一致。
	//只支持一个数据的复制
	if len(args.Entries) > 0 {
		if len(rf.logs)-1 == args.PrevLogIndex {
			rf.logs = append(rf.logs, args.Entries...)
		} else {
			if args.Entries[0].Term != rf.logs[args.PrevLogIndex+1].Term {
				rf.logs = rf.logs[0 : args.PrevLogIndex+1]
				rf.logs = append(rf.logs, args.Entries...)
			}
			//否则二者一定一致
		}
		stateChanged = true
	}
	if args.PrevLogIndex+len(args.Entries) > rf.followerMatchIndex {
		rf.followerMatchIndex = args.PrevLogIndex + len(args.Entries)
	}

	reply.Success = true
	if args.LeaderCommit > rf.commitIndex {

		//这里commitIndex还要去rf.commitIndex和rf.logs长度的最小值
		rf.commitIndex = min(args.LeaderCommit, rf.followerMatchIndex)
		rf.sendApplyMsg()
		DPrintf("[AppendEntries]Follower %d commit log index %d args %v %v", rf.me, rf.commitIndex, args, rf.logs)
	}
	if (len(args.Entries)) > 0 {
		DPrintf("[AppendEntries]node %d append entry %v, logs: %v", rf.me, args.Entries, rf.logs)
	}
	if stateChanged {
		rf.persist()
	}
}

func (rf *Raft) sendApplyMsg() {
	for i := rf.lastSendCommitIndex + 1; i <= rf.commitIndex && i < len(rf.logs); i++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		}
		rf.applyCh <- applyMsg
		rf.lastSendCommitIndex = i;
		DPrintf("node %d  index %d send apply msg %v", rf.me, i, applyMsg)
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
	rf.mu.Lock("Satrt")
	defer rf.mu.Unlock()
	isLeader = rf.state == Leader
	if !isLeader {
		return
	}
	term = rf.currentTerm
	entry := Entry{
		Term:    term,
		Command: command,
	}
	rf.logs = append(rf.logs, entry)
	DPrintf("[Start]leader %d append logs %v to index %v", rf.me, rf.logs, len(rf.logs)-1)
	rf.syncCond.Broadcast()
	return len(rf.logs) - 1, term, isLeader
}

/*
try to update commitIndex when replicate log success
*/
func (rf *Raft) startUpdateCommitIndex(term int) {
	for range rf.commitC {
		//DPrintf("[startUpdateCommitIndex]get node commitIndex update msg")
		go func() {
			rf.mu.Lock("UpdateCommitIndex")
			if rf.state != Leader || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			n := len(rf.peers)
			matchIndexes := make([]int, n)
			copy(matchIndexes, rf.matchIndex)
			matchIndexes[rf.me] = len(rf.logs)
			sort.Ints(matchIndexes)

			majorityMatchIndex := matchIndexes[(n-1)/2]
			if rf.currentTerm == rf.logs[majorityMatchIndex].Term && majorityMatchIndex > rf.commitIndex {
				rf.commitIndex = majorityMatchIndex
				rf.sendApplyMsg()
				DPrintf("[startUpdateCommitIndex]leader commit log index %d", rf.commitIndex)
			}
			rf.mu.Unlock()
		}()
	}
}

func (rf *Raft) startReplicateEntry(term int) {
	rf.mu.Lock("StartReplicateEntry")
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
		rf.mu.Lock("replicateToServ")
		DPrintf("[replicateToServ]start replication to %d of term %d next:%v match:%v", serv, term, rf.nextIndex, rf.matchIndex)
		if term != rf.currentTerm || rf.state != Leader || rf.killed {
			rf.mu.Unlock()
			return
		}
		for rf.nextIndex[serv] >= len(rf.logs) {
			rf.syncCond.Wait()
		}
		nextLogIndex := rf.nextIndex[serv]
		preLogIndex := nextLogIndex - 1
		preLogTerm := rf.logs[preLogIndex].Term
		entries := make([]Entry, 0)
		entries = append(entries, rf.logs[nextLogIndex])
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: preLogIndex,
			PrevLogTerm:  preLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		if len(args.Entries) > 0 {
			DPrintf("[replicateToServ] node %d replicate entries to serv %d index:%d term %d", rf.me, serv, preLogIndex+1, args.Term)
		}
		rf.mu.Unlock() //release lock here, wait rpc return
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(serv, args, reply)
		rf.mu.Lock("replicateToServ2")
		if !ok {
			rf.mu.Unlock()
			continue
		}
		if rf.state != Leader || rf.currentTerm != term || rf.killed {
			rf.mu.Unlock()
			return
		}
		rf.handleAppendEntriesResp(serv, nextLogIndex, args, reply)
		rf.mu.Unlock()
	}
}

func (rf *Raft) handleAppendEntriesResp(serv int, nextLogIndex int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Success {
		if nextLogIndex+len(args.Entries) > rf.nextIndex[serv] {
			rf.nextIndex[serv] = nextLogIndex + len(args.Entries)
		}
		if nextLogIndex > rf.matchIndex[serv] {
			rf.matchIndex[serv] = nextLogIndex - 1 + len(args.Entries)
			//must use goroutine
			//否则这个发送的逻辑是在rf.mu的临界区内，消费者读取消息的时候又需要获取这把锁。可能造成死锁
			go func(killed bool) {
				if !killed {
					rf.commitC <- struct{}{} // send on an closed channel
				}
			}(rf.killed)
		}
		if len(args.Entries) > 0 {
			DPrintf("[replicateToServ] replicate to serv %d index:%d success term %d", serv, nextLogIndex, args.Term)
		}
	} else {
		if reply.Term > rf.currentTerm {
			DPrintf("[replicateToServ] replicate to serv %d index:%d term invalid:replyTerm:%d ", serv, nextLogIndex, reply.Term)
			rf.updateTerm(reply.Term)
			rf.switchToFollower()
			rf.persist()
			return
		}
		//not match, decrease nextIndex
		if rf.nextIndex[serv] > 1 {
			//rf.nextIndex[serv] -- 这种写法可能有bug，假设心跳检测AppendEntries和尝试replicate的请求同时使用了相同的参数，两个请求都会被拒绝。实际LogIndex 应该减一
			rf.nextIndex[serv] = nextLogIndex - 1
			DPrintf("[replicateToServ] term %d node %v decrease next index for %v now: %d", rf.currentTerm, rf.me, serv, rf.nextIndex[serv])
			//decrease nextIndex，replicate goroutine can move on
			rf.syncCond.Broadcast()
		}
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
	rf.mu.Lock("Kill")
	defer rf.mu.Unlock()
	rf.killed = true
	//之前没有用goroutine，导致锁没有释放，进而导致后续一系列连锁反应。
	//这个问题找了好几天。。。
	go func() {
		<-time.After(time.Second * 1) //consume all msgs
		close(rf.commitC)
	}()
}

/*
检查选举超时，如果超时，发起选举
当Follower超过一段时间没有收到心跳包时，当前Raft节点发起选举
*/
func (rf *Raft) checkElecTimeout() {
	rf.mu.Lock("checkElecTimeout")
	if rf.killed == true {
		rf.mu.Unlock()
		return
	}
	electTimeOut := rf.randomElecTime()
	rf.resetElecTimer(electTimeOut)
	ch := rf.elecTimer.C
	rf.mu.Unlock()
	//wait to time out
	_, ok := <-ch
	if !ok {
		return
	}
	rf.mu.Lock("checkElecTimeout2")
	defer rf.mu.Unlock()
	d := time.Since(rf.lastHeartBeat)
	if rf.state == Leader {
		return
	}

	if d > electTimeOut {
		rf.currentTerm = rf.currentTerm + 1
		rf.state = Candidate
		rf.votedFor = rf.me
		rf.persist()
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
		go func(serv int) {
			rf.mu.Lock("startElection 1")
			args := RequestVoteArgs{
				Term:         electTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.logs) - 1,
				LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
			}
			rf.mu.Unlock()
			var reply RequestVoteReply
			ok := rf.sendRequestVote(serv, &args, &reply)
			waitGroup.Done()
			if ok {
				rf.mu.Lock("startElection 2")
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
func (rf *Raft) fireHeartBeats(term int) {
	rf.mu.Lock("fireHeartBeats")
	defer rf.mu.Unlock()
	DPrintf("node:%d term %v start fire heart beats", rf.me, rf.currentTerm)
	//no leader anymore, stop fire heart beats
	if rf.state != Leader || rf.currentTerm != term || rf.killed {
		return
	}
	//DPrintf("node %v fire heart Beats state:%v   term:%v", rf.me, rf.state, rf.currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(serv int) {
			rf.mu.Lock("fireHeartBeats 1")
			nextLogIndex := rf.nextIndex[serv]
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				Entries:      make([]Entry, 0),
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{
			}
			args.PrevLogIndex = nextLogIndex - 1
			if args.PrevLogIndex >= 0 {
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			}
			rf.mu.Unlock()
			DPrintf("leader %d send heart beats to serv %d", rf.me, serv)
			t1 := time.Now()
			ok := rf.sendAppendEntries(serv, args, reply)
			if !ok {
				return
			}
			t2 := time.Now()
			DPrintf("leader %d send heart beats to serv %d reply %v cost:%v", rf.me, serv, reply.Success, t2.Sub(t1))
			rf.mu.Lock("fireHeartBeats 2")
			defer rf.mu.Unlock()
			if rf.state != Leader || rf.currentTerm != term || rf.killed {
				return
			}
			//DPrintf("leader %d send heart beats to serv %d result %v", rf.me, serv, reply.Success)
			rf.handleAppendEntriesResp(serv, nextLogIndex, args, reply)
		}(i)
	}
	go func() {
		<-time.After(HeartBeatInterval)
		go rf.fireHeartBeats(term)
	}()
}

func (rf *Raft) String() string {
	return fmt.Sprintf("node:%d,\tterm %d\tstate:%v\tnext:%v \tlogs:%v", rf.me, rf.currentTerm, rf.state, rf.nextIndex, rf.logs)
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
	rf.mu = RaftMu{
		mu: sync.Mutex{},
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.syncCond = sync.NewCond(&rf.mu.mu)
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.readPersist(persister.ReadRaftState())
	rf.state = Follower
	rf.followerMatchIndex = 0
	rf.lastHeartBeat = time.Now()

	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.lastSendCommitIndex = 0
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.commitIndex = 0
	rf.commitC = make(chan struct{}, CommitBuffer)
	go rf.checkElecTimeout()

	// initialize from state persisted before a crash
	return rf
}
