package raft

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
	ElectionTimeoutMin = 1000 * time.Millisecond
	ElectionTimeoutMax = 2000 * time.Millisecond
	HeartBeatInterval  = 100 * time.Millisecond
)

const NonVotes = -1

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
	sendCH    chan ApplyMsg
	done      chan struct{}

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state     State
	elecTimer *time.Timer

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

/*
当ConflictingTerm > 0的时候表示有明确的log冲突，返回冲突日志的term和下标
当ConflictingTerm < 0的时候，表示当前对没有对应PrevLogIndex的日志，返回当前下表的长度，用户leader更新nextIndex下标
*/
type AppendEntriesReply struct {
	Term                  int
	Success               bool
	ConflictingTerm       int "conflicting term"
	StartConflictingIndex int "start index of conflicting term"
}

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

func (rf *Raft) readPersist(data []byte) {
	//Your code here (2C).
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.logs = make([]Entry, 0) //start index is 1
		rf.logs = append(rf.logs, Entry{Term: 0})
		rf.currentTerm = 0
		rf.votedFor = NonVotes
		rf.persist()
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

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

func (args RequestVoteArgs) String() string {
	return fmt.Sprintf("Term:%d, CandidaetId:%d, LastLogIndex:%d, LastLogTerm:%d", args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
}

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

	if (rf.votedFor == NonVotes || rf.votedFor == args.CandidateId) && rf.candidateLogUpToDate(args) {
		rf.votedFor = args.CandidateId
		granted = true
		rf.resetElecTimer() //重置选举时间
	}
	rf.persist()
	reply.Term = rf.currentTerm
	reply.VoteGranted = granted
	DPrintf("[RequestVotet]node:%d vote for node:%v oldTerm:%v Term:%v Granted:%v VotedFor:%v\n", rf.me, args.CandidateId, curTerm, args.Term, reply.VoteGranted, rf.votedFor)
}

func (rf *Raft) candidateLogUpToDate(args *RequestVoteArgs) bool {
	lastIndex := len(rf.logs) - 1
	latestTerm := rf.logs[lastIndex].Term
	return args.LastLogTerm > latestTerm || args.LastLogTerm == latestTerm && args.LastLogIndex >= lastIndex
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
		rf.elecTimer.Reset(rf.randomElecTime())
	}
	rf.followerMatchIndex = 0
	rf.state = Follower
}

func (rf *Raft) switchToLeader() {
	DPrintf("term %d node: %v switch to Leader from %v", rf.currentTerm, rf.me, rf.state)
	if rf.state == Leader {
		return
	}
	rf.elecTimer.Stop()
	rf.state = Leader
	rf.commitIndex = 0
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = 0
	}
	DPrintf("[switchToLeader] term %d leader infos:  next index %v  match index %v  loglen:%d logs %v", rf.currentTerm, rf.nextIndex, rf.matchIndex, len(rf.logs), rf.logs)
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

	rf.resetElecTimer() //合法请求(可以认为存在leader)，重置心跳时间
	stateChanged := false
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		rf.switchToFollower()
		stateChanged = true
	}

	lastIndex := len(rf.logs) - 1
	//见AppendEntriesReply注释
	if args.PrevLogIndex > lastIndex {
		reply.ConflictingTerm = -1
		reply.StartConflictingIndex = len(rf.logs) - 1
		return
	}
	//log not match, delete stale logs
	if args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
		validLogBoundery := min(args.PrevLogIndex, len(rf.logs))
		DPrintf("[AppendEntries]log dismatch current node %d args:%v, dismatchPoint:%d commitIndex:%d logs:%v reply:%v", rf.me, args, validLogBoundery, rf.commitIndex, rf.logs, reply)
		reply.ConflictingTerm = rf.logs[args.PrevLogIndex].Term
		reply.StartConflictingIndex = rf.firstLogIndexWithinTerm(reply.ConflictingTerm)
		rf.logs = rf.logs[0:validLogBoundery]
		return
	}

	//不能直接写成rf.logs = append(rf.logs, args.Entries...)
	//如果raft节点有很多过期的log，比leader要长。前缀能跟leader match，但后缀直接append会导致log不一致。
	//只支持一个数据的复制
	if len(args.Entries) > 0 {
		i := 0
		for ; i < len(args.Entries); i++ {
			index := args.PrevLogIndex + 1 + i
			if len(rf.logs) <= index {
				break
			}
			if args.Entries[i].Term != rf.logs[index].Term {
				rf.logs = rf.logs[0:index]
				break
			}
		}
		rf.logs = append(rf.logs, args.Entries[i:]...)
		//否则二者一定一致
		stateChanged = true
	}
	if args.PrevLogIndex+len(args.Entries) > rf.followerMatchIndex {
		rf.followerMatchIndex = args.PrevLogIndex + len(args.Entries)
	}

	reply.Success = true
	if args.LeaderCommit > rf.commitIndex {
		//这里commitIndex还要去rf.commitIndex和rf.logs长度的最小值
		rf.commitIndex = min(args.LeaderCommit, rf.followerMatchIndex)
		DPrintf("args %v reply %v preFollowerMatchIndex: %d %v", args, reply, rf.followerMatchIndex, rf)
		rf.sendApplych()
	}
	if stateChanged {
		rf.persist()
	}
}

/*
Follower send applyCh
*/
func (rf *Raft) sendApplych() {
	for i := rf.lastSendCommitIndex + 1; i <= rf.commitIndex && i < len(rf.logs); i++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		}
		rf.sendCH <- applyMsg
		rf.lastSendCommitIndex = i;
		DPrintf("node %d  index %d send apply msg %v", rf.me, i, applyMsg)
	}
}

func (rf *Raft) doSendApplyCH() {
	for {
		select {
		case msg := <-rf.sendCH:
			rf.applyCh <- msg
		case <-rf.done:
			return
		}
	}
}

func (rf *Raft) firstLogIndexWithinTerm(term int) int {
	firstIndex := -1
	for i := len(rf.logs) - 1; i >= 0; i-- {
		if rf.logs[i].Term < term {
			break
		}
		if rf.logs[i].Term == term {
			firstIndex = i
		}
	}
	DPrintf("first index %v", firstIndex)
	return firstIndex
}

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
	rf.persist()
	DPrintf("[Start]term %d leader %d append logs:%v len:%d logs:%v", rf.currentTerm, rf.me, command, len(rf.logs), rf.logs)
	rf.syncCond.Broadcast()
	return len(rf.logs) - 1, term, isLeader
}

/*
try to update commitIndex when replicate log success
*/
func (rf *Raft) startUpdateCommitIndex(term int) {
	for {
		select {
		case <-rf.done:
			return
		case <-rf.commitC:
			//DPrintf("[startUpdateCommitIndex]get node commitIndex update msg")
			rf.mu.Lock("UpdateCommitIndex")
			if rf.state != Leader || rf.currentTerm != term || rf.killed {
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
				rf.sendApplych()
				DPrintf("[startUpdateCommitIndex]leader commit log index %d logs:%v", rf.commitIndex, rf.logs)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) startReplicateEntry(term int) {
	rf.mu.Lock("StartReplicateEntry")
	defer rf.mu.Unlock()
	DPrintf("Leader infos %v", rf)
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
	DPrintf("[replicateToServ]start replication to %d of term %d next:%v match:%v", serv, term, rf.nextIndex, rf.matchIndex)
	for {
		rf.mu.Lock("replicateToServ")
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
		entries := rf.logs[nextLogIndex:]
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: preLogIndex,
			PrevLogTerm:  preLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		if len(args.Entries) > 0 {
			DPrintf("[replicateToServ] node %d replicate entries to serv %d index:%d term %d commitIndex:%d", rf.me, serv, preLogIndex+1, args.Term, args.LeaderCommit)
		}
		rf.mu.Unlock() //release lock here, wait rpc return
		reply := &AppendEntriesReply{}
		//不能一直等待rpc返回，如果个别rpc超时，要尽快发起新的RPC请求
		ok := rf.sendAppendEntries(serv, args, reply)
		if !ok {
			continue
		}
		rf.mu.Lock("replicateToServ2")
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
		if nextLogIndex-1+len(args.Entries) > rf.matchIndex[serv] {
			rf.matchIndex[serv] = nextLogIndex - 1 + len(args.Entries)
			//must use goroutine
			//否则这个发送的逻辑是在rf.mu的临界区内，消费者读取消息的时候又需要获取这把锁。可能造成死锁
			go func() {
				select {
				case <-rf.done:
					return
				case rf.commitC <- struct{}{}:
					return
				}
			}()
		}
		if len(args.Entries) > 0 {
			DPrintf("[replicateToServ] node %d replicate to serv %d index:%d success term %d, matchIndex:%d", rf.me, serv, nextLogIndex, args.Term, rf.matchIndex[serv])
		}
	} else {
		if reply.Term > rf.currentTerm {
			DPrintf("[replicateToServ] node %d replicate to serv %d index:%d term invalid:replyTerm:%d ", rf.me, serv, nextLogIndex, reply.Term)
			rf.updateTerm(reply.Term)
			rf.switchToFollower()
			rf.persist()
			return
		}
		before := rf.nextIndex[serv]
		nextIndex := rf.nextIndexWhenAppendFail(serv, reply)
		if rf.nextIndex[serv] > nextIndex {
			rf.nextIndex[serv] = nextIndex
		}
		DPrintf("[replicateToServ] term %d node %v decrease next index for %v before:%d now: %d", rf.currentTerm, rf.me, serv, before, rf.nextIndex[serv])
		rf.syncCond.Broadcast()
	}
}

/*
1.如果没有明确的冲突(Follower中没有足够长的log),next指向Follower log长度的下一个位置
如果leader包含冲突term的log，则返回nextIndex指向该term的最后一个log
如果leader不包含冲突term的log，则nextIndex指向startConflictIndex:Follower冲突term的第一个log
*/
func (rf *Raft) nextIndexWhenAppendFail(serv int, reply *AppendEntriesReply) int {
	if reply.ConflictingTerm < 0 {
		return reply.StartConflictingIndex + 1 //
	}
	for i := len(rf.logs) - 1; i >= 0; i-- {
		if rf.logs[i].Term == reply.ConflictingTerm {
			return i
		}
		if rf.logs[i].Term < reply.ConflictingTerm {
			break
		}
	}
	return reply.StartConflictingIndex
}

func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock("Kill")
	defer rf.mu.Unlock()
	DPrintf("Kill %d  infos:%v", rf.me, rf)
	rf.killed = true
	//之前没有用goroutine，导致锁没有释放，进而导致后续一系列连锁反应。
	//这个问题找了好几天。。。
	go func() {
		<-time.After(time.Second * 1) //consume all msgs
		close(rf.commitC)
	}()
	close(rf.done)
}

/*
检查选举超时，如果超时，发起选举
当Follower超过一段时间没有收到心跳包时，当前Raft节点发起选举
*/
func (rf *Raft) checkElecTimeout() {
	for {
		select {
		case <-rf.elecTimer.C:
			DPrintf("node:%d start election, infos:%v", rf.me, rf)
			go rf.startElection()
		case <-rf.done:
			DPrintf("%d DONE", rf.me)
			return
		}
	}
}

/**
guarded by rf.mu
*/
func (rf *Raft) resetElecTimer() {
	duration := rf.randomElecTime()
	rf.elecTimer.Stop()
	rf.elecTimer.Reset(duration)
}

func (rf *Raft) randomElecTime() time.Duration {
	interval := int64(ElectionTimeoutMax - ElectionTimeoutMin)
	elecTime := time.Duration(rand.Int63n(interval)) + ElectionTimeoutMin
	return elecTime
}

/**
开始选举
选举方法只需要等待足够数量的node返回选举结果即可，不能等待所有RPC请求都返回之后再统计,部分请求超时会影响选举。
*/
func (rf *Raft) startElection() {
	rf.mu.Lock("Start Election")
	defer rf.mu.Unlock()
	if rf.state == Leader {
		return
	}
	rf.resetElecTimer()
	rf.currentTerm += 1
	rf.state = Candidate
	rf.votedFor = rf.me
	//如果这里不设置0的话，候选人可能会含有过期的log，某些情况下可能会触发发送applyCh消息
	rf.followerMatchIndex = 0
	rf.persist()

	var votes int32 = 1
	n := len(rf.peers)
	for i := 0; i < n; i++ {
		if i == rf.me {
			continue
		}
		go func(serv int, elecTerm int) {
			rf.mu.Lock("startElection 1")
			args := RequestVoteArgs{
				Term:         elecTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.logs) - 1,
				LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
			}
			rf.mu.Unlock()
			var reply RequestVoteReply
			//t := time.Now()
			//DPrintf("node %d send RequestVote to node %d", rf.me, serv)
			ok := rf.sendRequestVote(serv, &args, &reply)
			if !ok {
				return
			}
			//DPrintf("node %d send RequestVote to node %d response %v request cost:%v", rf.me, serv, reply, time.Now().Sub(t))
			rf.mu.Lock("startElection 2")
			defer rf.mu.Unlock()
			if reply.VoteGranted {
				atomic.AddInt32(&votes, 1)
			}
			if reply.Term > rf.currentTerm {
				//DPrintf("electFailed:request Term %v, reply.Term: %v\n", args.Term, reply.Term)
				rf.updateTerm(reply.Term)
				rf.switchToFollower()
				rf.persist()
				return
			}
			if rf.state != Candidate || rf.currentTerm != elecTerm || rf.killed {
				return
			}
			//DPrintf("node:%d  term:%d votes:%d", rf.me, elecTerm, votes)
			if votes > int32(n/2) {
				rf.switchToLeader()
			}
		}(i, rf.currentTerm)
	}
}

/*
leader发出心跳包
*/
func (rf *Raft) fireHeartBeats(term int) {
	rf.mu.Lock("fireHeartBeats")
	defer rf.mu.Unlock()
	//DPrintf("node:%d term %v start fire heart beats", rf.me, rf.currentTerm)
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
			//DPrintf("leader %d send heart beats to serv %d", rf.me, serv)
			//t1 := time.Now()
			ok := rf.sendAppendEntries(serv, args, reply)
			if !ok {
				return
			}
			//t2 := time.Now()
			//DPrintf("leader %d send heart beats to serv %d reply %v cost:%v", rf.me, serv, reply.Success, t2.Sub(t1))
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
	s := fmt.Sprintf("node:%d,\tterm %d\tstate:%v\tnext:%v commitIndex:%d", rf.me, rf.currentTerm, rf.state, rf.nextIndex, rf.commitIndex)
	s += fmt.Sprintf("\tlogs info len :%d \n", len(rf.logs))
	for i := 0; i < len(rf.logs); i++ {
		s += fmt.Sprintf("[%d %d %v]", i, rf.logs[i].Term, rf.logs[i].Command)
	}
	return s + "\n"
}

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
	rf.done = make(chan struct{})

	rf.state = Follower
	rf.readPersist(persister.ReadRaftState())
	rf.followerMatchIndex = 0
	rf.elecTimer = time.NewTimer(rf.randomElecTime())
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.lastSendCommitIndex = 0
	rf.commitIndex = 0
	rf.commitC = make(chan struct{}, CommitBuffer)
	rf.sendCH = make(chan ApplyMsg, 10000)
	go rf.checkElecTimeout()
	go rf.doSendApplyCH()
	return rf
}
