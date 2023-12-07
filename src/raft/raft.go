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

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type Entry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//(2A)
	currentTerm       atomic.Int32
	votedFor          atomic.Int32
	logs              []Entry
	heartbeatReceived atomic.Bool
	state             atomic.Int32
	commitIdx         int
	lastApplied       atomic.Int32
	nextIdx           []int
	matchIdx          []int
	applyCh           chan ApplyMsg
	lastSnapshotIdx   int
	lastSnapshotTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func min(a int, b int) int {
	if a <= b {
		return a
	}
	return b
}
func max(a int, b int) int {
	if a >= b {
		return a
	}
	return b
}

func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.currentTerm.Load()), rf.state.Load() == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(isLocked bool, snapshot []byte) {
	// Your code here (2C).
	// Example:
	if !isLocked {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(int(rf.currentTerm.Load())) != nil ||
		e.Encode(rf.votedFor.Load()) != nil ||
		e.Encode(rf.logs) != nil ||
		e.Encode(rf.lastSnapshotIdx) != nil ||
		e.Encode(rf.lastSnapshotTerm) != nil {
		println("failed to encode")
		return
	}
	raftstate := w.Bytes()
	if snapshot == nil {
		snapshot = rf.persister.ReadSnapshot()
	}
	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Entry
	var lastSnapIdx int
	var lastSnapTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastSnapIdx) != nil ||
		d.Decode(&lastSnapTerm) != nil {
		print("failed to decode")
		return
	} else {
		rf.currentTerm.Store(int32(currentTerm))
		rf.votedFor.Store(int32(votedFor))
		rf.logs = logs
		rf.lastSnapshotIdx = lastSnapIdx
		rf.lastSnapshotTerm = lastSnapTerm
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastSnapshotIdx || index == 0 {
		return
	}
	newLogIdx := index - rf.lastSnapshotIdx
	lastSnapshotTerm := rf.logs[newLogIdx-1].Term

	for i := range rf.nextIdx {
		if i != rf.me {
			rf.nextIdx[i] = index + 1
		}
	}

	rf.logs = rf.logs[newLogIdx:]
	rf.lastSnapshotIdx = index
	rf.lastSnapshotTerm = lastSnapshotTerm
	DPrintf("*snapshot* Persisted snapshot! peerID: %v, up-through index: %v", rf.me, index)
	rf.persist(true, snapshot)

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateID int
	Term        int
	LastLogIdx  int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	VotedYes  bool
	VoterTerm int
	FailedId  int
}

// (2A)
type AppendEntriesArgs struct {
	LeaderTerm   int
	LeaderID     int
	PrevLogIdx   int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}
type AppendEntriesReply struct {
	XLen           int
	XTerm          int
	XIdx           int
	FollowerTerm   int
	PrevInSnapshot bool
	Success        bool
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	votedFor := rf.votedFor.Load()
	curTerm := int(rf.currentTerm.Load())
	candidateTerm := args.Term
	if curTerm < candidateTerm {
		rf.currentTerm.Store(int32(candidateTerm))
		rf.state.Store(FOLLOWER)
		rf.persist(true, nil)
	}
	lastLogIdx := rf.lastSnapshotIdx + len(rf.logs)
	lastLogTerm := 0
	if len(rf.logs) == 0 {
		lastLogTerm = rf.lastSnapshotTerm
	} else {
		lastLogTerm = rf.logs[len(rf.logs)-1].Term
	}
	if ((candidateTerm > curTerm) || ((votedFor == -1 || votedFor == int32(args.CandidateID)) && candidateTerm == curTerm)) &&
		(lastLogTerm < args.LastLogTerm ||
			(lastLogTerm == args.LastLogTerm && lastLogIdx <= args.LastLogIdx)) {
		rf.heartbeatReceived.Store(true)
		reply.VotedYes = true
		rf.votedFor.Store(int32(args.CandidateID))
		rf.state.Store(FOLLOWER)
		rf.persist(true, nil)
	} else {
		reply.VotedYes = false
	}
	reply.VoterTerm = int(rf.currentTerm.Load())
	DPrintf("*election* voterLogLen: %v, lastLogTerm: %v, candidateLogLen: %v, lastTerm: %v", lastLogIdx+1, lastLogTerm, args.LastLogIdx+1, args.LastLogTerm)
	DPrintf("*election* candidateTerm: %v, voterTerm: %v", args.Term, rf.currentTerm.Load())
	DPrintf("*election* voter: %d, candidate: %d, res: %v", rf.me, args.CandidateID, reply.VotedYes)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.XIdx = -1
	reply.XTerm = -1
	reply.XLen = -1
	lastLogIdx := rf.lastSnapshotIdx + len(rf.logs)
	curPrevLogIdx := args.PrevLogIdx - rf.lastSnapshotIdx - 1
	shouldPersist := false
	prevInSnapshot := false
	DPrintf("*log* PeerID: %d\n *log* peerLogs: %v\n *log* appendLogs: %v", rf.me, rf.logs, args.Entries)
	// println("length of entries slice", len(args.Entries))
	DPrintf("*Append* leader: %d, follower: %d", args.LeaderID, rf.me)
	reply.FollowerTerm = int(rf.currentTerm.Load())
	if args.LeaderTerm < reply.FollowerTerm {
		DPrintf("*Append* args.LeaderTerm < int(rf.currentTerm.Load()) == %v", args.LeaderTerm < int(rf.currentTerm.Load()))
		reply.Success = false
		return
	} else if args.LeaderTerm != reply.FollowerTerm {
		rf.votedFor.Store(-1)
		rf.currentTerm.Store(int32(args.LeaderTerm))
		rf.state.Store(FOLLOWER)
		shouldPersist = true
	}
	rf.heartbeatReceived.Store(true)
	if lastLogIdx < args.PrevLogIdx {
		DPrintf("*Append* len(rf.logs) < args.PrevLogIdx == %v", lastLogIdx < args.PrevLogIdx)
		reply.XLen = lastLogIdx + 1
		reply.Success = false
		return
	}
	prevLogTerm := 0
	if curPrevLogIdx == -1 {
		prevLogTerm = rf.lastSnapshotTerm
	} else {
		prevLogTerm = rf.logs[curPrevLogIdx].Term
	}
	if prevLogTerm != args.PrevLogTerm {
		if prevInSnapshot {
			reply.PrevInSnapshot = true
		} else {
			reply.XTerm = prevLogTerm
			reply.XIdx = curPrevLogIdx
			for i := curPrevLogIdx; i > 0; i-- {
				if rf.logs[i].Term == reply.XTerm {
					reply.XIdx = i
				} else {
					break
				}
			}
			reply.XIdx += rf.lastSnapshotIdx + 1
		}
		reply.Success = false
	} else {
		DPrintf("*Append* Append accepted by peerID: %d from leaderID: %d", rf.me, args.LeaderID)
		rf.logs = append(rf.logs[:curPrevLogIdx+1], args.Entries...)
		if args.LeaderCommit > rf.commitIdx {
			rf.commitIdx = min(args.LeaderCommit, lastLogIdx)
		}
		if len(args.Entries) != 0 || reply.FollowerTerm < args.LeaderTerm {
			shouldPersist = true
		}
		reply.Success = true
	}
	if shouldPersist {
		rf.persist(true, nil)
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	curTerm := int(rf.currentTerm.Load())
	reply.Term = curTerm
	if args.Term < curTerm {
		return
	}
	rf.heartbeatReceived.Store(true)
	if args.Term > curTerm || rf.state.Load() != FOLLOWER {
		rf.votedFor.Store(-1)
		rf.currentTerm.Store(int32(args.Term))
		rf.state.Store(FOLLOWER)
	}
	if int(rf.lastApplied.Load()) >= args.LastIncludedIndex {
		rf.persist(true, nil)
		return
	}
	lastIdx := args.LastIncludedIndex - rf.lastSnapshotIdx - 1
	rf.commitIdx = args.LastIncludedIndex
	rf.lastApplied.Store(int32(args.LastIncludedIndex))
	rf.lastSnapshotIdx = args.LastIncludedIndex
	rf.lastSnapshotTerm = args.LastIncludedTerm
	DPrintf("*snapshot* Persisted snapshot! peerID: %v", rf.me)
	rf.persist(true, args.Data)

	if lastIdx < len(rf.logs) && rf.logs[lastIdx].Term == args.LastIncludedTerm {
		rf.logs = rf.logs[lastIdx+1:]
	} else {
		rf.logs = make([]Entry, 0)
	}
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

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
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := rf.state.Load() == LEADER
	if isLeader {
		rf.mu.Lock()
		nextIdx := rf.nextIdx[rf.me]
		DPrintf("*newLog* New Log added to LeaderID: %d, LogIdx: %v, LogValue: %v", rf.me, nextIdx, command)
		index = nextIdx
		term = int(rf.currentTerm.Load())
		rf.logs = append(rf.logs, Entry{Term: int(rf.currentTerm.Load()), Command: command})
		rf.matchIdx[rf.me] = nextIdx
		rf.nextIdx[rf.me] = nextIdx + 1
		rf.persist(true, nil)
		rf.mu.Unlock()
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// println("tick")

		// (2A)
		switch rf.state.Load() {
		case FOLLOWER:
			rf.followerTicker()
		case CANDIDATE:
			rf.startElection()
		case LEADER:
			rf.leaderTicker()
		}
	}
}

func (rf *Raft) leaderTicker() {
	ms := 45
	startTerm := int(rf.currentTerm.Load())
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if startTerm != int(rf.currentTerm.Load()) {
			return
		}

		go rf.appendEntriesResponseHandler(i, startTerm)
	}
	time.Sleep(time.Duration(ms) * time.Millisecond)
}
func (rf *Raft) getLogSlice(startLogIdx int) []Entry {
	var res []Entry
	res = append(res, rf.logs[startLogIdx:]...)
	return res

}

func (rf *Raft) lastIdxOfLogWithTerm(term int) int {
	for i := len(rf.logs) - 1; i >= 0; i-- {
		curLogTerm := rf.logs[i].Term
		if curLogTerm == term {
			return i
		} else if curLogTerm < term {
			return -1
		}
	}
	return -1
}

func (rf *Raft) appendEntriesResponseHandler(peerId int, startTerm int) {
	rf.mu.Lock()
	prevLogIdx := min(rf.nextIdx[peerId]-1, len(rf.logs)+rf.lastSnapshotIdx)
	realPrevLogIdx := prevLogIdx - rf.lastSnapshotIdx - 1
	DPrintf("*CheckIdx* LeaderID: %v, lastSnap: %v, loglen: %v, realIdx: %v", rf.me, rf.lastSnapshotIdx, len(rf.logs), realPrevLogIdx)
	prevLogTerm := 0
	if realPrevLogIdx == -1 {
		prevLogTerm = rf.lastSnapshotTerm
	} else {
		prevLogTerm = rf.logs[realPrevLogIdx].Term
	}
	reply := AppendEntriesReply{}
	logLen := len(rf.logs) + rf.lastSnapshotIdx + 1
	args := AppendEntriesArgs{LeaderTerm: startTerm, LeaderCommit: rf.commitIdx, LeaderID: rf.me, PrevLogIdx: prevLogIdx, PrevLogTerm: prevLogTerm, Entries: rf.getLogSlice(realPrevLogIdx + 1)}
	rf.mu.Unlock()
	ok := rf.sendAppendEntries(peerId, &args, &reply)
	followerTerm := reply.FollowerTerm
	for ok && !reply.Success && rf.state.Load() == LEADER {
		if followerTerm > int(rf.currentTerm.Load()) {
			rf.state.Store(FOLLOWER)
			rf.currentTerm.Store(int32(followerTerm))
			rf.votedFor.Store(-1)
			rf.persist(false, nil)
			return
		}
		if reply.PrevInSnapshot {
			rf.mu.Lock()
			snapArgs := InstallSnapshotArgs{
				Term:              startTerm,
				LeaderID:          rf.me,
				LastIncludedIndex: rf.lastSnapshotIdx,
				LastIncludedTerm:  rf.lastSnapshotTerm,
				Data:              rf.persister.ReadSnapshot(),
			}
			DPrintf("*Check* Inside IF prevlogidx: %v, lastSnapIdx: %v, peerID: %v", prevLogIdx, rf.lastSnapshotIdx, peerId)
			snapReply := InstallSnapshotReply{}
			rf.mu.Unlock()
			done := false
			for !done {
				done = rf.sendInstallSnapshot(peerId, &snapArgs, &snapReply)
			}
			rf.mu.Lock()
			followerTerm = snapReply.Term
			prevLogIdx = snapArgs.LastIncludedIndex
			rf.nextIdx[peerId] = prevLogIdx + 1
			rf.matchIdx[peerId] = prevLogIdx
			reply = AppendEntriesReply{XLen: -1, XTerm: -1, XIdx: -1}
			rf.mu.Unlock()
			continue
		} else {
			rf.mu.Lock()
			if reply.XLen != -1 {
				prevLogIdx = reply.XLen - 1
			} else if reply.XIdx >= 0 {
				lastIdx := rf.lastIdxOfLogWithTerm(reply.XTerm)
				if lastIdx != -1 {
					prevLogIdx = lastIdx - 1 + rf.lastSnapshotIdx + 1
				} else {
					prevLogIdx = reply.XIdx - 1
				}
			}
			if prevLogIdx < 0 {
				DPrintf("*RANGE* curTerm: %v, reply == %v", rf.currentTerm.Load(), reply)
			}
			DPrintf("*Check* Outside prevlogidx: %v, lastSnapIdx: %v, peerID: %v", prevLogIdx, rf.lastSnapshotIdx, peerId)
			realPrevLogIdx = prevLogIdx - rf.lastSnapshotIdx - 1
			if realPrevLogIdx < -1 {
				reply.PrevInSnapshot = true
				rf.mu.Unlock()
				continue
			}
			if realPrevLogIdx == -1 {
				prevLogTerm = rf.lastSnapshotTerm
			} else {
				prevLogTerm = rf.logs[realPrevLogIdx].Term
			}
			reply = AppendEntriesReply{}
			args = AppendEntriesArgs{LeaderTerm: startTerm, LeaderCommit: rf.commitIdx, LeaderID: rf.me, PrevLogIdx: prevLogIdx, PrevLogTerm: prevLogTerm, Entries: rf.getLogSlice(realPrevLogIdx + 1)}
			logLen = len(rf.logs) + rf.lastSnapshotIdx + 1
			rf.mu.Unlock()
			ok = rf.sendAppendEntries(peerId, &args, &reply)
			if prevLogIdx == 0 {
				break
			}
			followerTerm = reply.FollowerTerm
		}
	}
	if reply.Success {
		rf.mu.Lock()
		rf.nextIdx[peerId] = logLen
		rf.matchIdx[peerId] = logLen - 1
		DPrintf("*matchIdx* changed on idx: %d to val: %d", peerId, logLen-1)
		rf.mu.Unlock()
	}
}

func (rf *Raft) followerTicker() {
	ms := 300 + (rand.Int63() % 350)
	n := int(ms / 10)
	for i := 0; i < n; i++ {
		time.Sleep(time.Duration(10) * time.Millisecond)
		if rf.heartbeatReceived.Swap(false) {
			i = 0
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.heartbeatReceived.Swap(false) {
		rf.votedFor.Store(int32(rf.me))
		rf.state.Store(CANDIDATE)
		rf.currentTerm.Add(1)
		rf.persist(true, nil)
	}
}

func (rf *Raft) getLastIdxAndTerm() (int, int) {
	realLastLogIdx := len(rf.logs) - 1
	lastLogTerm := 0
	lastLogIdx := 0
	if realLastLogIdx >= 0 {
		lastLogTerm = rf.logs[realLastLogIdx].Term
		lastLogIdx = realLastLogIdx + rf.lastSnapshotIdx + 1
	} else {
		lastLogIdx = rf.lastSnapshotIdx
		lastLogTerm = rf.lastSnapshotTerm
	}
	return lastLogIdx, lastLogTerm
}

func (rf *Raft) startElection() {
	DPrintf("*election* Starting election Candidate: %d, TermNum: %d", rf.me, rf.currentTerm.Load())
	votedYes := 1
	votedNo := 0
	currentTerm := int(rf.currentTerm.Load())
	channel := make(chan *RequestVoteReply)
	for i := 0; i < len(rf.peers) && !rf.heartbeatReceived.Load(); i++ {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		lastLogIdx, lastLogTerm := rf.getLastIdxAndTerm()
		DPrintf("*CHECK* candidateloglen: %v, lastSnapIdx: %v, lastSnapTerm: %v", len(rf.logs), rf.lastSnapshotIdx, rf.lastSnapshotTerm)
		rf.mu.Unlock()
		args := RequestVoteArgs{CandidateID: rf.me, Term: currentTerm, LastLogIdx: lastLogIdx, LastLogTerm: lastLogTerm}
		reply := RequestVoteReply{}
		go rf.sendRequestVoteCh(channel, i, &args, &reply)
	}
	for votedYes <= len(rf.peers)/2 && votedNo < len(rf.peers)/2 && !rf.heartbeatReceived.Load() {
		reply := <-channel
		if reply.FailedId != -1 {
			failedId := reply.FailedId
			rf.mu.Lock()
			lastLogIdx, lastLogTerm := rf.getLastIdxAndTerm()
			DPrintf("*CHECK* candidateloglen: %v, lastSnapIdx: %v, lastSnapTerm: %v", len(rf.logs), rf.lastSnapshotIdx, rf.lastSnapshotTerm)
			args := RequestVoteArgs{CandidateID: rf.me, Term: currentTerm, LastLogIdx: lastLogIdx, LastLogTerm: lastLogTerm}
			reply := RequestVoteReply{}
			rf.mu.Unlock()
			go rf.sendRequestVoteCh(channel, failedId, &args, &reply)
			continue
		}
		DPrintf("*election* Received vote voterTerm: %d, candidateTerm:%d candidate: %d, res: %v", reply.VoterTerm, rf.currentTerm.Load(), rf.me, reply.VotedYes)
		if reply.VotedYes {
			votedYes++
		} else {
			votedNo++
		}
		voterTerm := reply.VoterTerm
		if currentTerm < voterTerm {
			rf.currentTerm.Store(int32(voterTerm))
			rf.votedFor.Store(-1)
			rf.state.Store(FOLLOWER)
			rf.persist(false, nil)
			return
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if int(rf.currentTerm.Load()) == currentTerm && votedYes > len(rf.peers)/2 {
		DPrintf("*election* Became Leader ID: %d, voteCount: %d", rf.me, votedYes)
		rf.state.Store(LEADER)
		rf.nextIdx = make([]int, len(rf.peers))
		rf.matchIdx = make([]int, len(rf.peers))
		for i := range rf.nextIdx {
			rf.nextIdx[i] = len(rf.logs) + rf.lastSnapshotIdx + 1
			rf.matchIdx[i] = 0
		}
		go rf.commitChecker()
	} else {
		DPrintf("*election* Didn't Become Leader ID: %d, voteCount: %d", rf.me, votedYes)
		rf.state.Store(FOLLOWER)
	}
	rf.persist(true, nil)

}

func (rf *Raft) sendRequestVoteCh(channel chan<- *RequestVoteReply, server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.sendRequestVote(server, args, reply)
	if !ok {
		reply.FailedId = server
	} else {
		reply.FailedId = -1
	}
	channel <- reply
}

func (rf *Raft) commitChecker() {
	ms := 35
	n := rf.commitIdx + 1
	for !rf.killed() && rf.state.Load() == LEADER {
		rf.mu.Lock()
		counter := 0
		logLen := len(rf.logs) + rf.lastSnapshotIdx + 1
		for i := range rf.nextIdx {
			if rf.matchIdx[i] >= n {
				counter++
			}
		}
		if counter > len(rf.peers)/2 {
			curIndex := n - rf.lastSnapshotIdx - 1
			if rf.logs[curIndex].Term == int(rf.currentTerm.Load()) {
				DPrintf("*commit* updated commit = %d, leaderID: %d", n, rf.me)
				rf.commitIdx = n
				n++
			} else {
				for n+1 < logLen {
					n++
					if rf.logs[curIndex].Term == int(rf.currentTerm.Load()) {
						break
					}
				}

				DPrintf("*commit* failed, logTerm: %v, curTerm: %v leaderID: %d, n = %v", rf.logs[curIndex].Term, rf.currentTerm.Load(), rf.me, n)
			}

		} else {
			DPrintf("*commit* Not enough counter: %d, peerID: %d", counter, rf.me)
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applyMsgTicker() {
	ms := 40
	for !rf.killed() {
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		commitIdx := rf.commitIdx
		lastApplied := int(rf.lastApplied.Load())
		lastSnapshotIdx := rf.lastSnapshotIdx
		var logs []Entry
		if commitIdx > lastApplied {
			logs = rf.getLogSlice(0)
		}
		rf.mu.Unlock()
		for commitIdx > lastApplied {
			lastApplied += 1
			curIdx := lastApplied - lastSnapshotIdx - 1
			DPrintf("*apply* applying: %v isLeader: %v peerID: %v logLen:%v logVal: %v\n", lastApplied, rf.state.Load() == LEADER, rf.me, len(logs), logs[curIdx].Command)
			rf.applyCh <- ApplyMsg{
				Command:      logs[curIdx].Command,
				CommandValid: true,
				CommandIndex: lastApplied,
			}
		}
		rf.lastApplied.Store(int32(lastApplied))
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	//(2A)
	rf.currentTerm.Store(0)
	rf.votedFor.Store(-1)
	rf.logs = []Entry{{}}
	rf.heartbeatReceived.Store(false)
	rf.state.Store(FOLLOWER)
	rf.commitIdx = 0
	rf.lastApplied.Store(0)
	rf.applyCh = applyCh
	rf.lastSnapshotIdx = -1
	rf.lastSnapshotTerm = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("*persist* after Loading from persister: %v", rf.logs)

	if rf.lastSnapshotIdx != -1 {
		rf.lastApplied.Store(int32(rf.lastSnapshotIdx))
		rf.commitIdx = rf.lastSnapshotIdx
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyMsgTicker()

	return rf
}
