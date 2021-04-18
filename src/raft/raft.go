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
	"errors"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"

	"6.824/labgob"
	"6.824/labrpc"
	"github.com/sirupsen/logrus"
)

const (
	Leader uint8 = iota
	Candidate
	Follower 
)

const (
	ElectionTimeout time.Duration = 1500 * time.Millisecond
	HeartbeatTimeout time.Duration = 200 * time.Millisecond
	ReplicationTimeout time.Duration = 200 * time.Millisecond
	FailWait time.Duration = 10 * time.Millisecond
)

var (
	ErrNotLeader error = errors.New("Not a leader")
	ErrLeaderStepdown error = errors.New("Leader has stepped down")
	ErrLogNotFound error = errors.New("Log not found")
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
//
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()]
	applyCh	  chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	appliedIndex 		uint64					// (not used yet) highest index of log entry applied to the state machine
	commitIndex 		uint64					// (volatile) highest index of log entry replicated
	currentTerm			uint64					// (persistent) current term
	leader				int						// (volatile) leader id
	logger 				*logrus.Logger			// (don't care) logger instance
	logs 				*InmemLogStore			// (volatile) logs in memory
	lastContact 		time.Time				// (volatile) last contact by leader's heartbeat
	lastVotedTerm 		uint64 					// (persistent) last voted term
	lastVotedFor 		int						// (persistent) last voted candidate
	lastLogIndex		uint64					// cache of index of the last log
	lastLogTerm 		uint64					// cache of term of the last log
	state 				uint8					// (volatile) follower ? leader ? candidate ?
	notifyCh 			chan struct{} 			// notify main loop commit index has been updated
	snapshot 			[]byte					// (persistent) cache of last snapshot 
	snapshotLastIndex 	uint64					// (persistent) cache of last log index in the snapshot
	snapshotLastTerm 	uint64 					// (persistent) cache of last log term in the snapshot
	leaderState							    	// (volatile) states used by leader
}

// states used by leader
type leaderState struct {
	replState		map[int]*replicationState
	commitment 		*commitment			
	commitCh 		chan struct{}
}

// persistent states for general fields
type PersistState struct {
	CurrentTerm			uint64
	LastVotedTerm 		uint64
	LastVotedFor 		int
	Logs				map[uint64]*Log
}

// persistent states for snapshot
type PersistSnapshop struct {
	Snapshot 			[]byte
	SnapshotLastIndex 	uint64
	SnapshotLastTerm	uint64
}

// commitment keeps track of the committed index
type commitment struct {
	mu 				sync.Mutex
	matchIndexes 	map[int]uint64
	commitIndex 	uint64
	commitCh		chan struct{}
	startIndex 		uint64
}

type replicationState struct {
	mu 				sync.Mutex
	serverId 		int
	currentTerm 	uint64
	nextIndex 		uint64
	commitment 		*commitment
	notifyCh 		chan struct{}
}

type PersistLogStore struct{
	logs		map[uint64]*Log
	highIndex	uint64
	lowIndex 	uint64
}

type voteResult struct {
	server	int 
	ok 		bool
	reply 	*RequestVoteReply
}

// RequestVote RPC argument struct
type RequestVoteArgs struct {
	Term 			uint64
	CandidateId		int
	LastLogIndex	uint64
	LastLogTerm		uint64
}

// RequestVote RPC reply struct
type RequestVoteReply struct {
	Term			uint64
	VoteGranted		bool
}

// AppendEntries RPC argument struct 
type AppendEntriesArgs struct {
	Term 			uint64
	LeaderId		int
	LeaderCommit	uint64
	PrevLogIndex	uint64
	PrevLogTerm		uint64
	Entries 		[]*Log
}

// AppendEntries RPC reply struct
type AppendEntriesReply struct {
	Term 			uint64
	Success 		bool
	LastIndex		uint64
}

type InstallSnapshotArgs struct {
	Term 				uint64
	LeaderId			int 
	LastIncludedIndex 	uint64 
	LastIncludedTerm	uint64
	Data 				[]byte
}

type InstallSnapshotReply struct {
	Term 				uint64
}

type matchIndexSlice []uint64
func (s matchIndexSlice) Len() int           { return len(s) }
func (s matchIndexSlice) Less(i, j int) bool { return s[i] < s[j] }
func (s matchIndexSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (rf *Raft) getState() (s uint8){
	rf.mu.Lock()
	s = rf.state
	rf.mu.Unlock()
	return
}

func (rf *Raft) getLastEntry() (index uint64, term uint64){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = rf.lastLogIndex
	term = rf.lastLogTerm
	return
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = int(rf.currentTerm)
	isleader = rf.leader == rf.me
	rf.mu.Unlock()

	return term, isleader
}

func randomTimeout(base time.Duration) time.Duration {
	offset := time.Duration(rand.Int63()) % (base*2)
	return base + offset
}

func (rf *Raft) sendApplyMsg() {
	rf.mu.Lock()
	commitIndex := rf.commitIndex
	rf.mu.Unlock()
	for i := uint64(1); i<=commitIndex; i++ {
		var l Log
		if err := rf.readLog(i, &l); err != nil {
			return
		}
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command: l.Data,
			CommandIndex: int(l.Index),
			SnapshotValid: false,
		}
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.snapshotLastIndex > uint64(lastIncludedIndex) {
		// stale snapshot ? how ?
		return false
	}
	// persist snapshot
	rf.persistSnapshot(snapshot, uint64(lastIncludedIndex), false)
	rf.snapshot = snapshot
	rf.snapshotLastIndex = uint64(lastIncludedIndex)
	rf.snapshotLastTerm = uint64(lastIncludedTerm)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(endIndex int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// persist the snapshot
	rf.persistSnapshot(snapshot, uint64(endIndex), true)
	// clean up the logs
	rf.deleteLogs(rf.snapshotLastIndex+1, uint64(endIndex))
}

// installSnapshot PRC
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	if reply.Term > args.Term {
		return
	}
	rf.applyCh <- ApplyMsg{
		CommandValid: false,
		SnapshotValid: true,
		Snapshot: args.Data,
		SnapshotTerm: int(args.LastIncludedTerm),
		SnapshotIndex: int(args.LastIncludedIndex),
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(s *PersistState) error { 
	data := rf.persister.ReadRaftState()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		s.CurrentTerm = 0
		s.LastVotedFor = 0
		s.LastVotedTerm = 0
		s.Logs = make(map[uint64]*Log)
		return nil
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(s); err != nil {
		return err
	}
	return nil
}

func (rf *Raft) readSnapshot(s *PersistSnapshop) error {
	data := rf.persister.ReadSnapshot()
	if data == nil || len(data) < 1 {
		s.Snapshot = nil
		s.SnapshotLastIndex = 0
		s.SnapshotLastTerm = 0
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(s); err != nil {
		return err
	}
	return nil
}



// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// *WARNING*: don't hold the rf.mu lock, the calling chain should have a function tha has acquired the lock
func (rf *Raft) persist(state *PersistState, snapshot *PersistSnapshop) {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(state)
	dataState := w.Bytes()

	w = new(bytes.Buffer)
	e = labgob.NewEncoder(w)
	e.Encode(snapshot)
	dataSnapshot := w.Bytes()

	rf.persister.SaveStateAndSnapshot(dataState, dataSnapshot)
}

// *WARNING*: don't hold the rf.mu lock, calling function should have hold the lock
func (rf *Raft) persistSnapshot(snapshot []byte, lastIndex uint64, selfSnapshot bool){
	rf.logs.Lock()
	defer rf.logs.Unlock()
	var p PersistState
	var s PersistSnapshop
	p.CurrentTerm = rf.currentTerm
	p.LastVotedFor = rf.lastVotedFor
	p.LastVotedTerm = rf.lastVotedTerm
	p.Logs = rf.logs.copyOfLogs()
	s.Snapshot = snapshot
	s.SnapshotLastIndex = lastIndex
	s.SnapshotLastTerm = p.Logs[lastIndex].Term
	rf.persist(&p, &s)
	// update in memory fields
	if selfSnapshot == true {
		rf.snapshot = snapshot
		rf.snapshotLastIndex = lastIndex
		rf.snapshotLastTerm = p.Logs[lastIndex].Term
	}
}

// *WARNING*: don't hold the rf.mu lock, calling function should have hold the lock
func (rf *Raft) persistCurrentTerm(t uint64) {
	rf.logs.Lock()
	defer rf.logs.Unlock()
	var p PersistState
	var s PersistSnapshop
	p.CurrentTerm = t
	p.LastVotedFor = rf.lastVotedFor
	p.LastVotedTerm = rf.lastVotedTerm
	p.Logs = rf.logs.copyOfLogs()
	s.Snapshot = rf.snapshot
	s.SnapshotLastIndex = rf.snapshotLastIndex
	s.SnapshotLastTerm = p.Logs[rf.snapshotLastIndex].Term
	rf.persist(&p, &s)
	// update in memory fields
	rf.currentTerm = t
}

// *WARNING*: don't hold the rf.mu lock, calling function should have hold the lock
func (rf *Raft) persistVote(term uint64, candidateId int){
	rf.logs.Lock()
	defer rf.logs.Unlock()
	var p PersistState
	var s PersistSnapshop
	p.CurrentTerm = rf.currentTerm
	p.LastVotedFor = candidateId
	p.LastVotedTerm = term
	p.Logs = rf.logs.copyOfLogs()
	s.Snapshot = rf.snapshot
	s.SnapshotLastIndex = rf.snapshotLastIndex
	s.SnapshotLastTerm = p.Logs[rf.snapshotLastIndex].Term
	rf.persist(&p, &s)
	// update in memory fields
	rf.lastVotedTerm = term
	rf.lastVotedFor = candidateId
}

// *WARNING*: don't hold the rf.mu lock, calling function should have hold the lock
// writeLogs writes the logs to disk and memory
func (rf *Raft) writeLogs(newlogs []*Log) error{
	rf.logs.Lock()
	defer rf.logs.Unlock()
	var p PersistState
	var s PersistSnapshop
	p.CurrentTerm = rf.currentTerm
	p.LastVotedFor = rf.lastVotedFor
	p.LastVotedTerm = rf.lastVotedTerm
	p.Logs = rf.logs.copyOfLogs()
	for _, log := range newlogs {
		p.Logs[log.Index] = log
	}
	s.Snapshot = rf.snapshot
	s.SnapshotLastIndex = rf.snapshotLastIndex
	s.SnapshotLastTerm = p.Logs[rf.snapshotLastIndex].Term
	rf.persist(&p, &s)
	rf.logs.StoreLogs(newlogs)
	return nil
}

// *WARNING*: don't hold the rf.mu lock, calling function should have hold the lock
// readLog reads log from memory
func (rf *Raft) readLog(index uint64, l *Log) error{
	rf.logs.Lock()
	defer rf.logs.Unlock()
	err := rf.logs.GetLog(index, l)
	return err
}

// *WARNING*: don't hold the rf.mu lock, calling function should have hold the lock
// deleteLogs deletes logs from disk and memory
func (rf *Raft) deleteLogs(start, end uint64) error {
	rf.logs.Lock()
	defer rf.logs.Unlock()
	var p PersistState
	var s PersistSnapshop
	p.CurrentTerm = rf.currentTerm
	p.LastVotedFor = rf.lastVotedFor
	p.LastVotedTerm = rf.lastVotedTerm
	p.Logs = rf.logs.copyOfLogs()
	for i := start; i<=end; i++ {
		delete(p.Logs, i)
	}
	s.Snapshot = rf.snapshot
	s.SnapshotLastIndex = rf.snapshotLastIndex
	s.SnapshotLastTerm = p.Logs[rf.snapshotLastIndex].Term
	rf.persist(&p, &s)
	rf.logs.DeleteRange(start, end)
	return nil
}

// copyLogs returns a deep copy of logs
func (i *InmemLogStore) copyOfLogs() map[uint64]*Log {
	logcopy := make(map[uint64]*Log)
	for _, log := range i.logs {
		l := new(Log)
		*l = *log
		l.Data = copyValue(l)
		logcopy[l.Index] = l
	}
	return logcopy
}

// copyValue make a copy of interface type
func copyValue(l *Log) interface{} {
	return reflect.ValueOf(l.Data).Interface()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.killed() == true {
		return
	}

	if args.Term < reply.Term {
		// ignore older term
		return
	}

	if args.Term > reply.Term {
		//Persisit the term
		rf.persistCurrentTerm(args.Term)
		reply.Term = args.Term
	}

	if rf.lastVotedTerm == reply.Term {
		// no vote left
		return
	}

	if rf.lastLogTerm > args.LastLogTerm {
		return
	}

	if rf.lastLogTerm == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex {
		return
	}

	rf.persistVote(reply.Term, args.CandidateId)
	reply.VoteGranted = true
	return
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false 
	reply.Term = rf.currentTerm
	reply.LastIndex = rf.lastLogIndex
	
	if reply.Term > args.Term {
		return 
	}

	if args.Term > reply.Term || rf.state != Follower {
		rf.state = Follower 
		rf.persistCurrentTerm(args.Term)
		reply.Term = args.Term
	}

	rf.leader = args.LeaderId

	//check the previous log is identical
	if args.PrevLogIndex > 0 {
		var term uint64 
		if args.PrevLogIndex == rf.lastLogIndex {
			term = rf.lastLogTerm
		}else{
			var l Log
			if err := rf.readLog(args.PrevLogIndex, &l); err != nil{
				rf.logger.Errorf("1:GetLog error: %v", err)
				// missing logs
				// return the last log index, so leader will send from that index next time 
				reply.LastIndex = rf.lastLogIndex
				return
			}
			term = l.Term
		}
		if term != args.PrevLogTerm {
			// inconsistent logs
			// find the first index of the term
			startIndex := args.PrevLogIndex
			var l Log
			for i := args.PrevLogIndex; i > rf.snapshotLastIndex; i-- {
				if err := rf.readLog(i, &l); err != nil {
					rf.logger.Errorf("4:GetLog error: %v", err)
				}
				if l.Term != term {
					break
				}
				startIndex = i
			}
			reply.LastIndex = startIndex
			return
		}
	}

	if len(args.Entries) > 0 {
		// non-heartbeat
		var newlogs []*Log
		for i, entry := range args.Entries {
			if entry.Index > rf.lastLogIndex {
				newlogs = args.Entries[i:]
				break
			}
			// get the log
			var l Log 
			if err := rf.readLog(entry.Index, &l); err != nil {
				rf.logger.Errorf("3:GetLog error: %v", err)
				return
			}
			if l.Term != entry.Term {
				if err := rf.deleteLogs(l.Index, rf.lastLogIndex); err != nil {
					rf.logger.Errorf("DeleteRange error: %v", err) 
					return 
				}
				newlogs = args.Entries[i:]
				break
			}
		}
		if l := len(newlogs); l > 0 {
			// store the logs
			if err := rf.writeLogs(newlogs); err != nil {
				rf.logger.Errorf("StoreLogs error: %v", err)
				return
			}
			// update last log
			rf.lastLogIndex = newlogs[l-1].Index
			rf.lastLogTerm = newlogs[l-1].Term
		}
	}

	// update commit index
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(rf.lastLogIndex, args.LeaderCommit)
		go func(){
			// notify upper service
			rf.sendApplyMsg()
		}()
	}

	reply.Success = true
	rf.lastContact = time.Now()
	return
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = -1
	term = -1
	isLeader = true
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.leader == rf.me
	if isLeader == false {
		return
	}
	rf.lastLogIndex++
	rf.lastLogTerm = rf.currentTerm
	log := &Log{
		Data: command,
		Index: rf.lastLogIndex,
		Term: rf.currentTerm,
	}
	// persistently store the log 
	rf.writeLogs([]*Log{log})
	rf.commitment.mu.Lock()
	rf.commitment.matchIndexes[rf.me] = log.Index
	rf.commitment.mu.Unlock()
	// notify replication threads
	for _, peer := range rf.leaderState.replState {
		go func(r *replicationState){
			select {
			case r.notifyCh <- struct{}{}:
			default:
			}
		}(peer)
	}
	index = int(log.Index)
	term = int(log.Term)
	return
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
	}
}

func (rf *Raft) runFollower() {
	rf.logger.Infof("server %v entering Follower state", rf.me)
	for rf.getState() == Follower && rf.killed() == false {
		// select {
		// case r := <-rf.newLogCh:
		// 	r.respond(ErrNotLeader)
		// default:
		// }
		timeout := randomTimeout(ElectionTimeout)
		time.Sleep(timeout)
	
		rf.mu.Lock()
		lastContact := rf.lastContact
		rf.mu.Unlock()
	
		if lastContact.Add(timeout).After(time.Now()){
			continue
		}
		rf.logger.Infof("follower %v lost leader's heartbeat", rf.me)
	
		rf.mu.Lock()
		rf.leader = -1
		rf.state = Candidate
		rf.mu.Unlock()
	
		return
	}
}

func (rf *Raft) runCandidate() {
	rf.logger.Infof("server %v entering Candidate state", rf.me)

	rf.mu.Lock()
	rf.persistCurrentTerm(rf.currentTerm+1)
	newTerm := rf.currentTerm
	votesCh := rf.electSelf()
	rf.mu.Unlock()

	voteCount := 0
	voteNeed := len(rf.peers) / 2 + 1
	timeout:= time.After(ElectionTimeout)
	for rf.getState() == Candidate && rf.killed() == false {
		select {
		case <-timeout:
			rf.logger.Infof("candidate %v timeout", rf.me)
			return
		case vote := <-votesCh:
			if vote.ok == false {
				rf.logger.Infof("candidate %v has lost connection to %v", rf.me, vote.server)
				continue
			}

			if vote.reply.Term > newTerm {
				rf.logger.Infof("candidate %v receives newer term from %v", rf.me, vote.server)
			
				rf.mu.Lock()
				rf.persistCurrentTerm(vote.reply.Term)
				rf.state = Follower
				rf.mu.Unlock()
			
				return
			}

			if vote.reply.VoteGranted == true {
				rf.logger.Infof("candidate %v receives vote from %v", rf.me, vote.server)
				voteCount++
			}

			if voteCount >= voteNeed {
				rf.logger.Infof("candidate %v wins the election", rf.me)
			
				rf.mu.Lock()
				rf.leader = rf.me
				rf.state = Leader
				rf.mu.Unlock()
			
				return
			}
		// case r := <-rf.newLogCh:
		// 	r.respond(ErrNotLeader)
		}
	}
}

func (rf *Raft) electSelf() <-chan *voteResult{
	var args RequestVoteArgs

	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.lastLogIndex
	args.LastLogTerm = rf.lastLogTerm

	votesCh := make(chan *voteResult, len(rf.peers))

	askVote := func(serverId int, args *RequestVoteArgs, ch chan <- *voteResult){
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(serverId, args, reply)
		ch <- &voteResult{
			server: serverId,
			ok: ok,
			reply: reply,
		}
	}

	rf.persistVote(rf.currentTerm, rf.me)
	votesCh <- &voteResult{
		server: rf.me,
		ok: true,
		reply: &RequestVoteReply{
			Term: rf.currentTerm,
			VoteGranted: true,
		},
	}

	for peer := range rf.peers {
		if peer == rf.me {
			continue 
		}
		go askVote(peer, &args, votesCh)
	}

	return votesCh
}

func (rf *Raft) runLeader() {

	rf.logger.Infof("server %v entering Leader state", rf.me)

	rf.mu.Lock()
	rf.initializeLeaderState()
	rf.runAppendEntries()
	rf.mu.Unlock()

	defer rf.cleanupLeaderState()

	for rf.killed() == false && rf.getState() == Leader {
		select {
		case <- rf.leaderState.commitCh:
			rf.leaderState.commitment.mu.Lock()
			commitIndex := rf.leaderState.commitment.commitIndex
			rf.leaderState.commitment.mu.Unlock()
			rf.mu.Lock()
			rf.commitIndex = commitIndex
			rf.mu.Unlock()
			// notify tester
			go func(){
				rf.sendApplyMsg()
			}()
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) cleanupLeaderState() {
	rf.leaderState.replState = nil
	rf.commitment = nil
	rf.commitCh = nil
}

// lock is held in leader main thread
func (rf *Raft) initializeLeaderState() {
	rf.leaderState.replState = make(map[int]*replicationState, len(rf.peers)-1)
	rf.leaderState.commitCh = make(chan struct{}, 128)
	rf.leaderState.commitment = &commitment{
		matchIndexes: make(map[int]uint64, len(rf.peers)),
		commitIndex: rf.commitIndex,
		startIndex: rf.lastLogIndex,
		commitCh: rf.leaderState.commitCh,
	}

	//initialize matchIndexes
	for i := 0; i<len(rf.peers); i++ {
		rf.leaderState.commitment.matchIndexes[i] = 0
	}
}

// lock is held in leader main thread
func (rf *Raft) runAppendEntries() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		r := &replicationState{
			serverId: peer,
			currentTerm: rf.currentTerm,
			nextIndex: rf.lastLogIndex+1,
			commitment: rf.leaderState.commitment,
			notifyCh: make(chan struct{}, 1),
		}
		rf.leaderState.replState[peer] = r
		go rf.heartbeat(peer, rf.currentTerm)
		go rf.replicate(r)
	}
}

func (rf *Raft) replicate(r *replicationState){
	timeout := time.Tick(ReplicationTimeout)

	for {
		var args AppendEntriesArgs
		var reply AppendEntriesReply
		var err error

		select {
		case <- r.notifyCh:
		case <- timeout:
		}
	
		for{
			lastLogIndex, _ := rf.getLastEntry()
			if err = rf.makeAppendEntriesRequest(r, &args, lastLogIndex); err != nil {
				rf.logger.Errorf("replication thread for leader %v failed at makeAppendEntriesRequest: %v", rf.me, err)
				return
			}
			if ok := rf.sendAppendEntries(r.serverId, &args, &reply); ok == false {
				// rf.logger.Infof("replication thread for leader %v has lost connection with server %v", rf.me, r.serverId)
				time.Sleep(FailWait)
				continue
			}
			if reply.Success == false {
				if reply.Term > r.currentTerm {
					// found newer term, step down
					// rf.logger.Infof("replciation thead for leader %v has found newer term %v from server %v, stepping down", rf.me, reply.Term, r.serverId)
					rf.mu.Lock()
					rf.persistCurrentTerm(reply.Term)
					rf.leader = -1
					rf.state = Follower
					rf.mu.Unlock()
					return
				}
				// inconsistent log
				r.setNextIndex(reply.LastIndex)
				// send snapshot if log at next index is snapshotted
				rf.mu.Lock()
				snapshotLastIndex := rf.snapshotLastIndex
				rf.mu.Unlock()
				if r.nextIndex < snapshotLastIndex {
					// send snapshot
					var args1 InstallSnapshotArgs
					var reply1 InstallSnapshotReply
					rf.makeInstallSnapshotArgs(&args1)
					if ok := rf.sendSnapshot(r.serverId, &args1, &reply1); ok == false {
						time.Sleep(FailWait)
						continue
					}
					if reply1.Term > args1.Term {
						// found newer term, step down
						rf.mu.Lock()
						rf.persistCurrentTerm(reply.Term)
						rf.leader = -1
						rf.state = Follower
						rf.mu.Unlock()
						return
					}
					// update next index
					r.setNextIndex(args1.LastIncludedIndex + 1)
				}
				// next index is not in the snapshot
				time.Sleep(FailWait)
			}else{
			// rf.logger.Infof("replication thread for leader %v replicated %v logs to server %v", rf.me, len(args.Entries), r.serverId)
			// update leader commit index
			r.commitment.updateCommitIndex(r.serverId, lastLogIndex)
			// update next index 
			r.setNextIndex(lastLogIndex + 1)
			break
			}
		}
	}
}

func (rf *Raft) makeInstallSnapshotArgs(args *InstallSnapshotArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedTerm = rf.snapshotLastTerm
	args.LastIncludedIndex = rf.snapshotLastIndex
	args.Data = rf.snapshot
}


func (c *commitment) updateCommitIndex(serverId int, matchIndex uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.matchIndexes[serverId] = matchIndex
	c.calculateCommitIndex()
}

func (c *commitment) calculateCommitIndex() {
	matches := make([]uint64, 0, len(c.matchIndexes))
	for _, matchIndex := range c.matchIndexes {
		matches = append(matches, matchIndex)
	}
	sort.Sort(matchIndexSlice(matches))
	newCommitIndex := matches[(len(matches)-1)/2]
	if newCommitIndex > c.commitIndex && newCommitIndex > c.startIndex {
		c.commitIndex = newCommitIndex
		go func(){c.commitCh <- struct{}{}}()
	}

}

// makeAppendEntriesRequest constructs AppendEntriesArgs to send to peer 
func (rf *Raft) makeAppendEntriesRequest(r *replicationState, args *AppendEntriesArgs, lastLogIndex uint64) (err error) {
	err = nil
	nextIndex := r.getNextIndex()
	args.LeaderId = rf.me
	args.Term = r.currentTerm
	rf.mu.Lock()
	args.LeaderCommit = rf.commitIndex
	rf.mu.Unlock()
	if err = rf.makeLogs(args, nextIndex, lastLogIndex); err != nil{
		return err
	}
	if err = rf.makePrev(args, nextIndex); err != nil {
		return err
	}
	return
}

// makePrev is called by makeAppendEntriesRequest to continue setting up AppendEntriesArgs
func (rf *Raft) makePrev(args *AppendEntriesArgs, nextIndex uint64) error{
	if nextIndex == rf.snapshotLastIndex+1 {
		// start of the log
		args.PrevLogIndex = rf.snapshotLastIndex
		args.PrevLogTerm = rf.snapshotLastTerm
	}else{
		var l Log 
		if err := rf.readLog(nextIndex-1, &l); err != nil {
			rf.logger.Errorf("2:GetLog error: %v", err)
			return err
		}
		args.PrevLogIndex = l.Index
		args.PrevLogTerm = l.Term
	}
	return nil
}

// makeLogs is called by makeAppendEntriesRequest to set up AppendEntriesArgs
func (rf *Raft) makeLogs(args *AppendEntriesArgs, nextIndex, endIndex uint64) error {
	logs := make([]*Log, 0, 128)
	for i := nextIndex; i<=endIndex; i++ {
		l := new(Log)
		if err := rf.readLog(i, l); err != nil{
			return err
		}
		logs = append(logs, l)
	}
	args.Entries = logs
	return nil
}

func (r *replicationState) getNextIndex() (index uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	index = r.nextIndex
	return
}

func (r *replicationState) setNextIndex(index uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextIndex = index
	return
}

func (rf *Raft) heartbeat(serverId int, term uint64) {
	for {
		var args AppendEntriesArgs
		var reply AppendEntriesReply
		args.Term = term
		args.LeaderId = rf.me
		time.Sleep(HeartbeatTimeout)
		if err:= rf.sendAppendEntries(serverId, &args, &reply); err == false {
			// rf.logger.Infof("leader %v has lost connection to %v", rf.me, serverId)
			time.Sleep(FailWait)
			continue
		}
		if reply.Success == false {
			// rf.logger.Infof("leader %v's heartbeat was rejected by %v", rf.me, serverId)
			rf.mu.Lock()
			rf.persistCurrentTerm(reply.Term)
			rf.leader = -1
			rf.state = Follower
			rf.mu.Unlock()
			// rf.logger.Infof("leader %v's heartbeat thread for server %v is leaving", rf.me, serverId)
			return
		}
	}
}

func (rf *Raft) runRaft() {
	for rf.killed() == false {
		switch rf.getState() {
		case Leader:
			rf.runLeader()
		case Candidate:
			rf.runCandidate()
		case Follower:
			rf.runFollower()
		}
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

	rf.applyCh  = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.appliedIndex = 0
	rf.commitIndex = 0
	rf.currentTerm = 0
	rf.leader = -1
	rf.logger = logrus.New()
	rf.lastVotedTerm = 0 
	rf.lastVotedFor = 0
	rf.lastLogIndex = 0
	rf.lastLogTerm = 0
	rf.state = Follower
	// rf.newLogCh = make(chan *pending, MaxAppend)
	rf.logs = newInmemLogStore()

	// initialize from state persisted before a crash
	var s PersistState
	if err := rf.readPersist(&s); err != nil {
		rf.logger.Errorf("read persist error: %v", err)
	}	
	rf.currentTerm = s.CurrentTerm
	rf.lastVotedTerm = s.LastVotedTerm
	rf.lastVotedFor = s.LastVotedFor
	rf.logs.logs = s.Logs

	// update last log index & term
	var lastIndex uint64 = 0
	for _, log := range rf.logs.logs {
		if log.Index > lastIndex {
			lastIndex = log.Index
		}
	}
	rf.lastLogIndex = lastIndex
	if lastIndex == 0 {
		rf.lastLogTerm = 0
	}else {
		rf.lastLogTerm = rf.logs.logs[lastIndex].Term
	}

	// initialize snapshot state
	var s1 PersistSnapshop
	if err := rf.readSnapshot(&s1); err != nil {
		rf.logger.Errorf("read snapshot error: %v", err)
	}
	rf.snapshot = s1.Snapshot
	rf.snapshotLastIndex = s1.SnapshotLastIndex
	rf.snapshotLastTerm = s1.SnapshotLastTerm

	// start ticker goroutine to start elections
	// go rf.ticker()
	go rf.runRaft()

	return rf
}
