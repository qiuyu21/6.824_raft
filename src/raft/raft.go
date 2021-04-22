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
	"errors"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Leader uint8 = iota
	Candidate
	Follower
)

const (
	ElectionTimeout time.Duration = 1500 * time.Millisecond
	HeartbeatTimeout time.Duration = 100 * time.Millisecond
	FailWait time.Duration = 20 * time.Millisecond 
)

var (
	ErrNotLeader error = errors.New("not a leader")
	ErrLogNotFound error = errors.New("log not found")
)

//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh	  chan ApplyMsg 
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state 			uint8
	currentTerm 	uint64
	commitIndex 	uint64
	leader 			int
	logStore 		*InmemLogStore
	logger 			*log.Logger
	lastContact 	time.Time
	lastVotedTerm 	uint64
	lastVotedFor 	int
	lastLogIndex 	uint64
	lastLogTerm 	uint64
	snapshot
	leaderState
}

func (rf *Raft) getState() (s uint8) {
	rf.mu.Lock()
	s = rf.state
	rf.mu.Unlock()
	return
}

func (rf *Raft) getLastContact() (t time.Time) {
	rf.mu.Lock()
	t = rf.lastContact
	rf.mu.Unlock()
	return
}

func (rf *Raft) getLastLogIndex() uint64 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastLogIndex
}

func (rf *Raft) getCommitIndex() uint64 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}

func (rf *Raft) getLastSnapshot() (index uint64, term uint64){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = rf.snapshot.snapshotLastIndex
	term = rf.snapshot.snapshotLastTerm
	return
}

func (rf *Raft) quorum() int {
	return len(rf.peers) / 2 + 1
}

func (rf *Raft) readLog(index uint64, log *Log) error {
	err := rf.logStore.GetLog(index, log)
	return err
}

// ** snapshot share the same lock as the Raft's mu lock
type leaderState struct {
	replicationState 	map[int]*replState
	commitment			*commitment
	commitCh 			chan uint64
}

type replState struct {
	serverId		int
	currentTerm		uint64
	nextIndex 		uint64
	commitment		*commitment
	notifyCh 		chan struct{}
}

type commitment struct {
	mu 				sync.Mutex
	matchIndexes 	map[int]uint64
	commitIndex 	uint64
	startIndex		uint64
	commitCh 		chan uint64
}

func (c *commitment) updateMatchIndex(serverId int, commitIndex uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.matchIndexes[serverId] = commitIndex
	return
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
	if newCommitIndex > c.commitIndex && newCommitIndex >= c.startIndex {
		c.commitIndex = newCommitIndex
		// notify leader main thread
		go func(){c.commitCh <- newCommitIndex}()
	}
}

type voteResult struct {
	serverId	int
	ok 			bool		
	reply 		*RequestVoteReply
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = int(rf.currentTerm)
	isleader = (rf.leader == rf.me)
	rf.mu.Unlock()
	return term, isleader
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term > reply.Term {
		rf.persistCurrentTerm(args.Term)
		rf.state = Follower
		rf.leader = -1
		reply.Term = args.Term
	}
	if args.Term < reply.Term {
		//reject candidate with old term
		return
	}
	if rf.leader != -1 && rf.leader != args.CandidateId {
		return
	}
	if rf.lastVotedTerm == reply.Term {
		// already voted in the current term
		return
	}
	if rf.lastLogTerm > args.LastLogTerm {
		// candidate is not as up-to-date
		return
	}
	if rf.lastLogTerm == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex {
		// candidate is not as up-to-date
		return
	}
	rf.persistVote(reply.Term, args.CandidateId)
	reply.VoteGranted = true
	return
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.currentTerm
	reply.LastIndex = rf.lastLogIndex
	if reply.Term > args.Term {
		// old leader
		return
	}

	if args.Term > reply.Term || rf.state != Follower {
		rf.persistCurrentTerm(args.Term)
		rf.state = Follower
		// rf.leader = -1
		reply.Term = args.Term
	}

	rf.leader = args.LeaderId

	if args.PrevLogIndex > rf.lastLogIndex {
		// missing logs, return index of the last log
		return
	}

	if args.PrevLogIndex > 0 {
		// verify previous log
		var l1 Log 
		if err := rf.readLog(args.PrevLogIndex, &l1); err != nil {
			rf.logger.Printf("no log found at PrevLogIndex:%v",args.PrevLogIndex)
			return
		}

		if args.PrevLogTerm != l1.Term {
			// the term of previous log conflicts with the leader's
			// find the first log of the conflicting term
			var l2 Log 
			startIndex := args.PrevLogIndex
			for i := args.PrevLogIndex; i >= rf.logStore.FirstIndex(); i--{
				if err := rf.readLog(i, &l2); err != nil {
					rf.logger.Fatalf("read log failed 1")
				}
				if l2.Term != l1.Term {
					break
				}
				startIndex = i
			}
			reply.LastIndex = startIndex
			return
		}
	}

	if len(args.Entries) > 0 {
		var newLogs []*Log
		for i, entry := range args.Entries {
			if entry.Index > rf.lastLogIndex {
				newLogs = args.Entries[i:]
				break
			}
			var l Log
			if err := rf.readLog(entry.Index, &l); err != nil {
				rf.logger.Fatalf("read log failed 2")
				return
			}
			if l.Term != entry.Term {
				rf.persistDeleteLogs(l.Index, rf.lastLogIndex)
				newLogs = args.Entries[i:]
				break
			}
		}

		if l := len(newLogs); l > 0 {
			rf.persistWriteLogs(newLogs);
			rf.lastLogIndex = newLogs[l-1].Index
			rf.lastLogTerm = newLogs[l-1].Term
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex)
		// notify tester
		go func(){rf.sendApplyMsg()}()
	}

	reply.Success = true 
	rf.lastContact = time.Now()
	return
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
//
func (rf *Raft) Start(command interface{})  (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = -1
	term = -1
	isLeader = (rf.me == rf.leader)
	if isLeader == false {
		return
	}
	newIndex := rf.lastLogIndex + 1
	// persist the new log
	rf.persistWriteLogs([]*Log{&Log{Index:newIndex, Term: rf.currentTerm, Data: command}})
	rf.lastLogIndex = newIndex
	rf.lastLogTerm = rf.currentTerm
	// update commitment
	// notify leader to update match index
	rf.leaderState.commitment.updateMatchIndex(rf.me, newIndex)
	for _, peer := range rf.leaderState.replicationState {
		// notify replication thread
		go func(r *replState){
			select {
			case r.notifyCh <- struct{}{}:
			default:	
			}
		}(peer)
	}
	index = int(newIndex)
	term = int(rf.currentTerm)
	return index, term, isLeader
}

func (rf *Raft) sendApplyMsg() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.logStore.FirstIndex(); i <= rf.commitIndex; i++ {
		var l Log 
		if err := rf.readLog(i, &l); err != nil {
			rf.logger.Fatalf("read log error")
			return
		}
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command: l.Data,
			CommandIndex: int(l.Index),
		}
	}
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

func (rf *Raft) runLeader(){
	rf.logger.Printf("Server %v entering Leader state", rf.me)
	rf.mu.Lock()
	rf.initializeLeaderState()
	rf.runAppendEntries()
	rf.mu.Unlock()
	defer rf.cleanupLeaderState()

	for rf.getState() == Leader && rf.killed() == false {
		select {
		case newCommitIndex:= <- rf.leaderState.commitCh:
			// commit index has been updated
			// update leader's raft commit index
			rf.mu.Lock()
			rf.commitIndex = newCommitIndex
			rf.mu.Unlock()
			// notify tester
			go func(){
				rf.sendApplyMsg()
			}()
		default:
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func (rf *Raft) initializeLeaderState() {
	rf.leaderState.replicationState = make(map[int]*replState)
	rf.leaderState.commitCh = make(chan uint64)
	rf.leaderState.commitment = &commitment{
		matchIndexes: make(map[int]uint64),
		commitIndex: rf.commitIndex,
		startIndex: rf.lastLogIndex + 1,
		commitCh: rf.leaderState.commitCh,
	}
	for i := range rf.peers {
		rf.leaderState.commitment.matchIndexes[i] = 0
	}
}

func (rf *Raft) cleanupLeaderState() {
	rf.lastContact = time.Now()
	rf.leaderState.replicationState = nil
	rf.leaderState.commitCh = nil
	rf.leaderState.commitment = nil
}

func (rf *Raft) runAppendEntries() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		repl := &replState{
			serverId: peer,
			currentTerm: rf.currentTerm,
			nextIndex: rf.lastLogIndex+1,
			commitment: rf.leaderState.commitment,
			notifyCh: make(chan struct{}),
		}
		rf.leaderState.replicationState[peer] = repl
		go rf.replicate(repl)
	}
}

func (rf *Raft) replicate(repl *replState){
	for rf.getState() == Leader && rf.killed() == false {
		var args AppendEntriesArgs
		var reply AppendEntriesReply
		time.Sleep(HeartbeatTimeout)
		lastLogIndex := rf.getLastLogIndex()
		if err := rf.setupAppendEntriesArgs(repl, &args, lastLogIndex); err == ErrLogNotFound {
			// send snapshot
			rf.logger.Fatalf("snapshot has not implemented yet")
		}else if err != nil {
			rf.logger.Fatalf("bug bug bug")
		}

		if ok := rf.sendAppendEntries(repl.serverId, &args, &reply); ok == false {
			time.Sleep(FailWait)
			continue
		}
		if reply.Success == false {
			if reply.Term > repl.currentTerm {
				rf.mu.Lock()
				rf.persistCurrentTerm(reply.Term)
				rf.leader = -1
				rf.state = Follower
				rf.mu.Unlock()
				return
			}
			// inconsistent logs
			repl.nextIndex = max(min(repl.nextIndex-1, reply.LastIndex), 1)
		}else{
			// reply was successful
			// update next index
			if l := len(args.Entries); l > 0 {
				lastlog := args.Entries[l-1]
				repl.nextIndex = lastlog.Index + 1
				repl.commitment.updateCommitIndex(repl.serverId, lastlog.Index)
			}
		}
	}
}

func (rf *Raft) setupAppendEntriesArgs(r *replState, args *AppendEntriesArgs, endIndex uint64) (err error){
	args.Term = r.currentTerm
	args.LeaderId = rf.me	
	args.LeaderCommit = rf.getCommitIndex()
	if err := rf.makePrev(args, r.nextIndex); err != nil {
		return err
	}
	if err := rf.makeLogs(args, r.nextIndex, endIndex); err != nil {
		return err
	}
	return nil
}

func (rf *Raft) makePrev(args *AppendEntriesArgs, nextIndex uint64) error {
	snapshotLastIndex, snapshotLastTerm := rf.getLastSnapshot()
	if nextIndex == 1 {
		args.PrevLogIndex = 0
		args.PrevLogTerm = 0
	}else if (nextIndex - 1) == snapshotLastIndex {
		args.PrevLogIndex = snapshotLastIndex
		args.PrevLogTerm = snapshotLastTerm
	}else {
		var l Log 
		if err := rf.readLog(nextIndex-1, &l); err != nil {
			return err
		}
		args.PrevLogIndex = l.Index
		args.PrevLogTerm = l.Term
	}
	return nil
}

func (rf *Raft) makeLogs(args *AppendEntriesArgs, startIndex, endIndex uint64) error {
	logs := make([]*Log, 0, (endIndex - startIndex + 1))
	for i := startIndex; i <= endIndex; i++ {
		l := new(Log)
		if err := rf.readLog(i, l); err != nil {
			return err
		}
		logs = append(logs, l)
	}
	args.Entries = logs
	return nil
}

func (rf *Raft) runCandidate(){
	rf.logger.Printf("Server %v entering Candidate state", rf.me)
	term, votesCh := rf.electSelf()
	voteCount := 0
	voteNeed := rf.quorum()
	timeout := time.After(ElectionTimeout)
	for rf.getState() == Candidate && rf.killed() == false {
		select {
		case <- timeout:
			return
		case vote := <-votesCh:
			if vote.ok == false {
				continue
			}
			
			if vote.reply.Term > term {
				rf.mu.Lock()
				rf.persistCurrentTerm(vote.reply.Term)
				rf.state = Follower
				rf.mu.Unlock()
				return
			}

			if vote.reply.VoteGranted == true {
				voteCount++
			}

			if voteCount >= voteNeed {
				rf.mu.Lock()
				rf.leader = rf.me
				rf.state = Leader
				rf.mu.Unlock()
				// send an initial heartbeat
				go rf.onetimeheartbeat(rf.me, term)
				return
			}
		}
	}
}

func (rf *Raft) onetimeheartbeat(leaderId int, term uint64){
	for peer := range rf.peers {
		if peer == leaderId {
			continue
		}else{
			go func(serverId int){
				var args AppendEntriesArgs
				var reply AppendEntriesReply
				args.LeaderId = leaderId
				args.Term = term
				if ok := rf.sendAppendEntries(serverId, &args, &reply); ok == false {
					return
				}
				if reply.Success == false && reply.Term > term {
					rf.mu.Lock()
					rf.persistCurrentTerm(reply.Term)
					rf.leader = -1
					rf.state = Follower
					rf.mu.Unlock()
					return
				}
			}(peer)
		}
	}
}


func (rf *Raft) electSelf() (uint64, <-chan *voteResult) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// increment current term
	rf.persistCurrentTerm(rf.currentTerm + 1)
	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.lastLogIndex
	args.LastLogTerm = rf.lastLogTerm
	
	votesCh := make(chan *voteResult, len(rf.peers))
	askVote := func (serverId int, args *RequestVoteArgs, pollCh chan<- *voteResult){
		reply := new(RequestVoteReply)
		ok := rf.sendRequestVote(serverId, args, reply)
		pollCh <- &voteResult{
			serverId: serverId,
			ok: ok,
			reply: reply,
		}
	}
	// persist the vote
	rf.persistVote(rf.currentTerm, rf.me)
	// vote for itself
	votesCh <- &voteResult{
		serverId: rf.me,
		ok: true,
		reply: &RequestVoteReply{
			Term: args.Term,
			VoteGranted: true,
		},
	}
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}else{
			go askVote(peer, &args, votesCh)
		}
	}
	return rf.currentTerm, votesCh
}

func (rf *Raft) runFollower(){
	rf.logger.Printf("Server %v entering Follower state", rf.me)
	for rf.killed() == false {
		timeout := randomTimeout(ElectionTimeout)
		time.Sleep(timeout)
		lastContact := rf.getLastContact()
		if lastContact.Add(timeout).After(time.Now()){
			continue
		}
		rf.mu.Lock()
		rf.leader = -1
		rf.state = Candidate
		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) runRaft(){
	for rf.killed() == false {
		switch rf.getState(){
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
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.commitIndex = 0
	rf.leader = -1
	rf.logger = log.New(os.Stdout, "", log.LstdFlags)

	var pState PersistState
	if err := rf.readState(&pState); err != nil {
		rf.logger.Fatalf("read state failed: %v", err)
	}

	rf.currentTerm = pState.CurrentTerm
	rf.lastVotedFor = pState.LastVotedFor
	rf.lastVotedTerm = pState.LastVotedTerm
	rf.logStore = NewInmemLogStore()
	if pState.LogStore != nil {
		rf.logStore.LowIndex = pState.LogStore.LowIndex
		rf.logStore.HighIndex = pState.LogStore.HighIndex
		rf.logStore.Logs = pState.LogStore.Logs
	}

	var pSnapshot PersistSnapshot
	if err := rf.readSnapshot(&pSnapshot); err != nil {
		rf.logger.Fatalf("read snapshot failed: %v", err)
	}

	rf.snapshot.snapshot = pSnapshot.Snapshot
	rf.snapshot.snapshotLastIndex = pSnapshot.SnapshotLastIndex
	rf.snapshot.snapshotLastTerm = pSnapshot.SnapshotLastTerm

	rf.lastLogIndex = rf.snapshot.snapshotLastIndex + rf.logStore.LastIndex()
	var l Log
	rf.readLog(rf.lastLogIndex, &l)
	rf.lastLogTerm = l.Term

	go rf.runRaft()

	return rf
}
