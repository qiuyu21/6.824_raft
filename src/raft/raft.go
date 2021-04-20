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
	"math/rand"
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
	HeartbeatTimeout time.Duration = 200 * time.Millisecond
	ReplicationTimeout time.Duration = 200  * time.Millisecond
	FailWait time.Duration = 10 * time.Millisecond 
)

var (
	ErrNotLeader error = errors.New("not a leader")
	ErrLeaderStepdown error = errors.New("leader has stepped down")
	ErrLogNotFound error = errors.New("log not found")
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
	dead      int32               // set by Kill()
	applyCh	  chan ApplyMsg 
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state			uint8			// (N/A) follower ? leader ? candidate ?
	currentTerm		uint64 			// (persistent) current term 
	commitIndex		uint64			// (N/A) highest index of log entry replicated
	leader 			int 			// (N/A) leader id
	logStore 		*InmemLogStore	// (persistent) store log in memory
	logger 			*log.Logger		// (N/A) logger instance			
	lastContact 	time.Time		// (N/A) last contact time by leader's heartbeat
	lastVotedTerm	uint64 			// (persistent) last voted term 
	lastVotedFor 	int 			// (persistent) last voted candidate
	lastLogIndex	uint64 			// (N/A) index of the last log
	lastLogTerm		uint64 			// (N/A) term of the last log
	snapshot
	leaderState
}

// return the state of the raft node. Whether is leader ? follower ? candidate ?
func (rf *Raft) getState() uint8 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) getLastContact() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastContact
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

func (rf *Raft) readLog(index uint64, log *Log) error {
	err := rf.logStore.GetLog(index, log)
	return err
}

// ** snapshot share the same lock as the Raft's mu lock
type snapshot struct {
	snapshot 			[]byte 			// (persistent)	latest snapshot
	snapshotLastIndex	uint64 			// (persistent)	latest snapshot last log's index 
	snapshotLastTerm	uint64			// (persistent) latest snapshot last log's term
	snapshotCh			chan struct{}	// (N/A) notify leader a new snapshot has been taken
}

func (rf *Raft) getLastSnapshot() (index uint64, term uint64){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = rf.snapshot.snapshotLastIndex
	term = rf.snapshot.snapshotLastTerm
	return
}

// ** snapshot share the same lock as the Raft's mu lock
type leaderState struct {
	replicationState 	map[int]*replicationState	// replication status for each peer
	commitment 			*commitment					// keeps track of the current commit index
	commitCh 			chan struct{}				// notify leader commit index has been updated
}

type commitment struct {
	mu  				sync.Mutex
	matchIndexes		map[int]uint64 	// index of highest log entry known to be replication on server
	commitIndex 		uint64			// committed log index 
	commitCh	 		chan struct{}	// reference to the same channel in leaderState
	startIndex			uint64 			// start log index of the leader 
}

func (c *commitment) getCommitIndex() (commitIndex uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	commitIndex = c.commitIndex
	return
}

func (c *commitment) updateMatchIndex(serverId int, commitIndex uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.matchIndexes[serverId] = commitIndex
	return
}

func (c *commitment) updateCommitIndex(serverId int, matchIndex uint64) {
	calculateCommitIndex := func(){
		matches := make([]uint64, 0, len(c.matchIndexes))
		for _, matchIndex := range c.matchIndexes {
			matches = append(matches, matchIndex)
		}
		sort.Sort(matchIndexSlice(matches))
		newCommitIndex := matches[(len(matches)-1)/2]
		if newCommitIndex > c.commitIndex &&  newCommitIndex > c.startIndex {
			c.commitIndex = newCommitIndex
			// notify leader main loop
			go func(){c.commitCh <- struct{}{}}()
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.matchIndexes[serverId] = matchIndex
	calculateCommitIndex()
}

type replicationState struct {
	serverId 		int				// id of the server
	currentTerm 	uint64			// current term
	nextIndex 		uint64			// index of the next log entry to send to the server
	commitment 		*commitment		// reference to the commitment in leaderstate
	notifyCh 		chan struct{}	// used by leader to signal replication 
}

type voteResult struct {
	serverId	int 				// peer's id
	ok 			bool				// whether the request was sent successfully
	reply 		*RequestVoteReply	// pointer to request response
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

// return a random duration between base and 2 * base
func randomTimeout(base time.Duration) time.Duration {
	timeout := time.Duration(rand.Int63()) % (2*base)
	return base + timeout
}

func (rf *Raft) quorumNeed() int {
	return len(rf.peers) / 2 + 1
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}

func (rf *Raft) sendApplyMsg() {
	commitIndex := rf.getCommitIndex()
	for i := rf.logStore.FirstIndex(); i <= commitIndex; i++ {
		var l Log 
		if err := rf.readLog(i, &l); err != nil {
			rf.logger.Fatalf("here %v", err)
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

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	
	if rf.killed() == true {
		return
	}
	if args.Term < reply.Term {
		// reject candidate with older term
		return
	}
	if args.Term > reply.Term {
		rf.persistCurrentTerm(args.Term)
		reply.Term = args.Term
	}
	if rf.lastVotedTerm == reply.Term {
		// already voted in this term, no vote left
		return 
	}
	if rf.lastLogTerm > args.LastLogTerm {
		// candidate is not as up-to-date as us
		return
	}
	if rf.lastLogTerm == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex {
		// candidate is not as up-to-date as us
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
		// reject Leader with older term
		return
	}
	if args.Term > reply.Term || rf.state != Follower {
		// receives newer term 
		// or we are currently not a follower
		rf.persistCurrentTerm(args.Term)
		rf.state = Follower 
		reply.Term = args.Term
	}
	rf.leader = args.LeaderId
	// verify previous log
	if args.PrevLogIndex > 0 {
		var term uint64
		if args.PrevLogIndex == rf.lastLogIndex {
			term = rf.lastLogTerm
		}else{
			var l Log
			if err := rf.readLog(args.PrevLogIndex, &l); err != nil {
				// missing logs
				rf.logger.Fatalf("Follower %v read log with index %v failed. Location=1", rf.me, args.PrevLogIndex)
				reply.LastIndex = rf.lastLogIndex
				return
			}
			term = l.Term
		}
		if args.PrevLogTerm != term {
			// inconsistent logs
			// find the first index of the conflicting term
			var l Log
			startIndex := args.PrevLogIndex
			for i:= args.PrevLogIndex; i>=rf.logStore.FirstIndex(); i-- {
				if err := rf.readLog(i, &l); err != nil {
					rf.logger.Fatalf("Follower %v read log with index %v failed. Location=2", rf.me, args.PrevLogIndex)
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
		var newLogs []*Log
		for i, entry := range args.Entries {
			if entry.Index > rf.lastLogIndex {
				newLogs = args.Entries[i:]
				break
			}

			var l Log
			if err := rf.readLog(entry.Index, &l); err != nil {
				rf.logger.Fatalf("Follower %v read log with index %v failed. Location=3", rf.me, args.PrevLogIndex)
				return
			}

			if l.Term != entry.Term {
				if err := rf.persistDeleteLogs(l.Index, rf.lastLogIndex); err != nil {
					rf.logger.Fatalf("Follower %v delete logs with start index %v and end index %v failed", rf.me, l.Index, rf.lastLogIndex)
					return
				}
				newLogs = args.Entries[i:]
				break
			}
		}
		
		if l := len(newLogs); l > 0 {
			if err := rf.persistWriteLogs(newLogs); err != nil {
				rf.logger.Printf("Follower %v write logs failed", rf.me)
				return
			}
			rf.lastLogIndex = newLogs[l-1].Index
			rf.lastLogTerm = newLogs[l-1].Term
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		// update commit index
		rf.commitIndex = min(rf.lastLogIndex, args.LeaderCommit)
		go func(){
			// notify service
			rf.sendApplyMsg()
		}()
	}

	reply.Success = true
	rf.lastContact = time.Now()
	return
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = -1
	term = -1
	// Your code here (2B).
	isLeader = (rf.me == rf.leader)
	if isLeader == false {
		return
	}
	newlog := &Log{
		Index: rf.lastLogIndex + 1,
		Term: rf.currentTerm,
		Data: command,
	}
	// persist the new log
	rf.persistWriteLogs([]*Log{newlog})
	rf.lastLogIndex = newlog.Index
	rf.lastLogTerm = newlog.Term
	// update commitment
	rf.leaderState.commitment.updateMatchIndex(rf.me, newlog.Index)
	// notify replication thread
	for _, peer := range rf.leaderState.replicationState {
		go func(r *replicationState){
			select {
			case r.notifyCh <- struct{}{}:
			default:
			}
		}(peer)
	}
	index = int(newlog.Index)
	term = int(newlog.Term)
	return index, term, isLeader
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

// Leader logic when the node is Leader
func (rf *Raft) runLeader() {
	rf.logger.Printf("Server %v entering Leader state", rf.me)
	rf.mu.Lock()
	rf.initializeLeaderState()
	rf.runAppendEntries()
	rf.mu.Unlock()
	defer rf.cleanupLeaderState()

	for rf.getState() == Leader && rf.killed() == false {
		select {
		case <- rf.leaderState.commitCh:
			commitIndex := rf.leaderState.commitment.getCommitIndex()
			rf.mu.Lock()
			rf.commitIndex = commitIndex
			rf.mu.Unlock()
			go func() {
				// notify tester
				rf.sendApplyMsg()
			}()
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) cleanupLeaderState(){
	rf.lastContact = time.Now()
	rf.leaderState.replicationState = nil
	rf.leaderState.commitment = nil
	rf.leaderState.commitCh = nil
} 

func (rf *Raft) heartbeat(serverId int, term uint64){
	for rf.killed() == false {
		var args AppendEntriesArgs
		var reply AppendEntriesReply
		args.Term = term
		args.LeaderId = rf.me
		time.Sleep(HeartbeatTimeout)
		if ok := rf.sendAppendEntries(serverId, &args, &reply); ok == false {
			time.Sleep(FailWait)
			continue
		}
		if reply.Success == false {
			rf.mu.Lock()
			rf.persistCurrentTerm(reply.Term)
			rf.leader = -1
			rf.state = Follower
			rf.mu.Unlock()
			return
		}
	}
}

func (rf *Raft) replicate(repl *replicationState) {
	timeout := time.Tick(ReplicationTimeout)
	for rf.killed() == false {
		var args AppendEntriesArgs
		var reply AppendEntriesReply
		var err error
		select {
		case <- repl.notifyCh:
		case <-timeout:
		}
		lastLogIndex := rf.getLastLogIndex()
		if err = rf.setupAppendEntriesArgs(repl, &args, lastLogIndex); err == ErrLogNotFound {
			goto SEND_SNAPSHOT	
		}else if err != nil {
			rf.logger.Fatalf("%v", err)
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
			repl.nextIndex = reply.LastIndex
		}else{
			repl.commitment.updateCommitIndex(repl.serverId, lastLogIndex)
			repl.nextIndex = lastLogIndex + 1
		}
		continue

		SEND_SNAPSHOT:
			rf.logger.Fatalf("no log found")
	}
}

func (rf *Raft) setupAppendEntriesArgs(r *replicationState, args *AppendEntriesArgs, endIndex uint64) (err error){
	args.LeaderId = rf.me 
	args.Term = r.currentTerm
	rf.mu.Lock()
	args.LeaderCommit = rf.commitIndex
	rf.mu.Unlock()
	if err := rf.makePrev(args, r.nextIndex); err != nil {
		return err 
	}
	if err := rf.makeLogs(args, r.nextIndex, endIndex); err != nil {
		return err
	}
	return nil
}

func (rf *Raft) makePrev(args *AppendEntriesArgs, nextIndex uint64) error {
	snapshotIndex, snapshotTerm := rf.getLastSnapshot()
	prevIndex := nextIndex - 1
	if prevIndex == 0 {
		args.PrevLogIndex = 0
		args.PrevLogTerm = 0
	}else if prevIndex == snapshotIndex {
		args.PrevLogIndex = snapshotIndex
		args.PrevLogTerm = snapshotTerm
	}else {
		var l Log
		if err := rf.readLog(prevIndex, &l); err != nil {
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


func (rf *Raft) initializeLeaderState() {
	rf.leaderState.replicationState = make(map[int]*replicationState)
	rf.leaderState.commitCh = make(chan struct{})
	rf.leaderState.commitment = &commitment{
		matchIndexes: make(map[int]uint64),
		commitIndex: rf.commitIndex,
		startIndex: rf.lastLogIndex,
		commitCh: rf.leaderState.commitCh,
	}
	for i := range rf.peers {
		rf.leaderState.commitment.matchIndexes[i] = 0
	}
}

// runAppendEntries starts threads to periodically send heartbeat and new logs to peer
func (rf *Raft) runAppendEntries() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		repl := &replicationState{
			serverId: peer,
			currentTerm: rf.currentTerm,
			nextIndex: rf.lastLogIndex+1,
			commitment: rf.leaderState.commitment,
			notifyCh: make(chan struct{}),
		}
		rf.leaderState.replicationState[peer] = repl
		go rf.heartbeat(peer, repl.currentTerm)
		go rf.replicate(repl)
	}
}

// Candidate logic when the node is Candidate
func (rf *Raft) runCandidate() {
	rf.logger.Printf("Server %v entering Candidate state", rf.me)
	rf.mu.Lock()
	rf.persistCurrentTerm(rf.currentTerm+1)
	newTerm := rf.currentTerm
	votesCh := rf.electSelf()
	rf.mu.Unlock()

	voteCount := 0 
	voteNeed := rf.quorumNeed()
	timeout := time.After(ElectionTimeout)
	for rf.killed() == false && rf.getState() == Candidate {
		select {
		case <- timeout:
			rf.logger.Printf("Candidate %v timeout", rf.me)
			return
		case vote := <-votesCh:
			if vote.ok == false {
				rf.logger.Printf("Candidate %v has lost connection to %v", rf.me, vote.serverId)
				continue
			}
			if vote.reply.Term > newTerm {
				rf.logger.Printf("Candidate %v has received a newer term %v from %v", rf.me, vote.reply.Term, vote.serverId)
				rf.mu.Lock()
				rf.persistCurrentTerm(vote.reply.Term)
				rf.state = Follower
				rf.mu.Unlock()
				return
			}
			if vote.reply.VoteGranted == true {
				rf.logger.Printf("Candidate %v has received a vote from %v", rf.me, vote.serverId)
				voteCount++
			}
			if voteCount >= voteNeed {
				rf.logger.Printf("Candidate %v has won the election", rf.me)
				rf.mu.Lock()
				rf.leader = rf.me
				rf.state = Leader
				rf.mu.Unlock()
				return
		 	}
		}
	}
}

func (rf *Raft) electSelf() <-chan *voteResult {
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
	rf.persistVote(args.Term, args.CandidateId)
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
	return votesCh
}

func (rf *Raft) runFollower() {
	rf.logger.Printf("Server %v entering Follower state", rf.me)
	for rf.killed() == false {
		timeout := randomTimeout(ElectionTimeout)
		time.Sleep(timeout)
		lastContact := rf.getLastContact()
		if lastContact.Add(timeout).After(time.Now()){
			continue
		}
		rf.logger.Printf("Server %v lost leader's heartbeat", rf.me)
		// transition to Candidate state
		rf.mu.Lock()
		rf.leader = -1
		rf.state = Candidate
		rf.mu.Unlock()
		return
	}
}

// long-running function to run different subroutine based on the state of the raft node
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
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
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
