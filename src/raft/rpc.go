package raft

type RequestVoteArgs struct {
	Term 			uint64
	CandidateId		int
	LastLogIndex	uint64
	LastLogTerm		uint64
}

type RequestVoteReply struct {
	Term			uint64
	VoteGranted		bool
}

type AppendEntriesArgs struct {
	Term 			uint64
	LeaderId		int
	LeaderCommit	uint64
	PrevLogIndex	uint64
	PrevLogTerm		uint64
	Entries 		[]*Log
}

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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}