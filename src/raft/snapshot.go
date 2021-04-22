package raft

type snapshot struct {
	snapshot 			[]byte 			// (persistent)	latest snapshot
	snapshotLastIndex	uint64 			// (persistent)	latest snapshot last log's index 
	snapshotLastTerm	uint64			// (persistent) latest snapshot last log's term
	snapshotCh			chan struct{}	// (N/A) notify leader a new snapshot has been taken
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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