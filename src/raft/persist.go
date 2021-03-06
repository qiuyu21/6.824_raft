package raft

import (
	"bytes"

	"6.824/labgob"
)

type PersistState struct {
	CurrentTerm 	uint64
	LastVotedTerm 	uint64 
	LastVotedFor 	int 
	LogStore 		*PersistLog
}

type PersistSnapshot struct {
	Snapshot 			[]byte
	SnapshotLastIndex	uint64
	SnapshotLastTerm	uint64
}

type PersistLog struct {
	Logs 		map[uint64]*Log		
	LowIndex 	uint64				
	HighIndex 	uint64 	
}

func NewPersistLog() *PersistLog {
	i := new(PersistLog)
	i.Logs = make(map[uint64]*Log)
	i.LowIndex = 0
	i.HighIndex = 0
	return i
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// *WARNING*: don't hold the rf.mu lock, calling function should have hold the lock
func (rf *Raft) persist(pState *PersistState, pSnapshot *PersistSnapshot) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(pState)
	state := w.Bytes()

	w = new(bytes.Buffer)
	e = labgob.NewEncoder(w)
	e.Encode(pSnapshot)
	snapshot := w.Bytes()

	rf.persister.SaveStateAndSnapshot(state, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readState(pState *PersistState)  error {
	data := rf.persister.ReadRaftState()
	if data == nil || len(data) < 1 {
		pState.CurrentTerm = 0
		pState.LastVotedFor = -1
		pState.LastVotedTerm = 0
		pState.LogStore = nil
		return nil
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(pState); err != nil {
		return err
	}
	return nil
}

func (rf *Raft) readSnapshot(pSnapshot *PersistSnapshot) error {
	data := rf.persister.ReadSnapshot()
	if data == nil || len(data) < 1 {
		pSnapshot.Snapshot = nil
		pSnapshot.SnapshotLastIndex = 0
		pSnapshot.SnapshotLastTerm = 0
		return nil
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(pSnapshot); err != nil {
		return err
	}
	return nil
}

func (rf *Raft) makePersistStateArgs() (pState *PersistState, pSnapshot *PersistSnapshot){
	pState = new(PersistState)
	pSnapshot = new(PersistSnapshot)
	pState.CurrentTerm = rf.currentTerm
	pState.LastVotedTerm = rf.lastVotedTerm
	pState.LastVotedFor = rf.lastVotedFor
	pState.LogStore = NewPersistLog()
	pSnapshot.Snapshot = rf.snapshot.snapshot
	pSnapshot.SnapshotLastIndex = rf.snapshotLastIndex
	pSnapshot.SnapshotLastTerm = rf.snapshotLastTerm
	return
}

func (rf *Raft) persistCurrentTerm(term uint64) {
	pState, pSnap :=  rf.makePersistStateArgs()
	pState.CurrentTerm = term
	rf.persist(pState, pSnap)
	rf.currentTerm = term
}

func (rf *Raft) persistVote(term uint64, candidateId int){
	pState, pSnap :=  rf.makePersistStateArgs()
	pState.LastVotedTerm = term
	pState.LastVotedFor = candidateId
	rf.persist(pState, pSnap)
	rf.lastVotedFor = candidateId
	rf.lastVotedTerm = term
}

func (rf *Raft) persistWriteLogs(newlogs []*Log) error {
	pState, pSnap :=  rf.makePersistStateArgs()
	//
	logCopy := rf.logStore.copyOfStore()
	logCopy.StoreLogs(newlogs)
	pState.LogStore.LowIndex = logCopy.LowIndex
	pState.LogStore.HighIndex = logCopy.HighIndex
	pState.LogStore.Logs = logCopy.Logs
	//
	rf.persist(pState, pSnap)
	rf.logStore.StoreLogs(newlogs)
	return nil
}

func (rf *Raft) persistDeleteLogs(start, end uint64) error {
	pState, pSnap :=  rf.makePersistStateArgs()
	//
	logCopy := rf.logStore.copyOfStore()
	logCopy.DeleteLogs(start,end)
	pState.LogStore.LowIndex = logCopy.LowIndex
	pState.LogStore.HighIndex = logCopy.HighIndex
	pState.LogStore.Logs = logCopy.Logs
	//
	rf.persist(pState, pSnap)
	rf.logStore.DeleteLogs(start, end)
	return nil
}
