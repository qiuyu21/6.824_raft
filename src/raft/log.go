package raft

import "sync"

type LogStore interface {
	FirstIndex() uint64
	LastIndex() uint64
	GetLog(index uint64, log *Log) error
	StoreLog(log *Log) error
	StoreLogs(logs []*Log) error
	DeleteRange(min, max uint64) error
}

type Log struct {
	Index 	uint64
	Term 	uint64
	Data 	interface{}
}

// InmemLogStore implements LogStore interface, and is intended for testing
type InmemLogStore struct {
	sync.RWMutex
	logs		map[uint64]*Log
	highIndex 	uint64 
	lowIndex 	uint64
}

func newInmemLogStore() *InmemLogStore {
	return &InmemLogStore{
		logs: make(map[uint64]*Log),
	}
}

// *WARNING*: don't hold the s.mu lock, calling function should have hold the lock
func (s *InmemLogStore) FirstIndex() uint64 {
	s.RLock()
	defer s.RUnlock()
	return s.lowIndex
}

// *WARNING*: don't hold the s.mu lock, calling function should have hold the lock
func (s *InmemLogStore) LastIndex() uint64 {
	s.RLock()
	defer s.RUnlock()
	return s.highIndex
}

// *WARNING*: don't hold the s.mu lock, calling function should have hold the lock
func (s *InmemLogStore) GetLog(index uint64, log *Log) error {
	// s.RLock()
	// defer s.RUnlock()
	l, ok := s.logs[index];
	if ok == false {
		return ErrLogNotFound
	}
	*log = *l
	return nil
}

// *WARNING*: don't hold the s.mu lock, calling function should have hold the lock
func (s *InmemLogStore) StoreLog(log *Log) error {
	return s.StoreLogs([]*Log{log})
}

// *WARNING*: don't hold the s.mu lock, calling function should have hold the lock
func (s *InmemLogStore) StoreLogs(logs []*Log) error {
	// s.Lock()
	// defer s.Unlock()
	for _, l := range logs {
		s.logs[l.Index] = l 
		if s.lowIndex == 0 {
			s.lowIndex = l.Index
		}
		if s.highIndex < l.Index {
			s.highIndex = l.Index
		}
	}
	return nil
}

// *WARNING*: don't hold the s.mu lock, calling function should have hold the lock
func (s *InmemLogStore) DeleteRange(min, max uint64) error {
	// s.Lock()
	// defer s.Unlock()
	for i := min; i <= max; i++ {
		delete(s.logs, i)
	}
	if min <= s.lowIndex {
		s.lowIndex = max + 1
	}
	if max >= s.highIndex {
		s.lowIndex = min - 1
	}
	if s.lowIndex > s.highIndex {
		s.lowIndex = 0
		s.highIndex = 0
	}
	return nil
}