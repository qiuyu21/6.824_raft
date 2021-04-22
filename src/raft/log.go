package raft

import (
	"reflect"
	"sync"
)

type Log struct {
	Index	uint64
	Term	uint64
	Data 	interface{}
}

type InmemLogStore struct {
	mu sync.RWMutex
	Logs 		map[uint64]*Log		
	LowIndex 	uint64				
	HighIndex 	uint64 				
}

func (s *InmemLogStore) FirstIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.LowIndex
}

func (s *InmemLogStore) LastIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.HighIndex
}

func NewInmemLogStore() *InmemLogStore {
	i := new(InmemLogStore)
	i.Logs = make(map[uint64]*Log)
	i.LowIndex = 0
	i.HighIndex = 0
	return i
}

func (s *InmemLogStore) GetLog(index uint64, log *Log) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	l, ok := s.Logs[index]
	if ok == false {
		return ErrLogNotFound
	}
	*log = *l
	return nil
}

func (s *InmemLogStore) StoreLog(log *Log) error {
	return s.StoreLogs([]*Log{log})
}

func (s *InmemLogStore) StoreLogs(logs []*Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range logs {
		s.Logs[l.Index] = l 
		if s.LowIndex == 0 {
			s.LowIndex = l.Index
		}
		if s.HighIndex < l.Index {
			s.HighIndex = l.Index
		}
	}
	return nil
}

func (s *InmemLogStore) DeleteLogs(min, max uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := min; i <= max; i++ {
		delete(s.Logs, i)
	}
	if min <= s.LowIndex {
		s.LowIndex = max + 1
	}
	if max >= s.HighIndex {
		s.HighIndex = min - 1
	}
	if s.LowIndex > s.HighIndex {
		s.LowIndex = 0
		s.HighIndex = 0
	}
	return nil
}

func copyValue(l *Log) interface{} {
	return reflect.ValueOf(l.Data).Interface()
}

// copyOfStore makes a deep copy of InmemLogstore
func (i *InmemLogStore) copyOfStore() *InmemLogStore {
	i.mu.RLock()
	defer i.mu.RUnlock()
	storeCopy := new(InmemLogStore)
	logcopy := make(map[uint64]*Log)
	for _, log := range i.Logs {
		l := new(Log)
		*l = *log
		l.Data = copyValue(l)
		logcopy[l.Index] = l
	}
	storeCopy.Logs = logcopy
	storeCopy.LowIndex = i.LowIndex
	storeCopy.HighIndex = i.HighIndex
	return storeCopy
}