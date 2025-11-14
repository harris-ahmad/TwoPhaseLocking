package storage

import (
	"fmt"
	"sync"
)

type MemoryStorage struct {
	mu sync.RWMutex
	data map[string][]byte
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		data: make(map[string][]byte),
	}
}

func (s *MemoryStorage) Read(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value,ok := s.data[key]
	if !ok{
		return nil, fmt.Errorf("key %s not found", key)
	}
	result := make([]byte, len(value))
	copy(result, value)

	return result, nil
}

func (s *MemoryStorage) Write(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	stored := make([]byte, len(value))
	copy(stored,value)
	s.data[key]=stored
	return nil
}

func (s *MemoryStorage) GetAll() map[string][]byte {
	s.mu.RLock()
	defer s.mu.RLock()

	snapshot := make(map[string][]byte, len(s.data))
	for k, v := range s.data {
		copied:= make([]byte, len(v))
		copy(copied,v)
		snapshot[k]=copied
	}
	return snapshot
}

func (s *MemoryStorage) Clear(){
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data=make(map[string][]byte)
}