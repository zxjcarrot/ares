package raft

import "sync"

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) SaveRaftState(data []byte) {
	if ps == nil {
		return
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = data
}

func (ps *Persister) ReadRaftState() []byte {
	if ps == nil {
		return nil
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.raftstate
}

func (ps *Persister) RaftStateSize() uint64 {
	if ps == nil {
		return 0
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return uint64(len(ps.raftstate))
}

func (ps *Persister) SaveSnapshot(snapshot []byte) {
	if ps == nil {
		return
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.snapshot = snapshot
}

func (ps *Persister) ReadSnapshot() []byte {
	if ps == nil {
		return nil
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.snapshot
}
