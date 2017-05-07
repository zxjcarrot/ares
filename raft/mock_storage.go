package raft

import (
	"log"
	"os"
	"runtime/pprof"
)

type MockStorage struct {
	ms  *MemoryStorage
	fs  *FileStorage
	cfg *Config
}

func NewMockStorage(cfg *Config) *MockStorage {
	fs, err := NewFileStorage(cfg)
	if err != nil {
		panic(err)
	}
	return &MockStorage{
		NewMemoryStorage(cfg),
		fs,
		cfg,
	}
}

func (ms *MockStorage) StableState() StableState {
	ss1 := ms.ms.StableState()
	ss2 := ms.fs.StableState()
	if ss1 != ss2 {
		ms.cfg.E.Panicf("StableState(), expected %v, got %v", ss1, ss2)
	}
	return ss1
}

func (ms *MockStorage) StoreStableState(ss StableState) error {
	err1 := ms.ms.StoreStableState(ss)
	err2 := ms.fs.StoreStableState(ss)
	if err1 != err2 {
		ms.cfg.E.Panicf("StoreStableState(%v), expected %v, got %v", ss, err1, err2)
	}
	return err1
}

func (ms *MockStorage) StoreStableStateAndPersist(ss StableState) error {
	err1 := ms.ms.StoreStableStateAndPersist(ss)
	err2 := ms.fs.StoreStableStateAndPersist(ss)
	if err1 != err2 {
		ms.cfg.E.Panicf("StoreStableStateAndPersist(%v), expected %v, got %v", ss, err1, err2)
	}
	return err1
}

func (ms *MockStorage) PersistStableState() error {
	err1 := ms.ms.PersistStableState()
	err2 := ms.fs.PersistStableState()
	if err1 != err2 {
		ms.cfg.E.Panicf("PersistStableState(), expected %v, got %v", err1, err2)
	}
	return err1
}

func (ms *MockStorage) Entries(low, high uint64) ([]LogEntry, error) {
	ents1, err1 := ms.ms.Entries(low, high)
	ents2, err2 := ms.fs.Entries(low, high)
	if err1 != err2 {
		ms.cfg.E.Panicf("Entries(%d,%d), expected %v, got %v", low, high, err1, err2)
	}
	if !SameEntrySlice(ents1, ents2) {
		ms.cfg.E.Panicf("expected %v, got %v", ents1, ents2)
	}
	return ents1, err1
}
func (ms *MockStorage) StoreEntriesAndPersist(ents []LogEntry) error {
	err1 := ms.ms.StoreEntriesAndPersist(ents)
	err2 := ms.fs.StoreEntriesAndPersist(ents)
	if err1 != err2 {
		ms.cfg.E.Panicf("expected %v, got %v", err1, err2)
	}
	return err1
}
func (ms *MockStorage) PersistEntries() error {
	err1 := ms.ms.PersistAll()
	err2 := ms.fs.PersistAll()
	if err1 != err2 {
		ms.cfg.E.Panicf("expected %v, got %v", err1, err2)
	}
	return err1
}

func (ms *MockStorage) StoreEntries(ents []LogEntry) error {
	err1 := ms.ms.StoreEntries(ents)
	err2 := ms.fs.StoreEntries(ents)
	if err1 != err2 {
		ms.cfg.E.Panicf("StoreEntries(%v), expected %v, got %v", ents, err1, err2)
	}
	return err1
}
func (ms *MockStorage) Truncate(idx uint64) error {
	err1 := ms.ms.Truncate(idx)
	err2 := ms.fs.Truncate(idx)
	if err1 != err2 {
		ms.cfg.E.Panicf("Truncate(%d), expected %v, got %v", idx, err1, err2)
	}
	return err1
}
func (ms *MockStorage) Term(i uint64) (uint64, error) {
	t1, e1 := ms.ms.Term(i)
	t2, e2 := ms.fs.Term(i)
	if t1 != t2 || e1 != e2 {
		ms.cfg.E.Panicf("Term(%d), expected (%v,%v), got (%v,%v)", i, t1, e1, t2, e2)
	}
	return t1, e1
}

func (ms *MockStorage) SnapshotLastIndex() uint64 {
	i1 := ms.ms.SnapshotLastIndex()
	i2 := ms.fs.SnapshotLastIndex()
	if i1 != i2 {
		ms.cfg.E.Panicf("SnapshotLastIndex(), expected %v, got %v", i1, i2)
	}
	return i1
}
func (ms *MockStorage) SnapshotLastTerm() uint64 {
	t1 := ms.ms.SnapshotLastTerm()
	t2 := ms.fs.SnapshotLastTerm()
	if t1 != t2 {
		ms.cfg.E.Panicf("SnapshotLastIndex(), expected %v, got %v", t1, t2)
	}
	return t1
}
func (ms *MockStorage) FirstIndex() uint64 {
	i1 := ms.ms.FirstIndex()
	i2 := ms.fs.FirstIndex()
	if i1 != i2 {
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		ms.cfg.E.Panicf("FirstIndex(), expected %v, got %v", i1, i2)
	}
	return i1
}

func (ms *MockStorage) LastIndex() uint64 {
	i1 := ms.ms.LastIndex()
	i2 := ms.fs.LastIndex()
	if i1 != i2 {
		ms.cfg.E.Panicf("FirstIndex(), expected %v, got %v", i1, i2)
	}
	return i1
}
func (ms *MockStorage) LastEntry() (LogEntry, error) {
	ent1, err1 := ms.ms.LastEntry()
	ent2, err2 := ms.fs.LastEntry()
	if !SameEntry(ent1, ent2) || err1 != err2 {
		ms.cfg.E.Panicf("LastEntry(), expected (%v,%v), got (%v,%v)", ent1, err1, ent2, err2)
	}
	return ent1, err1
}
func (ms *MockStorage) SetLoggers(T, I, W, E *log.Logger) {

}
func (ms *MockStorage) SaveSnapshot(img Snapshot) error {
	err1 := ms.ms.SaveSnapshot(img)
	err2 := ms.fs.SaveSnapshot(img)
	if err1 != err2 {
		ms.cfg.E.Panicf("SaveSnapshot(%v), expected %v, got %v", img, err1, err2)
	}
	return err1
}
func (ms *MockStorage) Snapshot() Snapshot {
	s1 := ms.ms.Snapshot()
	s2 := ms.fs.Snapshot()
	if SameSnapshot(s1, s2) {
		ms.cfg.E.Panicf("Snapshot(), expected %v, got %v", s1, s2)
	}
	return s1
}
func (ms *MockStorage) Count() int {
	c1 := ms.ms.Count()
	c2 := ms.fs.Count()
	if c1 != c2 {
		ms.cfg.E.Panicf("Count(), expected %v, got %v", c1, c2)
	}
	return c1
}

func (ms *MockStorage) Close() error {
	e1 := ms.ms.Close()
	e2 := ms.fs.Close()
	if e1 != e2 {
		ms.cfg.E.Panicf("Close(), expected %v, got %v", e1, e2)
	}
	return e1
}

func (ms *MockStorage) Cleanup() error {
	e1 := ms.ms.Cleanup()
	e2 := ms.fs.Cleanup()
	if e1 != e2 {
		ms.cfg.E.Panicf("Cleanup(), expected %v, got %v", e1, e2)
	}
	return e1
}
func (ms *MockStorage) LogSize() uint64 {
	return ms.ms.LogSize()
}
