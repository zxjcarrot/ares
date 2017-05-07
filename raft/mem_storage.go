package raft

import "log"

type MemoryStorage struct {
	entries []LogEntry
	ss      StableState
	img     Snapshot
	cfg     *Config
	size    uint64
}

func NewMemoryStorage(cfg *Config) *MemoryStorage {
	return &MemoryStorage{
		[]LogEntry{},
		StableState{0, Unvoted},
		Snapshot{SnapshotMetadata{0, 0}, nil},
		cfg,
		0,
	}
}

func (ms *MemoryStorage) StableState() StableState {
	return ms.ss
}

func (ms *MemoryStorage) StoreStableState(ss StableState) error {
	ms.ss = ss
	return nil
}

func (ms *MemoryStorage) StoreStableStateAndPersist(ss StableState) error {
	ms.ss = ss
	return ms.PersistStableState()
}

func (ms *MemoryStorage) PersistStableState() error {
	return nil
}

func (ms *MemoryStorage) entrySliceSize(entries []LogEntry) uint64 {
	size := uint64(0)
	for i := range entries {
		size += uint64(16 + len(entries[i].Data))
	}
	return size
}
func (ms *MemoryStorage) Entries(low, high uint64) ([]LogEntry, error) {
	if high < low || low > ms.LastIndex() {
		return nil, ErrOOB
	}
	if len(ms.entries) == 0 {
		if ms.img.Metadata.LastIdx >= low {
			return nil, ErrCompacted
		}
		return nil, ErrOOB
	}
	if low < ms.entries[0].Index {
		return nil, ErrCompacted
	} else if ms.entries[len(ms.entries)-1].Index < high {
		return nil, ErrOOB
	}
	n := high - low + 1
	fi := ms.FirstIndex()
	return ms.entries[low-fi : low-fi+n], nil
}
func (ms *MemoryStorage) StoreEntriesAndPersist(ents []LogEntry) error {
	if err := ms.StoreEntries(ents); err != nil {
		return err
	}
	return ms.PersistAll()
}
func (ms *MemoryStorage) PersistAll() error {
	return ms.PersistRange(ms.FirstIndex(), ms.LastIndex())
}
func (ms *MemoryStorage) PersistRange(low, high uint64) error {
	return nil
}

func (ms *MemoryStorage) StoreEntries(ents []LogEntry) error {
	if len(ents) == 0 {
		return nil
	}
	if ents[0].Index < 1 {
		return ErrInvalidEntry
	}
	if ms.LastIndex()+1 < ents[0].Index {
		// there is gap between the last entry and the first entry in ents
		return ErrNotContinuous
	}
	// skip entries that have been already incorporated into the snapshot
	for len(ents) > 0 && ms.img.Metadata.LastIdx >= ents[0].Index {
		if len(ents) > 1 {
			ents = ents[1:]
		} else {
			ents = nil
		}
	}
	if len(ms.entries) > 0 && len(ents) > 0 {
		idx := ents[0].Index - ms.FirstIndex()
		if idx < uint64(len(ms.entries)) {
			// truncate from ents[0].Index
			ms.size -= ms.entrySliceSize(ms.entries[idx:])
			ms.entries = ms.entries[0:idx]
		}
	}
	if len(ents) > 0 {
		ms.size += ms.entrySliceSize(ents)
		ms.entries = append(ms.entries, ents...)
	}
	return nil
}
func (ms *MemoryStorage) Truncate(idx uint64) error {
	fi := ms.FirstIndex()
	if idx <= ms.SnapshotLastIndex() {
		return ErrCompacted
	} else if idx > ms.LastIndex() {
		return ErrOOB
	}
	ms.size -= ms.entrySliceSize(ms.entries[idx-fi:])
	ms.entries = ms.entries[:idx-fi]
	return nil
}
func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	ents, err := ms.Entries(i, i)
	if err != nil {
		return 0, err
	}
	return ents[0].Term, nil
}

func (ms *MemoryStorage) SnapshotLastIndex() uint64 {
	return ms.img.Metadata.LastIdx
}
func (ms *MemoryStorage) SnapshotLastTerm() uint64 {
	return ms.img.Metadata.LastTerm
}
func (ms *MemoryStorage) FirstIndex() uint64 {
	if len(ms.entries) == 0 {
		return ms.img.Metadata.LastIdx
	}
	return ms.entries[0].Index
}

func (ms *MemoryStorage) LastIndex() uint64 {
	if len(ms.entries) == 0 {
		return ms.img.Metadata.LastIdx
	}
	return ms.entries[len(ms.entries)-1].Index
}
func (ms *MemoryStorage) LastEntry() (LogEntry, error) {
	if len(ms.entries) == 0 {
		return dummyEntry, ErrEmptyLog
	}
	return ms.entries[len(ms.entries)-1], nil
}
func (ms *MemoryStorage) SetLoggers(T, I, W, E *log.Logger) {

}
func (ms *MemoryStorage) SaveSnapshot(img Snapshot) error {
	if ms.img.Metadata.LastIdx >= img.Metadata.LastIdx {
		return ErrStaleSnapshot
	}
	ms.img = img
	if img.Metadata.LastIdx >= ms.LastIndex() {
		ms.entries = nil
	} else {
		fi := ms.FirstIndex()
		ms.size -= ms.entrySliceSize(ms.entries[0 : img.Metadata.LastIdx-fi+1])
		tmp := ms.entries[img.Metadata.LastIdx-fi+1:]
		ms.entries = nil // make the old entries garbage collectable
		ms.entries = append(ms.entries, tmp...)
		tmp = nil
	}
	return nil
}
func (ms *MemoryStorage) Snapshot() Snapshot {
	return ms.img
}
func (ms *MemoryStorage) Count() int {
	return len(ms.entries)
}

func (ms *MemoryStorage) Close() error {
	return nil
}

func (ms *MemoryStorage) Cleanup() error {
	ms.entries = nil
	return nil
}

func (ms *MemoryStorage) LogSize() uint64 {
	return ms.size
}
