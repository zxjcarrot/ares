package raft

type LogEntry struct {
	Term  uint64
	Index uint64
	Data  []byte
}

var dummyEntry LogEntry = LogEntry{
	0, 0, nil,
}

type StableState struct {
	currentTerm uint64
	votedFor    uint64
}

type SnapshotMetadata struct {
	LastTerm uint64
	LastIdx  uint64
}

type Snapshot struct {
	Metadata SnapshotMetadata
	Data     []byte
}

type raftLog struct {
	s   Storage
	cfg *Config
}

func EntrySize(e LogEntry) int64 {
	return int64(len(e.Data) + 16)
}
func EntrySizeOnDisk(e LogEntry) int64 {
	return 8 + EntrySize(e) // header size + term + index + data
}
func newRaftLog(storage Storage, cfg *Config) *raftLog {
	if storage == nil {
		cfg.E.Fatalf("storage must not be nil")
	}
	return &raftLog{storage, cfg}
}

// Test whether the log contains an entry with idx and term
// If not, returns true and the first index at that term or
// the index of the last log entry negated if the entry isn't in the log at all.
// Otherwise, returns false and 0.
func (rl *raftLog) conflictingEntryUnlocked(idx, term uint64) (bool, int64, uint64) {
	if idx == 0 || idx <= rl.s.SnapshotLastIndex() {
		return false, 0, 0
	}
	var lastTerm uint64
	var lastIndex int64
	lastEntry, err := rl.s.LastEntry()
	if err == ErrEmptyLog {
		lastTerm = rl.s.SnapshotLastTerm()
		lastIndex = int64(rl.s.SnapshotLastIndex())
	} else {
		lastTerm = lastEntry.Term
		lastIndex = int64(lastEntry.Index)
	}
	ents, err := rl.s.Entries(idx, idx)
	if err == ErrOOB || err == ErrCompacted {
		rl.cfg.D.Printf("no entry found at index %d, error %v", idx, err)
		return true, -lastIndex, lastTerm
	} else if ents[0].Index != idx {
		rl.cfg.E.Fatalf("log slice contains an entry at %v but with a different entry index %d, snapshotLastIdx %d",
			idx, ents[0].Index, rl.s.SnapshotLastIndex())
		return true, 0, 0
	} else if ents[0].Index == idx && ents[0].Term != term {
		theTerm := ents[0].Term
		firstIndex := idx
		for ; firstIndex >= rl.s.FirstIndex(); firstIndex-- {
			ents, err = rl.s.Entries(firstIndex, firstIndex)
			if err != nil {
				rl.cfg.E.Fatalf("rl.s.Entries(%v, %v) returned unexpected error %v", firstIndex, firstIndex, err)
			}
			if ents[0].Term != theTerm {
				break
			}
		}
		firstIndex++
		ents, err = rl.s.Entries(firstIndex, firstIndex)
		rl.cfg.W.Printf("found an log entry with index %d but with a different term %d!=%d, first index at that term %d is %d",
			idx, ents[0].Term, term, ents[0].Term, firstIndex)
		return true, int64(firstIndex), theTerm
	}
	return false, 0, 0
}
