package raft

import "errors"

var ErrInvalidEntry = errors.New("invalid log entry, index must start at 1")
var ErrOOB = errors.New("index out of bound")
var ErrEmptyLog = errors.New("log contains no entry")
var ErrStaleSnapshot = errors.New("snapshot is older than the most recently saved one")
var ErrCompacted = errors.New("entries has been incorporated into the snapshot")
var ErrNotContinuous = errors.New("new entries must proceed the last entry")

type Storage interface {
	StableState() StableState
	// save stable state in the storage.
	StoreStableState(StableState) error
	StoreStableStateAndPersist(StableState) error
	PersistStableState() error
	// return a slice of entries with the index range of [low, high].
	// (nil, ErrOOB or ErrCompacted) will be returned if the range is out of bound.
	Entries(low, high uint64) ([]LogEntry, error)
	// stores and truncates entries in the log
	// This should first skip entries that overlap with the snapshot.
	// After that, ents will be store in the log at the index of ent[0].Index.
	// All the entries following ents will be truncated.
	StoreEntries(ents []LogEntry) error
	StoreEntriesAndPersist(ents []LogEntry) error
	// truncate the log starting at idx
	Truncate(idx uint64) error
	PersistAll() error
	PersistRange(low, high uint64) error
	// return the term of the entry at index i if the ith entry is in the log
	// otherwise ErrOOB is returned if i is out of bound or
	// ErrCompacted is returned if ith entry is compacted in the snapshot
	Term(i uint64) (uint64, error)
	// return the index of the first entry in the log if not empty
	// otherwise return the index of last included entry of the most recent snapshot
	FirstIndex() uint64
	// return the index of the last entry in the log if not empty
	// otherwise return the index of last included entry of the most recent snapshot
	LastIndex() uint64
	// return the last entry in the log if not empty
	// otherise return (dummyEntry, ErrEmptyLog)
	LastEntry() (LogEntry, error)
	// store the snapshot in the storage
	// return ErrStaleSnapshot if the snapshot is older than
	// the on already in the storage
	SaveSnapshot(Snapshot) error
	// return the latest snapshot in the storage
	Snapshot() Snapshot
	// return the index of the last included entry in the most recent snapshot
	SnapshotLastIndex() uint64
	// return the term of the last included entry in the most recent snapshot
	SnapshotLastTerm() uint64
	// return the number of entries in the log
	Count() int
	LogSize() uint64
	Close() error
	Cleanup() error
}
