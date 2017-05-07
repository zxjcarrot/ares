package raft

import "testing"

func TestMSStoreEntries(t *testing.T) {
	ms := NewMemoryStorage(nil)
	err := ms.StoreEntries([]LogEntry{{0, 0, nil}})
	if err != ErrInvalidEntry {
		t.Fatalf("expected ErrInvalidEntry, got %v", err)
	}
	exp := []LogEntry{{1, 1, nil}, {1, 2, nil}, {1, 3, nil}}
	err = ms.StoreEntries(exp)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	ents, err := ms.Entries(1, 3)
	if err != nil {
		t.Fatalf("expected nil error, got %#v", err)
	}
	if !SameEntrySlice(ents, exp) {
		t.Fatalf("expected entries %v, got %v", exp, ents)
	}
}
func TestMSEntries(t *testing.T) {
	ms := NewMemoryStorage(nil)
	err := ms.StoreEntries([]LogEntry{{0, 0, nil}})
	if err != ErrInvalidEntry {
		t.Fatalf("expected ErrInvalidEntry, got %v", err)
	}
	exp := []LogEntry{{1, 1, nil}, {1, 2, nil}, {1, 3, nil}}
	err = ms.StoreEntries(exp)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	ents, err := ms.Entries(1, 4)
	if err != ErrOOB || ents != nil {
		t.Fatalf("expected ErrOOB error and nil entries, got %#v and %v", err, ents)
	}

	term, err := ms.Term(1)
	if err != nil || term != 1 {
		t.Fatalf("expected nil error and term 1, got %#v and %v", err, term)
	}

	term, err = ms.Term(5)
	if err != ErrOOB {
		t.Fatalf("expected ErrOOB error, got %#v", err)
	}
}

func TestMSSnapshot(t *testing.T) {
	ms := NewMemoryStorage(nil)
	err := ms.StoreEntries([]LogEntry{{0, 0, nil}})
	if err != ErrInvalidEntry {
		t.Fatalf("expected ErrInvalidEntry, got %v", err)
	}
	exp := []LogEntry{{1, 1, nil}, {1, 2, nil}, {1, 3, nil}, {1, 4, nil}}
	err = ms.StoreEntries(exp)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	s := Snapshot{SnapshotMetadata{1, 2}, nil}
	err = ms.SaveSnapshot(s)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	ents, err := ms.Entries(1, 3)
	if err != ErrCompacted || ents != nil {
		t.Fatalf("expected ErrOOB error and nil entries, got %#v and %v", err, ents)
	}

	ents, err = ms.Entries(3, 3)
	if err != nil {
		t.Fatalf("expected nil error, got %#v and %v", err, ents)
	}
	exp = []LogEntry{{1, 3, nil}}
	if !SameEntrySlice(ents, exp) {
		t.Fatalf("expected entries %v, got %v", exp, ents)
	}

	s = Snapshot{SnapshotMetadata{1, 2}, nil}
	err = ms.SaveSnapshot(s)

	if err != ErrStaleSnapshot {
		t.Fatalf("expected ErrOOB error, got %#v", err)
	}
	_, err = ms.Term(1)
	if err != ErrCompacted {
		t.Fatalf("expected ErrCompacted error, got %#v", err)
	}

	// store entries that overlap with the snapshot
	err = ms.StoreEntries([]LogEntry{{1, 1, nil}, {1, 2, nil}, {1, 3, []byte("data")}})
	if err != nil {
		t.Fatalf("expected nil error, got %#v", err)
	}
	ents, err = ms.Entries(3, 4)
	if err != ErrOOB {
		t.Fatalf("expected ErrOOB error, got %#v", err)
	}
	ents, err = ms.Entries(3, 3)
	exp = []LogEntry{{1, 3, []byte("data")}}
	if err != nil || !SameEntrySlice(ents, exp) {
		t.Fatalf("expected entries %v, got %v, %#v", exp, ents, err)
	}

	fi := ms.FirstIndex()
	li := ms.LastIndex()
	if fi != 3 {
		t.Fatalf("expected first index = 3, got %v", fi)
	}
	if li != 3 {
		t.Fatalf("expected last index = 3, got %v", li)
	}

	lastEntry, err := ms.LastEntry()
	if err != nil || lastEntry.Index != 3 || lastEntry.Term != 1 {
		t.Fatalf("expected nil error, got %#v, %v", err, lastEntry)
	}

	s = Snapshot{SnapshotMetadata{1, 3}, nil}
	err = ms.SaveSnapshot(s)

	if err != nil {
		t.Fatalf("expected nil error, got %#v", err)
	}

	lastEntry, err = ms.LastEntry()
	if err == nil || SameEntry(lastEntry, dummyEntry) == false {
		t.Fatalf("expected ErrEmptyLog error, got %#v, %v", err, lastEntry)
	}
}

func TestMSLogSize(t *testing.T) {
	ms := NewMemoryStorage(nil)
	err := ms.StoreEntries([]LogEntry{{0, 0, nil}})
	if err != ErrInvalidEntry {
		t.Fatalf("expected ErrInvalidEntry, got %v", err)
	}
	//normal appending
	exp := []LogEntry{{1, 1, []byte{1, 2, 3}}, {1, 2, []byte{1, 2, 3}}, {1, 3, []byte{1, 2, 3}}}
	err = ms.StoreEntries(exp)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if ms.LogSize() != 16*3+3*3 {
		t.Fatalf("expected log size %v, got %v", 16*3+3*3, ms.LogSize())
	}

	// snapshotting
	s := Snapshot{SnapshotMetadata{1, 2}, nil}
	err = ms.SaveSnapshot(s)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if ms.LogSize() != 16+3 {
		t.Fatalf("expected log size %v, got %v", 16+3, ms.LogSize())
	}

	// skip entries already compacted & truncation
	exp = []LogEntry{{1, 1, []byte{1, 2, 3}}, {1, 2, []byte{1, 2, 3}}, {1, 3, []byte{1, 2, 3}}, {1, 4, []byte{1, 2, 3}}}
	err = ms.StoreEntries(exp)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if ms.LogSize() != 16*2+3*2 {
		t.Fatalf("expected log size %v, got %v", 16*2+3*2, ms.LogSize())
	}

	// truncate entries starting at index 3
	err = ms.Truncate(3)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if ms.LogSize() != 0 {
		t.Fatalf("expected log size %v, got %v", 0, ms.LogSize())
	}

}
