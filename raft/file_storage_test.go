package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func makeConfig() *Config {
	cfg := Config{StorageType: "stable",
		RpcType:              "emulated",
		Verbosity:            VerbosityDebug,
		HeartbeatTimeout:     DefaultHeartbeatTimeout,
		ElectionTimeoutLower: DefaultElectionTimeoutLower,
		ElectionTimeoutUpper: DefaultElectionTimeoutUpper,
		SnapshotChunkSize:    DefaultSnapshotChunkSize,
		SegmentFileSize:      DefaultSegmentFileSize,
		StableStateFilename:  DefaultStableStateFilename}
	cfg.WalDir = "wal"
	cfg.StableStateFilename = "stable.state"
	cfg.SegmentFileSize = 1024 * 10
	cfg.T = log.New(ioutil.Discard, "", log.Ltime|log.Lshortfile|log.Lmicroseconds)
	cfg.D = log.New(os.Stdout, "", log.Ltime|log.Lshortfile|log.Lmicroseconds)
	cfg.I = log.New(os.Stdout, "", log.Ltime|log.Lshortfile|log.Lmicroseconds)
	cfg.W = log.New(os.Stdout, "", log.Ltime|log.Lshortfile|log.Lmicroseconds)
	cfg.E = log.New(os.Stderr, "", log.Ltime|log.Lshortfile|log.Lmicroseconds)
	return &cfg
}
func cleanup(fs *FileStorage) {
	if fs != nil {
		fs.Cleanup()
	}
}

func TestFSStoreEntries(t *testing.T) {
	fs, err := NewFileStorage(makeConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup(fs)
	err = fs.StoreEntries([]LogEntry{{0, 0, nil}})
	if err != ErrInvalidEntry {
		t.Fatalf("expected ErrInvalidEntry, got %v", err)
	}
	exp := generateEntries(1000)
	err = fs.StoreEntries(exp)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	ents, err := fs.Entries(1, 1000)
	if err != nil {
		t.Fatalf("expected nil error, got %#v", err)
	}
	if !SameEntrySlice(ents, exp) {
		t.Fatalf("expected entries %v, got %v", exp, ents)
	}
}

func TestFSTruncate(t *testing.T) {
	fs, err := NewFileStorage(makeConfig())
	defer cleanup(fs)
	if err != nil {
		t.Fatal(err)
	}
	exp := generateEntries(1000)
	err = fs.StoreEntries(exp)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if fs.Count() != 1000 {
		t.Fatalf("expected 1000 items, got %v", fs.Count())
	}
	ents, err := fs.Entries(1, 1000)
	if err != nil {
		t.Fatalf("expected nil error, got %#v", err)
	}
	if !SameEntrySlice(ents, exp) {
		t.Fatalf("expected entries %v, got %v", exp, ents)
	}

	err = fs.Truncate(501)
	if err != nil {
		t.Fatalf("expected nil error, got %#v", err)
	}
}
func TestFSEntries(t *testing.T) {

	segSizes := []int64{5 * 1024, 10 * 1024, 20 * 1024}
	sizes := []uint64{1, 5, 128, 512, 1024, 1024 * 5}

	for j := 0; j < len(segSizes); j++ {
		fs, err := NewFileStorage(makeConfig())
		if err != nil {
			t.Fatal(err)
		}
		fs.cfg.SegmentFileSize = segSizes[j]
		for i := 0; i < len(sizes); i++ {
			fmt.Printf("Test: FSEntries segment size %d, %d entries...\n", segSizes[j], sizes[i])

			err = fs.StoreEntries([]LogEntry{{0, 0, nil}})
			if err != ErrInvalidEntry {
				t.Fatalf("expected ErrInvalidEntry, got %v", err)
			}

			exp := generateEntries(int(sizes[i]))
			err = fs.StoreEntries(exp)
			if err != nil {
				t.Fatalf("expected nil error, got %v", err)
			}
			ents, err := fs.Entries(1, sizes[i]+1)
			if err != ErrOOB || ents != nil {
				t.Fatalf("expected ErrOOB error and nil entries, got %#v and %v", err, ents)
			}

			ents, err = fs.Entries(1, sizes[i])
			if err != nil {
				t.Fatalf("expected nil error, got %v", err)
			}
			if !SameEntrySlice(exp, ents) {
				t.Fatalf("expected entries %v, got %v", exp, ents)
			}

			rn := rand.Uint64() % sizes[i]
			exp = generateEntries(int(sizes[i] - rn))
			err = fs.StoreEntries(exp)
			if err != nil {
				t.Fatalf("expected nil error, got %v", err)
			}
			ents, err = fs.Entries(1, fs.LastIndex())
			if err != nil {
				t.Fatalf("expected nil error, got %v", err)
			}
			if !SameEntrySlice(exp, ents) {
				t.Fatalf("expected entries %v, got %v", exp, ents)
			}

			term, err := fs.Term(1)
			if err != nil || term != 1 {
				t.Fatalf("expected nil error and term 1, got %#v and %v", err, term)
			}

			term, err = fs.Term(sizes[i] + 1)
			if err != ErrOOB {
				t.Fatalf("expected ErrOOB error, got %#v", err)
			}
			fmt.Printf("  ... Passed\n")
		}
		cleanup(fs)
	}

}

func TestFSSnapshot(t *testing.T) {
	fmt.Printf("Test: FSSnapshot...\n")
	fs, err := NewFileStorage(makeConfig())
	fs.cfg.SegmentFileSize = 128
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup(fs)
	err = fs.StoreEntries([]LogEntry{{0, 0, nil}})
	if err != ErrInvalidEntry {
		t.Fatalf("expected ErrInvalidEntry, got %v", err)
	}
	exp := generateEntries(50)
	err = fs.StoreEntries(exp)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	log.Printf("before saving snapshot %v", fs)
	s := Snapshot{SnapshotMetadata{1, 48}, nil}
	err = fs.SaveSnapshot(s)
	log.Printf("done saving snapshot %v", fs)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if i := fs.SnapshotLastIndex(); i != 48 {
		t.Fatalf("expect SnapshotLastIndex to be 48, got %d", i)
	}
	if term := fs.SnapshotLastTerm(); term != 1 {
		t.Fatalf("expect SnapshotLastTerm to be 1, got %d", term)
	}
	//log.Printf("here1 %v", fs)
	ents, err := fs.Entries(1, 3)
	if err != ErrCompacted || ents != nil {
		t.Fatalf("expected ErrOOB error and nil entries, got %#v and %v", err, ents)
	}

	ents, err = fs.Entries(49, 50)
	if err != nil {
		fmt.Print(fs.segs)
		t.Fatalf("expected nil error, got %#v and %v", err, ents)
	}
	exp = exp[48:50]
	if !SameEntrySlice(ents, exp) {
		t.Fatalf("expected entries %v, got %v", exp, ents)
	}
	//log.Printf("here2 %v", fs)
	s = Snapshot{SnapshotMetadata{1, 47}, nil}
	err = fs.SaveSnapshot(s)

	if err != ErrStaleSnapshot {
		t.Fatalf("expected ErrOOB error, got %#v", err)
	}
	_, err = fs.Term(1)
	if err != ErrCompacted {
		t.Fatalf("expected ErrCompacted error, got %#v", err)
	}

	// store entries that overlap with the snapshot
	exp = generateEntries(55)
	err = fs.StoreEntries(exp)
	//log.Printf("here3 %v", fs)
	if err != nil {
		t.Fatalf("expected nil error, got %#v", err)
	}
	ents, err = fs.Entries(49, 56)
	if err != ErrOOB {
		t.Fatalf("expected ErrOOB error, got %#v", err)
	}
	ents, err = fs.Entries(49, 55)
	exp = exp[48:]
	if err != nil || !SameEntrySlice(ents, exp) {
		t.Fatalf("expected entries %v, got %v, %#v", exp, ents, err)
	}

	fi := fs.FirstIndex()
	li := fs.LastIndex()
	if fi != 49 {
		t.Fatalf("expected first index = 3, got %v", fi)
	}
	if li != 55 {
		t.Fatalf("expected last index = 3, got %v", li)
	}

	lastEntry, err := fs.LastEntry()
	if err != nil || lastEntry.Index != 55 || lastEntry.Term != 1 {
		t.Fatalf("expected nil error, got %#v, %v", err, lastEntry)
	}

	s = Snapshot{SnapshotMetadata{1, 55}, nil}
	err = fs.SaveSnapshot(s)

	if err != nil {
		t.Fatalf("expected nil error, got %#v", err)
	}

	lastEntry, err = fs.LastEntry()
	if err != ErrEmptyLog {
		t.Fatalf("expected ErrEmptyLog error, got %#v, %v", err, lastEntry)
	}

	fs.Close()

	fs, err = NewFileStorage(makeConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup(fs)
	exp = generateEntriesAt(10, 56)
	err = fs.StoreEntries(exp)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	ents, err = fs.Entries(56, 65)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !SameEntrySlice(ents, exp) {
		t.Fatalf("expected entries %v, got %v", exp, ents)
	}
	fmt.Printf("   Passed...\n")
}

func generateEntriesAt(n, startIdx int) []LogEntry {
	ents := make([]LogEntry, 0, n)
	for i := 0; i < n; i++ {
		ents = append(ents, LogEntry{1, uint64(i + startIdx), []byte(randstring(16))})
	}
	return ents
}
func generateEntries(n int) []LogEntry {
	return generateEntriesAt(n, 1)
}
func TestFSReadWal(t *testing.T) {
	segSizes := []int64{65536 / 2, 65536, 65536 * 2}
	sizes := []int{rand.Int() % 10000, rand.Int() % 10000, rand.Int() % 10000}
	for i := 0; i < len(segSizes); i++ {
		for j := 0; j < len(sizes); j++ {
			fmt.Printf("Test: FSReadWal segment size %d, %d entries...\n", segSizes[i], sizes[j])
			cfg := makeConfig()
			cfg.SegmentFileSize = segSizes[i]
			fs, err := NewFileStorage(cfg)
			size := sizes[j]

			if err != nil {
				t.Fatal(err)
			}

			exp := generateEntries(size)
			err = fs.StoreEntriesAndPersist(exp)
			fmt.Printf("here1\n")
			if err != nil {
				t.Fatalf("expected nil error, got %v", err)
			}
			ents, err := fs.Entries(1, uint64(size))
			fmt.Printf("here2\n")
			if err != nil || !SameEntrySlice(ents, exp) {
				t.Fatalf("expected entries %v, got %v, %#v", exp, ents, err)
			}
			fs.Close()

			fs, err = NewFileStorage(cfg)
			if err != nil {
				t.Fatalf("failed to reopen wal %v", err)
			}
			ents, err = fs.Entries(1, uint64(size))
			fmt.Printf("here3\n")
			if err != nil || !SameEntrySlice(ents, exp) {
				t.Fatalf("expected entries %v, got %v, %#v", exp, ents, err)
			}

			exp = generateEntries(50)
			err = fs.StoreEntries(exp)
			if err != nil {
				t.Fatalf("expected nil error, got %v", err)
			}
			if fs.Count() != 50 {
				t.Fatalf("expected 50 items, got %v", fs.Count())
			}
			ents, err = fs.Entries(1, 50)
			if err != nil || !SameEntrySlice(ents, exp) {
				t.Fatalf("expected entries %v, got %v, %#v", exp, ents, err)
			}
			fs.Close()

			fs, err = NewFileStorage(cfg)
			if err != nil {
				t.Fatalf("failed to reopen wal %v", err)
			}
			ents, err = fs.Entries(1, 100)
			if err != ErrOOB {
				t.Fatalf("expected ErrOOB, got %v, %#v", ents, err)
			}

			ents, err = fs.Entries(1, 50)
			if err != nil || !SameEntrySlice(ents, exp) {
				t.Fatalf("expected entries %v, got %v, %#v", exp, ents, err)
			}
			cleanup(fs)
			fmt.Printf("  ... Passed\n")

			fmt.Printf("Test: FSReadWal incrementally appending, segment size %d, %d entries...\n", segSizes[i], sizes[j])
			fs, err = NewFileStorage(cfg)
			if err != nil {
				t.Fatalf("failed to reopen wal %v", err)
			}
			exp = generateEntries(size)
			n := uint64(rand.Int() % 500)
			for i := uint64(0); i < uint64(len(exp)); i++ {
				if fs == nil {
					fs, err = NewFileStorage(cfg)
					if err != nil {
						t.Fatalf("failed to reopen wal %v", err)
					}
				}
				err = fs.StoreEntries(exp[i : i+1])
				if err != nil {
					t.Fatalf("expected nil error, got %v", err)
				}
				ents, err = fs.Entries(1, i+1)
				if err != nil || !SameEntrySlice(ents, exp[0:i+1]) {
					t.Fatalf("expected entries %v, got %v, %#v", exp[0:i+1], ents, err)
				}
				if i == n {
					fs.Close()
					fs = nil
					n = uint64(rand.Int()%500) + i
				}
			}
			cleanup(fs)
			fmt.Printf("  ... Passed\n")
		}
	}

}

func TestFSSnapshotAndWalMixed(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	segSizes := []int64{rand.Int63() % 65536 / 2, rand.Int63() % 65536, rand.Int63() % 65536 * 2}
	sizes := []int{rand.Int() % 10000, rand.Int() % 10000, rand.Int() % 10000, rand.Int() % 10000}

	for i := 0; i < len(segSizes); i++ {
		for j := 0; j < len(sizes); j++ {
			fmt.Printf("Test: incrementally appending mixed with snapshoting, segment size %d, %d entries...\n", segSizes[i], sizes[j])
			fs, err := NewFileStorage(makeConfig())
			if err != nil {
				t.Fatalf("failed to reopen wal %v", err)
			}
			var expSnapshot Snapshot
			size := sizes[j]
			snapshotThresh := rand.Int()%128 + 1
			cfg := makeConfig()
			cfg.SegmentFileSize = segSizes[i]
			n := uint64(rand.Int() % 128)
			exp := generateEntries(size)
			for k := uint64(0); k < uint64(len(exp)); k++ {
				if fs == nil {
					fs, err = NewFileStorage(cfg)
					if err != nil {
						t.Fatalf("failed to reopen wal %v", err)
					}
					if !SameSnapshot(expSnapshot, fs.Snapshot()) {
						t.Fatalf("expected snapshot %v, got %v", expSnapshot, fs.Snapshot())
					}
				}
				err = fs.StoreEntries(exp[k : k+1])
				if err != nil {
					t.Fatalf("expected nil error, got %v", err)
				}
				if fs.Count() >= snapshotThresh {
					buf := new(bytes.Buffer)
					encoder := gob.NewEncoder(buf)
					encoder.Encode(exp[0 : k+1])
					expSnapshot = Snapshot{SnapshotMetadata{1, k + 1}, buf.Bytes()}
					err = fs.SaveSnapshot(expSnapshot)
					if err != nil {
						t.Fatalf("expected nil error, got %v", err)
					}
					if !SameSnapshot(expSnapshot, fs.Snapshot()) {
						t.Fatalf("expected snapshot %v, got %v", expSnapshot, fs.Snapshot())
					}
					//next threshold
					snapshotThresh = rand.Int()%128 + 1
				}
				if fs.SnapshotLastIndex() <= k {
					ents, err := fs.Entries(fs.FirstIndex(), fs.LastIndex())
					if err != nil || !SameEntrySlice(ents, exp[fs.FirstIndex()-1:k+1]) {
						t.Fatalf("expected range [%d-%d], entries %v, got [%d-%d] %v, %#v", expSnapshot.Metadata.LastIdx+1, i+1, exp[fs.FirstIndex()-1:i+1], fs.FirstIndex(), fs.LastIndex(), ents, err)
					}
				}
				if k == n && k != uint64(len(exp))-1 {
					fs.Close()
					fs = nil
					n = uint64(rand.Int()%128) + k
				}
			}
			cleanup(fs)
			fmt.Printf("  ... Passed\n")
		}
	}

}
func TestFSStableState(t *testing.T) {
	fs, err := NewFileStorage(makeConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup(fs)
	ss := fs.StableState()
	if ss.currentTerm != 0 || ss.votedFor != 0 {
		t.Fatalf("expect stable state (0, 0), got (%d, %d)", ss.currentTerm, ss.votedFor)
	}

	ss.currentTerm = 1
	ss.votedFor = 5
	err = fs.StoreStableStateAndPersist(ss)
	if err != nil {
		t.Fatalf("expect nil error, got %v", err)
	}

	ss = fs.StableState()
	if ss.currentTerm != 1 || ss.votedFor != 5 {
		t.Fatalf("expect stable state (1, 5), got (%d, %d)", ss.currentTerm, ss.votedFor)
	}

	fs.Close()

	fs, err = NewFileStorage(makeConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup(fs)
	ss = fs.StableState()
	if ss.currentTerm != 1 || ss.votedFor != 5 {
		t.Fatalf("expect stable state (1, 5), got (%d, %d)", ss.currentTerm, ss.votedFor)
	}
}
