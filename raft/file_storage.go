package raft

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

var ErrMissingLastSeg = errors.New("missing last segment!")
var ErrNoSnapshotFile = errors.New("no snapshot file found")
var ErrEntriesLoaded = errors.New("entries in segment already in memory")
var ErrEmptySeg = errors.New("segment contains no entry")
var ErrFileClosed = errors.New("file closed")

// The log is cut into segments of size SegmentFileSize.
// The current segment is named last.seg.
// When the size of current segment exceeds SegmentFileSize,
// a segment named as entIdx.seg is created.
// For example:
// seg1: 100.seg (containes entries from 1 - 100)
// seg2: 210.seg (contains entries from 101-210)
// seg3: last.seg (contains entries from 211-lastIndex).
type segment struct {
	file     *os.File
	startIdx uint64     // first index of the raft entry in the segment
	endIdx   uint64     // last index of the raft entry in the segment
	ents     []LogEntry // cache the entire segment of entries in memory
	offs     []int64    // offsets of the entries within the file
	reseek   bool
	cfg      *Config
	writer   *bufio.Writer
}

func handleErrorCommon(err error, info string, cfg *Config) error {
	pc, fn, line, _ := runtime.Caller(2)
	_, filename := filepath.Split(fn)
	msg := fmt.Sprintf("error in %s:%s:%d => %v, %s", filename, runtime.FuncForPC(pc).Name(), line, err, info)
	cfg.W.Printf(msg)
	return errors.New(msg)
}

func (s *segment) handleError(err error, info string) error {
	return handleErrorCommon(err, info, s.cfg)
}

func (s segment) String() string {
	name := "closed!!!"
	if s.file != nil {
		name = s.file.Name()
	}
	return fmt.Sprintf("(%s, [%d-%d])", name, s.startIdx, s.endIdx)
}

func (s *segment) count() uint64 {
	if s.endIdx >= s.startIdx {
		return s.endIdx - s.startIdx + 1
	}
	return 0
}
func (s *segment) empty() bool {
	return s.endIdx == s.startIdx-1
}
func (s *segment) fillSegment() error {
	//log.Println("reading entries for", s, "from ", s.file.Name())
	if len(s.ents) > 0 {
		return ErrEntriesLoaded
	}
	_, err := s.file.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}
	s.ents = make([]LogEntry, 0, 1024)

	reader := bufio.NewReader(s.file)

	// Since we need mantain offsets within segment file and
	// encoding.gob can't efficiently give the size of the serialized data in advance,
	// binary package is used here.
	var offset int64
	for {
		var size int64
		err = binary.Read(reader, binary.LittleEndian, &size)
		if err != nil {
			if err == io.EOF {
				break
			}
			s.handleError(err, fmt.Sprintf("%s, reading entry size", *s))
			panic(err)
		}

		// allocate a fix-sized slice for binary.Reader
		entry := LogEntry{0, 0, make([]byte, size-16)}
		err := binary.Read(reader, binary.LittleEndian, &entry.Term)
		if err != nil {
			s.handleError(err, fmt.Sprintf("%s, binary.Reader(reader, binary.LittleEndian, &entry.Term)", *s))
			return err
		}
		err = binary.Read(reader, binary.LittleEndian, &entry.Index)
		if err != nil {
			s.handleError(err, fmt.Sprintf("%s, binary.Reader(reader, binary.LittleEndian, &entry.Index)", *s))
			return err
		}
		err = binary.Read(reader, binary.LittleEndian, entry.Data)
		if err != nil {
			s.handleError(err, fmt.Sprintf("%s, binary.Reader(reader, binary.LittleEndian, entry.Data)", *s))
			return err
		}
		//log.Printf("read size: %d, entry : %v", size, entry)
		s.ents = append(s.ents, entry)
		s.offs = append(s.offs, offset)
		offset += size + 8
	}
	if len(s.ents) > 0 {
		s.startIdx = s.ents[0].Index
		s.endIdx = s.ents[len(s.ents)-1].Index
	}
	//	log.Printf("%v", s.ents)
	s.reseek = false
	return nil
}

func (s *segment) writeEntries(ents []LogEntry) error {
	oldEndIdx := s.endIdx
	low := ents[0].Index
	var off int64
	if s.empty() == false && low < s.startIdx {
		return ErrOOB
	} else if s.empty() == false && low > s.endIdx+1 {
		return ErrNotContinuous
	} else {
		if s.empty() {
			off = 0
			s.startIdx = ents[0].Index
			s.endIdx = ents[0].Index
			s.reseek = true
		} else if low <= s.endIdx {
			off = s.offs[low-s.startIdx]
			s.ents = s.ents[:low-s.startIdx]
			s.offs = s.offs[:low-s.startIdx]
			s.reseek = true
		} else {
			off = s.lastOffset() + EntrySizeOnDisk(s.ents[len(s.ents)-1])
			/*
				fi, err := s.file.Stat()
				if err != nil {
					s.handleError(err, fmt.Sprintf("%s, s.file.Stat()", *s))
					return err
				}
				off = fi.Size()
			*/
		}
	}
	if s.reseek {
		s.cfg.D.Printf("%v reseeking to %v", s, off)
		newOff, err := s.file.Seek(off, os.SEEK_SET)
		if err != nil {
			s.handleError(err, fmt.Sprintf("%s, s.file.Seek(off, os.SEEK_SET)", *s))
			return err
		}
		off = newOff
	}
	writer := s.writer
	writer.Reset(s.file)
	for _, entry := range ents {
		s.ents = append(s.ents, entry)
		s.offs = append(s.offs, off)
		s.endIdx = entry.Index
		size := EntrySize(entry)
		//fs.cfg.I.Printf("write size: %d", size)
		err := binary.Write(writer, binary.LittleEndian, size)
		if err != nil {
			s.handleError(err, fmt.Sprintf("%s, binary.Write(writer, binary.LittleEndian, %d)", *s, size))
			return err
		}
		err = binary.Write(writer, binary.LittleEndian, entry.Term)
		if err != nil {
			s.handleError(err, fmt.Sprintf("%s, binary.Write(writer, binary.LittleEndian, %d)", *s, entry.Term))
			return err
		}
		err = binary.Write(writer, binary.LittleEndian, entry.Index)
		if err != nil {
			s.handleError(err, fmt.Sprintf("%s, binary.Write(writer, binary.LittleEndian, %d)", *s, entry.Index))
			return err
		}
		err = binary.Write(writer, binary.LittleEndian, entry.Data)
		if err != nil {
			s.handleError(err, fmt.Sprintf("%s, binary.Write(writer, binary.LittleEndian, %d bytes)", *s, len(entry.Data)))
			return err
		}
		off += EntrySizeOnDisk(entry)
	}
	err := writer.Flush()
	if err != nil {
		s.handleError(err, fmt.Sprintf("%s, writer.Flush()", s))
		return err
	}
	s.reseek = false
	if s.endIdx <= oldEndIdx {
		err = s.file.Truncate(off)
		if err != nil {
			s.handleError(err, fmt.Sprintf("%s,  s.file.Truncate(%d)", *s, off))
			return err
		}
	}

	return err
}

// seal the segment and rename segment to idx.seg
func (s *segment) fix(snapshotIdx, idx uint64) error {
	if err := s.truncate(snapshotIdx, idx+1); err != nil {
		return err
	}
	dir, _ := filepath.Split(s.file.Name())
	filename := filepath.Join(dir, fmt.Sprintf("%016X.seg", idx))
	err := os.Rename(s.file.Name(), filename)
	if err != nil {
		s.handleError(err, fmt.Sprintf("%s,  os.Rename(%s, %s)", *s, s.file.Name(), filename))
		return err
	}
	s.cfg.I.Printf("%v, fix() renamed segment file %s to %s", *s, s.file.Name(), filename)
	tmpfile := s.file
	defer tmpfile.Close()
	s.file, err = os.OpenFile(filename, os.O_RDWR, 0660)
	if err != nil {
		s.handleError(err, fmt.Sprintf("%s,  os.OpenFile(%s, os.O_RDWR, 0660)", *s, filename))
	}
	s.reseek = true
	return err
}

// remove entries starting at idx in segment
func (s *segment) truncate(snapshotIdx, idx uint64) error {
	s.cfg.I.Printf("seg %v truncating at idx %d", s, idx)
	if idx < s.startIdx {
		return ErrOOB
	} else if idx > s.endIdx {
		return nil
	}
	if len(s.ents) == 0 {
		s.fillSegment()
	}
	off := s.offs[idx-s.startIdx]
	s.offs = s.offs[:idx-s.startIdx]
	s.ents = s.ents[:idx-s.startIdx]
	s.endIdx = idx - 1
	if len(s.ents) == 0 {
		s.startIdx = s.endIdx + 1
	}
	s.cfg.I.Printf("seg after truncation => %v", s)
	s.reseek = true
	return s.file.Truncate(off)
}

// rename the segment to last.seg
func (s *segment) fixToLast() error {
	if s.file == nil {
		s.handleError(ErrFileClosed, fmt.Sprintf("%v", *s))
		return ErrFileClosed
	}
	dir, _ := filepath.Split(s.file.Name())
	newFilename := filepath.Join(dir, "last.seg")
	if newFilename == s.file.Name() {
		return nil
	}
	s.cfg.I.Printf("fixToLast renaming %s => %s", s.file.Name(), newFilename)
	err := os.Rename(s.file.Name(), newFilename)
	if err != nil {
		s.handleError(err, fmt.Sprintf("%s, os.Rename(%s, %s)", *s, s.file.Name(), newFilename))
		return err
	}
	s.file.Close()
	s.file, err = os.OpenFile(newFilename, os.O_RDWR, 0660)
	if err != nil {
		s.handleError(err, fmt.Sprintf("%s,  os.OpenFile(%s, os.O_RDWR, 0660)", *s, newFilename))
	}
	s.reseek = true
	return err
}

// remove entries ending at idx in segment
func (s *segment) truncateBegin(idx uint64, last bool) error {
	s.cfg.I.Printf("segment %v, truncateBegin at idx %d", s, idx)
	if idx < s.startIdx {
		return ErrOOB
	}

	if s.empty() {
		err := s.fillSegment()
		if err != nil {
			return err
		}
		s.cfg.I.Printf("after reading in entries => %v", s)
		if s.empty() {
			s.startIdx = idx + 1
			s.endIdx = idx
			return s.fixToLast()
		}
	}
	s.reseek = true
	if idx < s.endIdx {
		tmpfile, err := ioutil.TempFile("", "seg")
		if err != nil {
			s.handleError(err, fmt.Sprintf("%s, ioutil.TempFile(\"\", \"seg\")", *s))
			return err
		}

		_, err = s.file.Seek(s.offs[idx-s.startIdx+1], os.SEEK_SET)
		if err != nil {
			return err
		}
		_, err = io.Copy(tmpfile, s.file)
		if err != nil {
			return err
		}
		tmpfile.Close()
		s.file.Close()
		newFilename := s.file.Name()
		s.file = nil
		err = os.Rename(tmpfile.Name(), newFilename)
		if err != nil {
			s.handleError(err, fmt.Sprintf("%s, os.Rename(%s, %s)", *s, tmpfile.Name(), newFilename))
			return err
		}
		s.file, err = os.OpenFile(newFilename, os.O_RDWR, 0660)
		if err != nil {
			s.handleError(err, fmt.Sprintf("%s, os.OpenFile(%s, os.O_RDWR, 0660)", *s, newFilename))
			return err
		}
		startOffset := s.offs[idx-s.startIdx+1]
		s.offs = s.offs[idx-s.startIdx+1:]
		for i := range s.offs {
			s.offs[i] -= startOffset
		}
		s.ents = s.ents[idx-s.startIdx+1:]
		s.startIdx = idx + 1
		s.cfg.I.Printf("after truncate at begining => segment %v\n", s)
		return nil
	} else {
		s.cfg.I.Printf("idx %d, truncated out the whole segment[%d-%d]", idx, s.startIdx, s.endIdx)
		s.startIdx = idx + 1
		s.endIdx = idx
		s.offs = nil
		s.ents = nil
		err := s.file.Truncate(0)
		if err != nil {
			s.handleError(err, fmt.Sprintf("%s, s.file.Truncate(0)", s))
			return err
		}
		if last {
			return s.fixToLast()
		}
		return s.destroy()
	}
}

func (s *segment) destroy() error {
	//fs.cfg.I.Printf("destroying %v", s)
	s.endIdx = s.startIdx - 1
	s.offs = nil
	s.ents = nil
	if s.file != nil {
		s.file.Close()
		err := os.Remove(s.file.Name())
		if err != nil {
			s.handleError(err, fmt.Sprintf("%s, os.Remove(%s)", *s, s.file.Name()))
			return err
		}
		s.cfg.W.Printf("removed file %s", s.file.Name())
	}
	s.file = nil
	return nil
}

func (s *segment) close() error {
	s.offs = nil
	s.ents = nil
	s.cfg.I.Printf("closing %s", s.file.Name())
	err := s.file.Close()
	s.file = nil
	return err
}
func (s *segment) entries(low, high uint64) ([]LogEntry, error) {
	if low < s.startIdx || high < low || low > s.endIdx || high > s.endIdx {
		return nil, ErrOOB
	}
	if len(s.ents) == 0 {
		err := s.fillSegment()
		if err != nil {
			return nil, err
		}
	}
	if len(s.ents) <= 0 {
		s.handleError(ErrEmptyLog, fmt.Sprintf("%s, reading on empty segment", *s))
		return nil, ErrEmptySeg
	}
	n := high - low + 1
	return s.ents[low-s.startIdx : low-s.startIdx+n], nil
}

func (s *segment) lastOffset() int64 {
	if len(s.offs) > 0 {
		return s.offs[len(s.offs)-1]
	}
	return 0
}

func (s *segment) size() int64 {
	off := s.lastOffset()
	if off == 0 {
		st, err := s.file.Stat()
		if err != nil {
			return 0
		}
		return st.Size()
	}
	return off
}
func (s *segment) sync() error {
	if len(s.ents) == 0 {
		return nil
	}
	return s.file.Sync()
}

func (s *segment) cut(snapshotLastIdx uint64) (*segment, error) {
	offs := s.offs
	ents := s.ents
	idx := lowerBound(offs, s.cfg.SegmentFileSize)
	//fs.cfg.I.Printf("lowerBound(%v, %d) => %d", offs, fs.cfg.SegmentFileSize, idx)
	_, err := s.file.Seek(offs[idx], os.SEEK_SET)
	if err != nil {
		s.cfg.I.Printf("s.file.Seek(%d, os.SEEK_SET) => %v", offs[idx], err)
		return nil, err
	}
	tmpfile, err := ioutil.TempFile(s.cfg.WalDir, "last")
	if err != nil {
		s.cfg.I.Printf("ioutil.TempFile(%v, \"last\") => %v", s.cfg.WalDir, err)
		return nil, err
	}
	defer tmpfile.Close()
	_, err = io.Copy(tmpfile, s.file)
	if err != nil {
		s.cfg.I.Printf("io.Copy(tmpfile, s.file) => %v", err)
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	err = s.fix(snapshotLastIdx, uint64(idx)+s.startIdx-1)
	if err != nil {
		s.cfg.I.Printf("s.fix(%d, %d) => %v", snapshotLastIdx, uint64(idx)+s.startIdx-1, err)
		return nil, err
	}
	err = os.Rename(tmpfile.Name(), filepath.Join(s.cfg.WalDir, "last.seg"))
	if err != nil {
		s.cfg.I.Printf("os.Rename(%s, %s) => %v", tmpfile.Name(), filepath.Join(s.cfg.WalDir, "last.seg"), err)
		return nil, err
	}

	newLastFile, err := os.OpenFile(filepath.Join(s.cfg.WalDir, "last.seg"), os.O_RDWR, 0660)
	if err != nil {
		s.cfg.I.Printf("os.OpenFile(%s, os.O_RDWR, 0660) => %v", filepath.Join(s.cfg.WalDir, "last.seg"), err)
		return nil, err
	}
	newLast := &segment{newLastFile, 0, 0, nil, nil, true, s.cfg, bufio.NewWriterSize(newLastFile, 40960)}
	startOff := offs[idx]
	for ; idx < len(offs); idx++ {
		newLast.ents = append(newLast.ents, ents[idx])
		newLast.offs = append(newLast.offs, offs[idx]-startOff)
	}
	newLast.startIdx = newLast.ents[0].Index
	newLast.endIdx = newLast.ents[len(newLast.ents)-1].Index

	return newLast, nil
}

type FileStorage struct {
	segs []segment
	// Every snapshot is saved as "lastIdx.snapshot"
	// The snapshot with the largest lastIdx is the latest one.
	img Snapshot
	// imgFile points to the latest snapshot file
	imgFile *os.File
	// stable state is save in "stable.state" file at default
	ss     StableState
	ssFile *os.File
	cfg    *Config
	T      *log.Logger
	I      *log.Logger
	W      *log.Logger
	E      *log.Logger
}

// cut the last segment if its size exceeds threshold
func (fs *FileStorage) cut() error {
	//fs.cfg.I.Printf("%v", fs.segs)
	if fs.lastSegment().lastOffset() < fs.cfg.SegmentFileSize {
		return nil
	}
	newLast, err := fs.lastSegment().cut(fs.img.Metadata.LastIdx)
	if err != nil {
		return err
	}
	fs.segs = append(fs.segs, *newLast)
	// keep cutting until the last segment fits
	return fs.cut()
}
func (fs *FileStorage) handleError(err error, info string) error {
	return handleErrorCommon(err, info, fs.cfg)
}

func (fs *FileStorage) readSnapshotFile() error {
	if fs.imgFile == nil {
		return ErrNoSnapshotFile
	}
	decoder := gob.NewDecoder(fs.imgFile)
	return decoder.Decode(&fs.img)
}

func (fs *FileStorage) writeSnapshotFile() error {
	if fs.imgFile == nil {
		return ErrNoSnapshotFile
	}
	encoder := gob.NewEncoder(fs.imgFile)
	return encoder.Encode(fs.img)
}

func (fs *FileStorage) initSnapshot() error {
	files, err := ioutil.ReadDir(fs.cfg.WalDir)
	if err != nil {
		fs.handleError(err, fmt.Sprintf("ioutil.ReadDir(%s)", fs.cfg.WalDir))
		return err
	}
	segFilenames := []string{}
	maxLastIdx := 0
	latestSnapshtFilename := ""
	for _, fi := range files {
		if fi.IsDir() == false {
			parts := strings.Split(fi.Name(), ".")
			if len(parts) == 2 && parts[1] == "snapshot" {
				if lastIdx, err := strconv.Atoi(parts[0]); err == nil || parts[0] == "last" {
					filename := filepath.Join(fs.cfg.WalDir, fi.Name())
					segFilenames = append(segFilenames, filename)
					if maxLastIdx < lastIdx {
						latestSnapshtFilename = filename
						maxLastIdx = lastIdx
					}
				}
			}
		}
	}
	fs.cfg.I.Printf("latestSnapshtFilename: %s", latestSnapshtFilename)
	if latestSnapshtFilename == "" {
		return nil // no snapshot saved yet
	}

	imgFile, err := os.OpenFile(latestSnapshtFilename, os.O_RDWR, 0660)
	if err != nil {
		fs.handleError(err, fmt.Sprintf("os.OpenFile(%s, os.O_RDWR, 0660)", latestSnapshtFilename))
		return err
	}
	fs.imgFile = imgFile

	return fs.readSnapshotFile()
}

func (fs *FileStorage) readStableState() error {
	_, err := fs.ssFile.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}
	decoder := gob.NewDecoder(fs.ssFile)
	if err := decoder.Decode(&fs.ss.currentTerm); err != nil {
		return err
	}
	if err := decoder.Decode(&fs.ss.votedFor); err != nil {
		return err
	}
	return nil
}

func (fs *FileStorage) writeStableState() error {
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)
	encoder.Encode(fs.ss.currentTerm)
	encoder.Encode(fs.ss.votedFor)
	_, err := fs.ssFile.WriteAt(w.Bytes(), 0)
	return err
}
func (fs *FileStorage) initStableState() error {
	filename := filepath.Join(fs.cfg.WalDir, fs.cfg.StableStateFilename)
	ssFile, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		fs.handleError(err, fmt.Sprintf("os.OpenFile(%s, os.O_RDWR|os.O_CREATE, 0660)", filename))
		return err
	}

	st, err := ssFile.Stat()
	if err != nil {
		ssFile.Close()
		return err
	}
	fs.ssFile = ssFile
	if st.Size() == 0 { // write initial state to file
		return fs.writeStableState()
	}
	return fs.readStableState()
}

func (fs *FileStorage) initSegments() error {
	failed := false
	defer func() {
		if failed == true {
			for i := range fs.segs {
				if fs.segs[i].file != nil {
					fs.segs[i].file.Close()
				}
			}
		}
	}()
	files, err := ioutil.ReadDir(fs.cfg.WalDir)
	if err != nil {
		return err
	}
	segFilenames := []string{}
	hasLast := false
	for _, fi := range files {
		if fi.IsDir() == false {
			parts := strings.Split(fi.Name(), ".")
			if len(parts) == 2 && parts[1] == "seg" {
				if _, err := strconv.ParseInt(parts[0], 16, 64); err == nil || parts[0] == "last" {
					segFilenames = append(segFilenames, filepath.Join(fs.cfg.WalDir, fi.Name()))
					if parts[0] == "last" {
						hasLast = true
					}
				}
			}
		}
	}
	sort.Slice(segFilenames, func(i, j int) bool {
		return segFilenames[i] < segFilenames[j]
	})
	log.Println(segFilenames)
	if len(segFilenames) > 0 && hasLast == false {
		return ErrMissingLastSeg
	} else if len(segFilenames) == 0 {
		lastFilename := filepath.Join(fs.cfg.WalDir, "last.seg")
		lastSegFile, err := os.Create(lastFilename)
		if err != nil {
			return err
		}
		fs.segs = []segment{segment{lastSegFile, fs.img.Metadata.LastIdx + 1, fs.img.Metadata.LastIdx, nil, nil, true, fs.cfg, bufio.NewWriterSize(lastSegFile, 40960)}}
		return nil
	}
	lastIdx := fs.img.Metadata.LastIdx
	for _, filename := range segFilenames {
		file, err := os.OpenFile(filename, os.O_RDWR, 0660)
		if err != nil {
			failed = true

			return err
		}
		_, base := filepath.Split(filename)
		idx := strings.Split(base, ".")[0]
		endIdx := lastIdx
		if idx != "last" {
			idx, _ := strconv.ParseInt(idx, 16, 64)
			endIdx = uint64(idx)
		}
		fs.segs = append(fs.segs, segment{file, lastIdx + 1, uint64(endIdx), nil, nil, true, fs.cfg, bufio.NewWriterSize(file, 40960)})
		lastIdx = uint64(endIdx)
	}
	return fs.lastSegment().fillSegment()
}

func NewFileStorage(cfg *Config) (*FileStorage, error) {
	gob.Register(LogEntry{})

	if cfg.SegmentFileSize == 0 {
		cfg.SegmentFileSize = DefaultSegmentFileSize
	}
	fs := &FileStorage{
		nil,
		Snapshot{SnapshotMetadata{0, 0}, nil},
		nil,
		StableState{0, 0},
		nil,
		cfg, nil, nil, nil, nil,
	}
	err := os.Mkdir(cfg.WalDir, 0755)
	if err != nil && !os.IsExist(err) {
		fs.handleError(err, fmt.Sprintf("os.Mkdir(%s)", cfg.WalDir))
		return nil, err
	}
	err = fs.initStableState()
	if err != nil {
		return nil, err
	}
	err = fs.initSnapshot()
	if err != nil {
		return nil, err
	}
	err = fs.initSegments()
	if err != nil {
		return nil, err
	}
	return fs, nil
}
func (fs *FileStorage) StableState() StableState {
	return fs.ss
}

func (fs *FileStorage) StoreStableState(ss StableState) error {
	fs.ss = ss
	return nil
}

func (fs *FileStorage) StoreStableStateAndPersist(ss StableState) error {
	fs.StoreStableState(ss)
	return fs.PersistStableState()
}

func (fs *FileStorage) PersistStableState() error {
	if err := fs.writeStableState(); err != nil {
		return err
	}
	return fs.ssFile.Sync()
}

func (fs *FileStorage) lastSegment() *segment {
	if len(fs.segs) == 0 {
		return nil
	}
	return &fs.segs[len(fs.segs)-1]
}
func (fs *FileStorage) findSegIdx(idx uint64) int {
	if fs.lastSegment().empty() && idx >= fs.lastSegment().startIdx {
		return len(fs.segs) - 1
	}
	for i := len(fs.segs) - 1; i >= 0; i-- {
		if fs.segs[i].startIdx <= idx {
			return i
		}
	}
	return -1
}
func (fs *FileStorage) Entries(low, high uint64) (ents []LogEntry, err error) {
	if high < low || low > fs.LastIndex() || high > fs.LastIndex() {
		return nil, ErrOOB
	}
	if low < fs.FirstIndex() {
		return nil, ErrCompacted
	}

	firstSegIdx := fs.findSegIdx(low)
	lastSegIdx := fs.findSegIdx(high)
	if firstSegIdx == lastSegIdx { // hot path
		return fs.segs[firstSegIdx].entries(low, high)
	} else { // firstSegIdx < lastSegIdx
		pc, fn, line, _ := runtime.Caller(1)
		_, filename := filepath.Split(fn)
		msg := fmt.Sprintf("%s:%s:%d", filename, runtime.FuncForPC(pc).Name(), line)
		fs.cfg.I.Printf("Entries(%d-%d) from %s", low, high, msg)
		ents, err = fs.segs[firstSegIdx].entries(low, fs.segs[firstSegIdx].endIdx)
		for i := firstSegIdx + 1; i < lastSegIdx; i++ {
			ents2, err := fs.segs[i].entries(fs.segs[i].startIdx, fs.segs[i].endIdx)
			if err != nil {
				return nil, err
			}
			ents = append(ents, ents2...)
		}
		ents2, err := fs.segs[lastSegIdx].entries(fs.segs[lastSegIdx].startIdx, high)
		if err != nil {
			return nil, err
		}
		ents = append(ents, ents2...)
	}
	return
}
func (fs *FileStorage) PersistRange(low, high uint64) error {
	firstSegIdx := fs.findSegIdx(low)
	if firstSegIdx == -1 {
		return ErrOOB
	}
	lastSegIdx := fs.findSegIdx(high)
	if lastSegIdx == -1 {
		return ErrOOB
	}
	if firstSegIdx == lastSegIdx { // hot path
		return fs.segs[firstSegIdx].sync()
	} else { // firstSegIdx < lastSegIdx
		// issue multiple syncs
		ch := make(chan bool)
		go func(ch chan bool) {
			fs.segs[firstSegIdx].sync()
			ch <- true
		}(ch)
		for i := firstSegIdx + 1; i < lastSegIdx; i++ {
			go func(i int, ch chan bool) {
				fs.segs[i].sync()
				ch <- true
			}(i, ch)
		}
		go func(ch chan bool) {
			fs.segs[lastSegIdx].sync()
			ch <- true
		}(ch)
		for i := 0; i < lastSegIdx-firstSegIdx+1; i++ {
			_ = <-ch
		}
	}
	return nil
}

func (fs *FileStorage) StoreEntriesAndPersist(ents []LogEntry) error {
	if err := fs.StoreEntries(ents); err != nil {
		return err
	}
	if ents != nil {
		if ents[0].Index >= fs.FirstIndex() {
			return fs.PersistRange(ents[0].Index, ents[len(ents)-1].Index)
		} else {
			return fs.PersistRange(fs.FirstIndex(), ents[len(ents)-1].Index)
		}
	}
	return nil
}

func (fs *FileStorage) PersistAll() error {
	return fs.PersistRange(fs.FirstIndex(), fs.LastIndex())
}

func (fs *FileStorage) StoreEntries(ents []LogEntry) error {
	if len(ents) == 0 {
		return nil
	}
	if ents[0].Index < 1 {
		return ErrInvalidEntry
	} else if ents[0].Index < fs.FirstIndex() {
		// skip entries that have already been incorporated into the snapshot
		for len(ents) > 0 && ents[0].Index < fs.FirstIndex() {
			if len(ents) > 1 {
				ents = ents[1:]
			} else {
				ents = nil
			}
		}
	}
	if ents == nil {
		return nil
	}
	if ents[0].Index < fs.lastSegment().startIdx {
		err := fs.Truncate(ents[0].Index)
		if err != nil {
			return err
		}
	}
	err := fs.lastSegment().writeEntries(ents)
	if err != nil {
		return err
	}
	return fs.cut()
}

func lowerBound(A []int64, x int64) int {
	l := 0
	r := len(A)
	for l < r {
		m := (l + r) / 2
		if x <= A[m] {
			r = m
		} else { // A[m] < x
			l = m + 1
		}
	}
	return l
}

func segsToString(segs []segment) string {
	ans := ""
	for i := range segs {
		ans += segs[i].String() + ","
	}
	return ans
}
func (fs *FileStorage) Truncate(idx uint64) error {
	fs.cfg.I.Printf("segs before truncation at idx %d => %v", idx, segsToString(fs.segs))
	if idx > fs.LastIndex() {
		return nil
	}
	segIdx := fs.findSegIdx(idx)
	if segIdx == -1 {
		return ErrOOB
	}
	fs.cfg.I.Printf("findSegIdx(%v, %d) => %d", segsToString(fs.segs), idx, segIdx)
	err := fs.segs[segIdx].truncate(fs.img.Metadata.LastIdx, idx)
	if err != nil {
		return err
	}
	for i := segIdx + 1; i < len(fs.segs); i++ {
		err := fs.segs[i].destroy()
		if err != nil {
			return err
		}
	}
	fs.segs = fs.segs[:segIdx+1]
	fs.cfg.I.Printf("segs after truncation at idx %d => %v", idx, segsToString(fs.segs))
	tmpfile := fs.lastSegment().file
	defer tmpfile.Close()
	newLastSegFilename := filepath.Join(fs.cfg.WalDir, "last.seg")
	err = os.Rename(tmpfile.Name(), newLastSegFilename)
	if err != nil {
		fs.handleError(err, fmt.Sprintf("os.Rename(%s, %s)", tmpfile.Name(), newLastSegFilename))
		return err
	}
	newLastSegFile, err := os.OpenFile(newLastSegFilename, os.O_RDWR, 0660)
	if err != nil {
		fs.handleError(err, fmt.Sprintf("os.OpenFile(%s, os.O_RDWR, 0660)", newLastSegFilename))
		return err
	}
	fs.lastSegment().file = newLastSegFile
	fs.lastSegment().reseek = true
	return nil
}
func (fs *FileStorage) Term(i uint64) (uint64, error) {
	ents, err := fs.Entries(i, i)
	if err != nil {
		return 0, err
	}
	return ents[0].Term, nil
}

func (fs *FileStorage) SnapshotLastIndex() uint64 {
	return fs.img.Metadata.LastIdx
}
func (fs *FileStorage) SnapshotLastTerm() uint64 {
	return fs.img.Metadata.LastTerm
}
func (fs *FileStorage) FirstIndex() uint64 {
	return fs.SnapshotLastIndex() + 1
}

func (fs *FileStorage) LastIndex() uint64 {
	lastEntry, _ := fs.LastEntry()
	return lastEntry.Index
}

func (fs *FileStorage) LastEntry() (LogEntry, error) {
	for i := len(fs.segs) - 1; i >= 0; i-- {
		if fs.segs[i].empty() == false {
			ents, err := fs.segs[i].entries(fs.segs[i].endIdx, fs.segs[i].endIdx)
			if err != nil {
				return LogEntry{fs.SnapshotLastTerm(), fs.SnapshotLastIndex(), nil}, err
			}
			return ents[0], nil
		}
	}
	return LogEntry{fs.SnapshotLastTerm(), fs.SnapshotLastIndex(), nil}, ErrEmptyLog
}
func (fs *FileStorage) SaveSnapshot(img Snapshot) error {
	if fs.img.Metadata.LastIdx >= img.Metadata.LastIdx {
		return ErrStaleSnapshot
	}
	fs.img = img

	latestSnapshotFilename := filepath.Join(fs.cfg.WalDir, fmt.Sprintf("%d.snapshot", fs.img.Metadata.LastIdx))
	latestSnapshotFile, err := os.OpenFile(latestSnapshotFilename, os.O_CREATE|os.O_RDWR, 0660)
	if err != nil {
		fs.handleError(err, fmt.Sprintf("os.OpenFile(%s, os.O_EXCL|os.O_CREATE|os.O_RDWR, 0660)", latestSnapshotFilename))
		return err
	}
	oldImgFile := fs.imgFile
	if fs.imgFile != nil {
		fs.imgFile.Close()
	}
	fs.imgFile = latestSnapshotFile
	err = fs.writeSnapshotFile()
	if err != nil {
		fs.handleError(err, "fs.writeSnapshotFile()")
		return err
	}
	if oldImgFile != nil {
		defer os.Remove(oldImgFile.Name())
	}
	segIdx := fs.findSegIdx(fs.img.Metadata.LastIdx)
	fs.cfg.W.Printf("fs.findSegIdx(%d)=> %d, segs %v", fs.img.Metadata.LastIdx, segIdx, fs.segs)
	for i := 0; i < segIdx; i++ {
		err = fs.segs[i].destroy()
		if err != nil {
			fs.handleError(err, fmt.Sprintf("%s.destroy()", fs.segs[i]))
			panic(err)
		}
	}
	if segIdx == -1 {
		return nil
	}
	isLast := segIdx == len(fs.segs)-1
	err = fs.segs[segIdx].truncateBegin(fs.img.Metadata.LastIdx, isLast)
	if err != nil {
		fs.handleError(err, fmt.Sprintf("%v.truncateBegin(%d)", fs.segs[segIdx], fs.img.Metadata.LastIdx))
		return err
	}
	var tmp []segment
	// remove empty segment
	if isLast == false && fs.segs[segIdx].empty() {
		tmp = fs.segs[segIdx+1:]
	} else {
		tmp = fs.segs[segIdx:]
	}
	fs.segs = nil
	fs.segs = append(fs.segs, tmp...)
	fs.cfg.W.Printf("after saving snapshtoting, segs %s ", segsToString(fs.segs))
	return nil
}
func (fs *FileStorage) Snapshot() Snapshot {
	return fs.img
}
func (fs *FileStorage) Count() int {
	n := 0
	for i := range fs.segs {
		n += int(fs.segs[i].count())
	}
	return n
}

func (fs *FileStorage) Close() error {
	for i := range fs.segs {
		err := fs.segs[i].close()
		if err != nil {
			return err
		}
	}
	if fs.imgFile != nil {
		fs.cfg.I.Printf("closing %s", fs.imgFile.Name())
		fs.imgFile.Close()
	}
	if fs.ssFile != nil {
		fs.cfg.I.Printf("closing %s", fs.ssFile.Name())
		fs.ssFile.Close()
	}

	return nil
}

func (fs *FileStorage) Cleanup() error {
	for i := range fs.segs {
		fs.segs[i].destroy()
	}
	if fs.ssFile != nil {
		os.Remove(fs.ssFile.Name())
		fs.ssFile.Close()
		fs.ssFile = nil
	}

	if fs.imgFile != nil {
		os.Remove(fs.imgFile.Name())
		fs.imgFile.Close()
		fs.imgFile = nil
	}

	absDir, err := filepath.Abs(fs.cfg.WalDir)
	log.Printf("removing dir %s", absDir)
	err = os.RemoveAll(absDir)
	if err != nil {
		fs.cfg.I.Printf("error  os.RemoveAll(%s), %v", absDir, err)
		return err
	}
	return nil
}

func (fs *FileStorage) LogSize() uint64 {
	size := uint64(0)
	for i := range fs.segs {
		size += uint64(fs.segs[i].size())
	}
	return size
}
