package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zxjcarrot/ares/mockrpc"
)

//
// ApplyMsg is used to notify whoever is on the other side of the channel that
// an entry has been commited and it could be safely applied to the state machine.
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Term        uint64
	Index       uint64
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//Role consists of three roles in the raft algorithm.
type Role int

const (
	Follower  Role = 1
	Candidate      = 2
	Leader         = 3
)

// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     map[uint64]*PeerInfo // indexed by peerInfo.id
	rpcServer RpcServer
	rpcClient RpcClient
	persister *Persister
	me        uint64 // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	rg        *rand.Rand
	elecTimer *time.Timer
	quitCh    chan bool
	applyCh   chan<- ApplyMsg
	// notifies the commit goroutine that commitIdx has advanced,
	// a false value notifies the committer to quit
	commitUpdateCh chan int
	// notifies anyone who is waiting for the commitRoutine to exit.
	// It's necessary since commitRoutine has to apply
	// every committed entry to the upper state machine
	// before Raft can be safely stopped
	commitRoutineDoneCh chan bool //
	resetElectTimerCh   chan int
	repChan             chan bool // notify the replication routine that there are new entries to be replicated
	heartbeatTicker     *time.Ticker
	stopped             int32
	// Persistent states
	l *raftLog

	// Volatile states on all servers
	oldCommitIdx uint64 // oldCommitIdx <= commitIdx
	commitIdx    uint64
	lastApplied  uint64
	leaderId     uint64 // leaderId in current term

	// Volatile states on leaders
	nextIdx  map[uint64]uint64
	matchIdx map[uint64]uint64

	role Role
	// fields for log compaction
	// actual snapshot data is stored in l.s
	snapshotSenderChs map[uint64]chan int
	partialSnapshots  map[uint64]partialSnapshot

	T, D, I, W, E *log.Logger

	cfg *Config
}

func (rf *Raft) isStopped() bool {
	return atomic.LoadInt32(&rf.stopped) == 1
}

//Generate randomized election timeout
func (rf *Raft) nextElectionTimeout() time.Duration {
	d := rf.cfg.ElectionTimeoutLower + time.Duration(rf.rg.Int31n(int32(rf.cfg.ElectionTimeoutUpper-rf.cfg.ElectionTimeoutLower)))
	return d
}

func (rf *Raft) getCurTermUnlocked() uint64 {
	return rf.l.s.StableState().currentTerm
}

func (rf *Raft) getCurTerm() uint64 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getCurTermUnlocked()
}

func (rf *Raft) getStateUnlocked() (uint64, bool) {
	return rf.getCurTermUnlocked(), rf.getRoleUnlocked() == Leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (uint64, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getStateUnlocked()
}

func (rf *Raft) getLeaderIdUnlocked() uint64 {
	return rf.leaderId
}

func (rf *Raft) getLeaderId() uint64 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getLeaderIdUnlocked()
}

func (rf *Raft) setLeaderIdUnlocked(leaderId uint64) {
	rf.leaderId = leaderId
}

func (rf *Raft) setLeaderId(leaderId uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.setLeaderIdUnlocked(leaderId)
}

func (rf *Raft) getVotedForUnlocked() uint64 {
	return rf.l.s.StableState().votedFor
}

func (rf *Raft) getVotedFor() uint64 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getVotedForUnlocked()
}

func (rf *Raft) setVotedForUnlocked(votedFor uint64) {
	ss := rf.l.s.StableState()
	ss.votedFor = votedFor
	rf.l.s.StoreStableStateAndPersist(ss)
	rf.persist()
}
func (rf *Raft) setVotedFor(votedFor uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.setVotedForUnlocked(votedFor)
}

func (rf *Raft) getRoleUnlocked() Role {
	return rf.role
}

func (rf *Raft) getRole() Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getRoleUnlocked()
}

func (rf *Raft) setRoleUnlocked(r Role) {
	rf.role = r
}
func (rf *Raft) setRole(r Role) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.setRoleUnlocked(r)
}

func (rf *Raft) getTermAtUnlocked(idx uint64) (uint64, error) {
	if idx == rf.l.s.SnapshotLastIndex() {
		return rf.l.s.SnapshotLastTerm(), nil
	}
	ents, err := rf.l.s.Entries(idx, idx)
	if err != nil {
		return 0, err
	}
	return ents[0].Term, nil
}

func (rf *Raft) getTermAt(idx uint64) (uint64, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getTermAtUnlocked(idx)
}

func (rf *Raft) getCommitIdxUnlocked() uint64 {
	return rf.commitIdx

}
func (rf *Raft) getCommitIdx() uint64 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getCommitIdxUnlocked()
}

func (rf *Raft) getNextIdxUnlocked(server uint64) uint64 {
	return rf.nextIdx[server]
}
func (rf *Raft) getNextIdx(server uint64) uint64 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getNextIdxUnlocked(server)
}

func (rf *Raft) decNextIdx(serverId uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIdx[serverId]--
	if rf.nextIdx[serverId] <= 0 {
		rf.nextIdx[serverId] = 1
	}
}

func (rf *Raft) setNextIdxUnlocked(server, idx uint64) {
	rf.nextIdx[server] = idx
}
func (rf *Raft) setNextIdx(server, idx uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.setNextIdxUnlocked(server, idx)
}

func (rf *Raft) getMatchIdx(server uint64) uint64 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.matchIdx[server]
}

func (rf *Raft) decMatchIdx(serverId uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.matchIdx[serverId]--
}

func (rf *Raft) setMatchIdxUnlocked(server, idx uint64) {
	rf.matchIdx[server] = idx
}
func (rf *Raft) setMatchIdx(server, idx uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.setMatchIdxUnlocked(server, idx)
}

func (rf *Raft) setTermUnlocked(newTerm uint64) {
	ss := rf.l.s.StableState()
	if ss.currentTerm < newTerm {
		ss.currentTerm = newTerm
		ss.votedFor = Unvoted
		rf.l.s.StoreStableStateAndPersist(ss)
		rf.persist()
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) setTerm(newTerm uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.setTermUnlocked(newTerm)
}

func (rf *Raft) getLastLogEntryInfoUnlocked() (uint64, uint64) {
	lastEntry, err := rf.l.s.LastEntry()
	if err == ErrEmptyLog {
		return rf.l.s.SnapshotLastTerm(), rf.l.s.SnapshotLastIndex()
	}
	return lastEntry.Term, lastEntry.Index
}

// return the term and index of last log entry
// 0 if the log is empty
func (rf *Raft) getLastLogEntryInfo() (uint64, uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getLastLogEntryInfoUnlocked()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	if rf.persister == nil {
		return
	}
	// in test mode
	//PrintMemoryStatus("before persist", rf.W)
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	ss := rf.l.s.StableState()
	fi := rf.l.s.FirstIndex()
	li := rf.l.s.LastIndex()
	var logs []LogEntry
	if fi > rf.l.s.SnapshotLastIndex() {
		//rf.I.Printf("fi %d, li %d, rf.l.s.SnapshotLastIndex() %d", fi, li, rf.l.s.SnapshotLastIndex())
		logs, _ = rf.l.s.Entries(fi, li)
	}
	e.Encode(ss.currentTerm)
	e.Encode(ss.votedFor)
	e.Encode(logs)
	rf.persister.SaveRaftState(w.Bytes())
	//PrintMemoryStatus("after persist", rf.W)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if rf.persister == nil {
		return // in test mode
	}
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	d := gob.NewDecoder(bytes.NewBuffer(data))
	var ss StableState
	var logs []LogEntry
	d.Decode(&ss.currentTerm)
	d.Decode(&ss.votedFor)
	d.Decode(&logs)
	if len(logs) > 0 && logs[0].Index != rf.l.s.SnapshotLastIndex()+1 {
		rf.E.Fatalf("snapshotLastIdx+1 = %d != rf.logs[0].Index=%d !!!!!!!!!!!!!", rf.l.s.SnapshotLastIndex()+1, logs[0].Index)
	}
	if ss.currentTerm > 0 {
		rf.l.s.StoreStableState(ss)
	}
	rf.l.s.StoreEntries(logs)
}

func (rf *Raft) readSnapshot(data []byte) {
	if rf.persister == nil {
		return // in test mode
	}
	if data == nil {
		return
	}
	var s Snapshot
	d := gob.NewDecoder(bytes.NewBuffer(data))
	d.Decode(&s)
	rf.l.s.SaveSnapshot(s)
	rf.commitUpdateCh <- applySnapshot
	rf.T.Printf("read snapshot %v", s)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         uint64
	CandidateId  uint64
	LastLogTerm  uint64
	LastLogIndex uint64
}

func (rva RequestVoteArgs) String() string {
	return fmt.Sprintf("(%v,%v,%v,%v)", rva.Term, rva.CandidateId, rva.LastLogTerm, rva.LastLogIndex)
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        uint64
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	if rf.isStopped() {
		return errors.New("server already stopped")
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.getCurTermUnlocked()
	// Your code here.
	if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.VoteGranted = false
		return nil
	}

	lastLogTerm, lastLogIndex := rf.getLastLogEntryInfoUnlocked()
	rf.T.Printf("lastLogTerm:%d, lastLogIndex:%d", lastLogTerm, lastLogIndex)
	asUpToDateAs := func() bool {
		if lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			return true
		}
		rf.I.Printf("candidate%d's log(%d,%d) is not as up to date as this server(%d, %d), reject",
			args.CandidateId, args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex)
		return false
	}

	if currentTerm < args.Term {
		rf.I.Printf("higher term %d from candidate%d. To the follower!", args.Term, args.CandidateId)
		rf.convertToFollowerUnlocked(args.Term)
		currentTerm = args.Term
	}
	votedFor := rf.getVotedForUnlocked()
	if (votedFor == Unvoted || votedFor == args.CandidateId) && asUpToDateAs() {
		rf.setVotedForUnlocked(args.CandidateId)
		reply.VoteGranted = true
		reply.Term = args.Term
		return nil
	}
	rf.I.Printf("voted false to candidate%d, voted for peer%d", args.CandidateId, votedFor)
	reply.VoteGranted = false
	reply.Term = currentTerm
	return nil
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if mockrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server uint64, args RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.rpcClient.Call(rf.peers[server], "Raft.RequestVote", args, reply) == nil
}

//
// example RequestVote RPC arguments structure.
//
type AppendEntriesArgs struct {
	// Your data here.
	Term         uint64
	LeaderId     uint64
	PrevLogIdx   uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

func (aea AppendEntriesArgs) String() string {
	if len(aea.Entries) == 0 {
		return fmt.Sprintf("%d, %d, %d, %d, hearbeat, %d)",
			aea.Term, aea.LeaderId, aea.PrevLogIdx, aea.PrevLogTerm, aea.LeaderCommit)
	} else {
		return fmt.Sprintf("%d, %d, %d, %d, [%d-%d], %d)",
			aea.Term, aea.LeaderId, aea.PrevLogIdx, aea.PrevLogTerm, aea.Entries[0].Index, aea.Entries[len(aea.Entries)-1].Index, aea.LeaderCommit)
	}
}

//
// example RequestVote RPC reply structure.
//
type AppendEntriesReply struct {
	// Your data here.
	Term    uint64
	Success bool
	// The index of the first entry at term Term.
	// If Success == false and FirstIndex == 0,
	// then it means the peer's log don't have the log entry
	// with a index of PrevLogIdx and term of PrevLogTerm.
	// If Success == false and FirstIndex > 0,
	// the leader should set the nextIndex for this peer to FirstIndex + 1,
	// skipping all the conflicting entries.
	// If Success is true, then this field is ignored.
	FirstIndex   int64  // if FirstIndex is negative, then negating it gives the lastEntryLogEntry
	ConflictTerm uint64 // term of the entry at FirstIndex
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if rf.isStopped() {
		return errors.New("server already stopped")
	}
	currentTerm := rf.getCurTerm()
	if args.Term > currentTerm && rf.me != args.LeaderId {
		rf.I.Printf("higher term %d. To the follower!", args.Term)
		rf.convertToFollower(args.Term)
	}
	notifyCommitRoutine := false
	rf.resetElectTimerCh <- 1 // reset tiemout timer anyway
	rf.mu.Lock()
	reply.FirstIndex = 0
	reply.ConflictTerm = 0
	if args.Term < currentTerm {
		reply.Success = false
		reply.Term = currentTerm
	} else if c, firstIndex, conflictTerm := rf.l.conflictingEntryUnlocked(args.PrevLogIdx, args.PrevLogTerm); c {
		reply.Term = currentTerm
		reply.Success = false
		reply.FirstIndex = firstIndex
		reply.ConflictTerm = conflictTerm
		rf.setLeaderIdUnlocked(args.LeaderId)
	} else {
		/*
			3. If an existing entry conflicts with a new one (same index
			but different terms), delete the existing entry and all that
			follow it (§5.3)
			4. Append any new entries not already in the log
			5. If leaderCommit > commitIndex, set commitIndex =
			min(leaderCommit, index of last new entry)
		*/
		if len(args.Entries) > 0 {
			candidates := make([]LogEntry, 0, len(args.Entries))
			for _, entry := range args.Entries {
				ents, err := rf.l.s.Entries(entry.Index, entry.Index)
				if err == ErrCompacted {
					rf.I.Printf("entry %v already applied in snapshot, ignored...", entry)
					continue
				} else if err == nil {
					if ents[0].Index != entry.Index {
						rf.E.Fatalf("leader sent an entry at index %d have a different log index %d", entry.Index, ents[0].Index)
					} else if ents[0].Term != entry.Term {
						rf.W.Printf("found conflicting entries at index %d, removing the entry and these follow it.", entry.Index)
						rf.l.s.Truncate(entry.Index)
					}
					/*
						else -> rf.logs[realIdx].Term == entry.Term
							simply ignore
					*/
				}
				if entry.Index > rf.l.s.LastIndex() { // readlIdx >= len(rf.logs)
					var lastEntryIndex uint64
					if len(candidates) == 0 {
						_, lastEntryIndex = rf.getLastLogEntryInfoUnlocked()
					} else {
						lastEntryIndex = candidates[len(candidates)-1].Index
					}

					if lastEntryIndex == entry.Index-1 {
						candidates = append(candidates, entry)
					} else {
						rf.E.Fatalf("lastEntryIdx = %d, newEntryIdx = %d, found a gap!!!", lastEntryIndex, entry.Index)
					}
				}
			}

			if len(candidates) > 0 {
				err := rf.l.s.StoreEntriesAndPersist(candidates)
				if err != nil {
					rf.E.Fatalf("StoreEntriesAndPersist(%v) => %v", candidates, err)
				}
				rf.persist()
			}
		}

		if args.LeaderCommit > rf.commitIdx {
			oldCommit := rf.commitIdx
			_, lastNewEntryIndex := rf.getLastLogEntryInfoUnlocked()
			if args.LeaderCommit <= lastNewEntryIndex {
				rf.commitIdx = args.LeaderCommit
			} else {
				rf.commitIdx = lastNewEntryIndex
			}
			if rf.commitIdx < oldCommit {
				rf.E.Fatalf("rf.commitIdx %d < oldCommitIdx %d!!!!!!!", rf.commitIdx, oldCommit)
			}
			rf.I.Printf("commitIdx updated %d --> %d", oldCommit, rf.commitIdx)
			if oldCommit < rf.commitIdx {
				notifyCommitRoutine = true
			}
		}

		rf.setLeaderIdUnlocked(args.LeaderId)

		reply.Success = true
		reply.Term = currentTerm
	}
	rf.mu.Unlock()
	if notifyCommitRoutine {
		rf.commitUpdateCh <- advanceCommitIdx
	}
	return nil
}

func (rf *Raft) sendAppendEntries(server uint64, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.rpcClient.Call(rf.peers[server], "Raft.AppendEntries", args, reply) == nil
}

// append the command to the leader's local log
// return the index and term at which the command is stored
func (rf *Raft) appendLocalLogEntriesUnlocked(commands [][]byte) ([]LogEntry, uint64, uint64) {
	currentTerm := rf.getCurTermUnlocked()
	_, lastEntryIndex := rf.getLastLogEntryInfoUnlocked()
	newEntries := make([]LogEntry, 0, len(commands))
	for i := 0; i < len(commands); i++ {
		newEntries = append(newEntries, LogEntry{currentTerm, lastEntryIndex + uint64(i) + 1, commands[i]})
	}
	err := rf.l.s.StoreEntries(newEntries)
	if err != nil {
		rf.E.Fatalf("failed to store new entries [%d-%d] to the log: %v", lastEntryIndex+1, lastEntryIndex+uint64(len(commands)), err)
	}
	rf.persist()
	return newEntries, currentTerm, lastEntryIndex + 1
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command []byte) (uint64, uint64, bool) {
	return rf.StartBatch([][]byte{command})
}

// start agreement on next multiple commands to be appended to the Raft's log.
// if this server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.

// the first return value is the index that the first command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) StartBatch(commands [][]byte) (uint64, uint64, bool) {
	rf.mu.Lock()
	currentTerm, isLeader := rf.getStateUnlocked()
	if isLeader == false {
		rf.mu.Unlock()
		//rf.I.Printf("not a leader in term %d", currentTerm)
		return 0, currentTerm, false
	}
	_, logEntryTerm, logEntryIndex := rf.appendLocalLogEntriesUnlocked(commands)
	// do fsync & replication in parallel

	select {
	case rf.repChan <- true:
	default:
	}
	rf.l.s.PersistRange(logEntryIndex, rf.l.s.LastIndex())
	rf.mu.Unlock()
	return logEntryIndex, logEntryTerm, isLeader
}

// reset the election timer and return the new election timeout
func (rf *Raft) resetElectionTimer(drain bool) time.Duration {
	d := rf.nextElectionTimeout()
	if !rf.elecTimer.Stop() && drain {
		<-rf.elecTimer.C //drain the expired channel
	}
	//rf.T.Printf("New election timer set to expire in %s", d)
	rf.elecTimer.Reset(d)

	return d
}

// convert to follower role.
// this must be called with rf.mu unlocked
func (rf *Raft) convertToFollowerUnlocked(term uint64) {
	rf.role = Follower
	rf.setTermUnlocked(term)
	go func() {
		rf.resetElectTimerCh <- 1
	}()
}

// convert to follower role.
// this must be called with rf.mu unlocked
func (rf *Raft) convertToFollower(term uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.convertToFollowerUnlocked(term)
}

func (rf *Raft) sendHeartbeat(currentTerm, server uint64) {
	rf.mu.Lock()
	_, prevLogIdx := rf.getLastLogEntryInfoUnlocked()
	prevLogTerm, err := rf.getTermAtUnlocked(prevLogIdx)
	leaderCommit := rf.getCommitIdxUnlocked()
	if err != nil {
		rf.W.Printf("can't get term at index %v, error %#v", prevLogIdx, err)
		return
	}
	rf.mu.Unlock()
	args := AppendEntriesArgs{currentTerm,
		rf.me, prevLogIdx,
		prevLogTerm,
		nil,
		leaderCommit,
	}
	reply := &AppendEntriesReply{}
	go func() {
		ok := rf.sendAppendEntries(server, args, reply)
		if ok == false {
			rf.T.Printf("Failed to send heartbeat to peer%d in term %d", server, currentTerm)
			return
		}
		rf.mu.Lock()
		curRole := rf.getRoleUnlocked()
		newestTerm := rf.getCurTermUnlocked()
		rf.mu.Unlock()
		if currentTerm < newestTerm || curRole != Leader {
			rf.I.Printf("heartbeating routine, a newer term %d started, commiting suicide in term", newestTerm)
			return
		}
		if reply.Success {
			//log.T.Printf("Peer%d pong, match %d", server, rf.getMatchIdx(server))
			rf.setMatchIdx(server, prevLogIdx)
			return
		} else {
			//rf.I.Println("peer", server, "'s reply ", reply)

			if reply.Term > currentTerm {
				rf.I.Printf("newer term %d started, stepping down", reply.Term)
				rf.convertToFollower(reply.Term)
			} else {
				rf.mu.Lock()
				// follower is lagged behind, start recovery
				nextIdx := rf.getNextIdxUnlocked(server)
				var newNextIdx uint64
				if reply.FirstIndex < 0 {
					newNextIdx = uint64(-reply.FirstIndex + 1)
				} else if reply.FirstIndex == 0 {
					newNextIdx = nextIdx - 1
				} else {
					//the leader decrement nextIndex to bypass all of the conflicting entries in that term
					newNextIdx = args.PrevLogIdx
					for ; newNextIdx >= rf.l.s.FirstIndex() && newNextIdx >= uint64(reply.FirstIndex); newNextIdx-- {
						ents, err := rf.l.s.Entries(newNextIdx, newNextIdx)
						if err == nil && ents[0].Term == reply.ConflictTerm {
							break
						}
					}
					newNextIdx++
				}
				if newNextIdx <= 0 {
					newNextIdx = 1
				}
				rf.setNextIdxUnlocked(server, newNextIdx)
				rf.mu.Unlock()
				if nextIdx != newNextIdx {
					rf.I.Printf("sendHeartbeat found peer%d's log inconsistent, nextidx %d --> %d", server, nextIdx, newNextIdx)
				}
				rf.replicateSingle(server, currentTerm)
			}
		}
	}()
}

func (rf *Raft) sendHeartbeats(thisTerm uint64) {
	// broadcast heartbeats to all servers
	for serverId := range rf.peers {
		if serverId == rf.me {
			// Prevent the timeout timer from firing off.
			// This works as a heartbeat to itself.
			rf.resetElectTimerCh <- 1
		} else {
			rf.sendHeartbeat(thisTerm, serverId)
		}
	}
}

func (rf *Raft) advanceCommit(thisTerm uint64) {
	rf.mu.Lock()
	/*
		If there exists an N such that N > commitIndex, a majority
		of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
	*/
	_, N := rf.getLastLogEntryInfoUnlocked()
loop:
	for ; N > rf.commitIdx; N-- {
		counts := 1 // count leader itself
		for serverId, v := range rf.matchIdx {
			if serverId != rf.me && v >= N {
				counts++
			}
		}
		if counts > len(rf.peers)/2 {
			term, err := rf.getTermAtUnlocked(N)
			if err == nil && term == thisTerm {
				break loop
			} else {
				rf.I.Printf("don't advancing commit idx to %d since it contains log entries belong to previous leader at term %d.", N, term)
			}
		}
	}
	notifyCommitRoutine := false
	if rf.commitIdx < N {
		rf.I.Printf("advancing commmit index %d --> %d", rf.commitIdx, N)
		rf.commitIdx = N
		notifyCommitRoutine = true
	}
	rf.mu.Unlock()
	if notifyCommitRoutine {
		rf.commitUpdateCh <- advanceCommitIdx
	}
}

type InstallSnapshotArgs struct {
	Id       uint64 //randomly generated integer for every snapshot transfer
	Term     uint64
	LeaderId uint64
	LastIdx  uint64 // the index of the last applied entry in the snapshot
	LastTerm uint64 // the term of the last applied entry in the snapshot
	Offset   int
	Data     []byte
	Done     bool
}

func (isa InstallSnapshotArgs) String() string {
	return fmt.Sprintf("(%v,%v,%v,%v,%v,%v,payload...,%v)", isa.Id, isa.Term, isa.LeaderId, isa.LastIdx, isa.LastTerm, isa.Offset, isa.Done)
}

type InstallSnapshotReply struct {
	Term uint64
	Good bool
}

type partialSnapshot struct {
	id       uint64
	term     uint64
	lastIdx  uint64
	lastTerm uint64
	snapshot []byte
	offset   int
	done     bool
}

func (rf *Raft) saveSnapshotUnlocked(state []byte, lastAppliedIdx, lastAppliedTerm uint64) {
	if rf.isStopped() || lastAppliedIdx < 1 {
		return
	}
	if lastAppliedIdx <= rf.l.s.SnapshotLastIndex() {
		rf.I.Printf("Stale snapshot[%v], latest snapshot[%v], ignored.", lastAppliedIdx, rf.l.s.SnapshotLastIndex())
		return
	}
	rf.I.Printf("saving snapshot[%d]", lastAppliedIdx)
	snapshot := Snapshot{SnapshotMetadata{lastAppliedTerm, lastAppliedIdx}, state}
	err := rf.l.s.SaveSnapshot(snapshot)
	if err != nil {
		rf.E.Printf("rf.l.s.SaveSnapshot(%v) => %v", snapshot, err)
		return
	}
	if rf.persister != nil {
		w := new(bytes.Buffer)
		encoder := gob.NewEncoder(w)
		encoder.Encode(snapshot)
		rf.persister.SaveSnapshot(w.Bytes())
		rf.persist()
	}

	rf.I.Printf("snapshot containing entries [1-%d] saved, lastIncludedTerm %d, now purging log entries...", lastAppliedIdx, lastAppliedTerm)

	if rf.l.s.Count() == 0 {
		rf.I.Printf("logs after purging: empty [%d-%d]", rf.l.s.FirstIndex(), rf.l.s.LastIndex())
	} else {
		rf.I.Printf("logs after purging: [%d-%d]", rf.l.s.FirstIndex(), rf.l.s.LastIndex())
	}
}

func (rf *Raft) LatestSnapshotIdx() uint64 {
	return rf.l.s.SnapshotLastIndex()
}

func (rf *Raft) SaveSnapshot(state []byte, lastAppliedIdx uint64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ents, err := rf.l.s.Entries(lastAppliedIdx, lastAppliedIdx)
	if err == nil {
		rf.saveSnapshotUnlocked(state, ents[0].Index, ents[0].Term)
	} else if err == ErrCompacted {
		rf.W.Printf("Stale snapshot[%v], latest snapshot[%v], ignored.", lastAppliedIdx, rf.l.s.SnapshotLastIndex())
	} else {
		rf.E.Fatalf("rf.l.s.Entries(%d, %d) => %v", lastAppliedIdx, lastAppliedIdx, err)
	}
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	if rf.isStopped() {
		return errors.New("server already stopped")
	}
	currentTerm := rf.getCurTerm()
	if args.Term > currentTerm {
		rf.I.Printf("higher term %d. To the follower!", args.Term)
		rf.convertToFollower(args.Term)
		currentTerm = args.Term
	} else if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.Good = false
		return nil
	}

	reply.Term = currentTerm
	reply.Good = true
	rf.mu.Lock()
	notifyCommitRoutine := false
	ps, ok := rf.partialSnapshots[args.LastIdx]
	if ok == false {
		// create a new partial snapshot
		ps = partialSnapshot{args.Id, args.Term, args.LastIdx, args.LastTerm, nil, 0, false}
	}
	if ps.id != args.Id || ps.lastTerm != args.LastTerm || ps.offset != args.Offset {
		rf.W.Printf("InstallSnapshot partialSnapshot[%d] %v mismatched with args %v", ps.lastIdx, ps, args)
		reply.Good = false
	} else if ps.lastIdx <= rf.l.s.SnapshotLastIndex() {
		rf.W.Printf("snapshot[%d] is no newer than the last snapshot[%d], reply false", args.LastIdx, rf.l.s.SnapshotLastIndex())
		reply.Good = false
	} else {
		ps.snapshot = append(ps.snapshot, args.Data...)
		ps.done = args.Done
		ps.offset += len(args.Data)
		rf.partialSnapshots[ps.lastIdx] = ps
		rf.I.Printf("InstallSnapshot received partialSnapshot[%d]'s payload[%d-%d]", ps.lastIdx, args.Offset, args.Offset+len(args.Data)-1)
		if ps.done {
			rf.I.Printf("snapshot[%d] at term %d from leader%d successfully received", ps.lastIdx, args.Term, rf.leaderId)

			// 5. Save snapshot file, discard any existing or partial snapshot
			//	with a smaller index
			for lastIdx := range rf.partialSnapshots {
				if lastIdx <= ps.lastIdx {
					delete(rf.partialSnapshots, lastIdx)
				}
			}
			rf.saveSnapshotUnlocked(ps.snapshot, ps.lastIdx, ps.lastTerm)
			notifyCommitRoutine = true
		}
	}
	rf.setLeaderIdUnlocked(args.LeaderId)
	rf.mu.Unlock()
	if notifyCommitRoutine {
		rf.commitUpdateCh <- advanceCommitIdx
	}
	return nil
}

func (rf *Raft) snapshotSender(server, term uint64, ch <-chan int) {
	rf.I.Printf("Term %d snapshot sender for peer%d started. Now standing by to transfer snapshots.", term, server)
	for _ = range ch {
		rf.I.Printf("begin snapshot transfer to peer%d at term %d", server, term)
		if rf.getCurTerm() != term || rf.isStopped() {
			break
		}
		snapshot := rf.l.s.Snapshot()
		lastIdx := snapshot.Metadata.LastIdx
		lastTerm := snapshot.Metadata.LastTerm
		snapshotBytes := snapshot.Data

		args := InstallSnapshotArgs{rf.rg.Uint64(), term, rf.me, lastIdx, lastTerm, 0, nil, false}
		off := 0
		for ; off < len(snapshotBytes); off += rf.cfg.SnapshotChunkSize {
			args.Offset = off
			if off+rf.cfg.SnapshotChunkSize < len(snapshotBytes) {
				args.Data = snapshotBytes[off : off+rf.cfg.SnapshotChunkSize]
			} else {
				args.Data = snapshotBytes[off:]
				args.Done = true
			}
			reply := InstallSnapshotReply{0, false}
			err := rf.rpcClient.Call(rf.peers[server], "Raft.InstallSnapshot", args, &reply)
			if err != nil {
				rf.W.Printf("%s, transfer aborted", err)
				break
			}
			if reply.Term > term {
				rf.I.Printf("InstallSnapshot to peer%d replied a higher term %d, stepping down", server, reply.Term)
				if rf.getCurTerm() < reply.Term {
					rf.convertToFollower(reply.Term)
				}
				break
			}
			if reply.Good == false {
				rf.W.Printf("InstallSnapshot to peer%d replied false, transfer aborted", server)
				break
			}
		}
		if off >= len(snapshotBytes) {
			rf.I.Printf("snapshot containing entries [1-%d] successfully transferred to peer%d at term %d", lastIdx, server, term)
			rf.setNextIdx(server, lastIdx+1)
		}
	}
	rf.I.Printf("Term %d snapshot sender for peer%d stopped.", term, server)
}

func (rf *Raft) replicateSingle(server, thisTerm uint64) {
	rf.mu.Lock()
	_, lastLogEntryIdx := rf.getLastLogEntryInfoUnlocked()
	prevLogIdx := rf.getNextIdxUnlocked(server) - 1
	if prevLogIdx >= lastLogEntryIdx { //no new entries
		rf.mu.Unlock()
		//rf.I.Printf("prevLogIdx %d, lastLogEntryIdx %d, nothing to replicate to peer%d.", prevLogIdx, lastLogEntryIdx, server)
		return
	}
	//rf.T.Printf("replicating entries [%d-%d] to peer%d", prevLogIdx+1, lastLogEntryIdx, server)
	if rf.l.s.Count() == 0 || prevLogIdx+1 < rf.l.s.FirstIndex() {
		rf.mu.Unlock()
		rf.I.Printf("peer%d missing entries starting at index %d, sending snapshot instead.", server, prevLogIdx+1)
		select {
		case rf.snapshotSenderChs[server] <- 1:
			rf.I.Printf("snapshot transfer to peer%d at term %d initiated", server, thisTerm)
		default:
			rf.I.Printf("a snapshot transfer to peer%d at term %d already started.", server, thisTerm)
		}

		return
	}
	// We replicate rf.cfg.ReplicationUnit entries at a time at most to avoid flooding the receiver
	n := lastLogEntryIdx - prevLogIdx
	if n > rf.cfg.ReplicationUnit {
		n = rf.cfg.ReplicationUnit
	}
	//entries := make([]LogEntry, )
	// replicate last n entries
	entries, err := rf.l.s.Entries(prevLogIdx+1, prevLogIdx+n)
	if err != nil {
		rf.E.Fatalf("rf.l.s.Entries(%v, %v) -> %#v", prevLogIdx+1, rf.l.s.LastIndex(), err)
	}
	//copy(entries, ents)

	prevLogTerm, err := rf.getTermAtUnlocked(prevLogIdx)
	if err != nil {
		rf.E.Fatalf("rf.getTermAtUnlocked(%v) -> %#v", prevLogIdx, err)
	}
	leaderCommit := rf.getCommitIdxUnlocked()
	rf.mu.Unlock()

	args := AppendEntriesArgs{thisTerm,
		rf.me, prevLogIdx,
		prevLogTerm,
		entries,
		leaderCommit,
	}

	reply := &AppendEntriesReply{}
	go func() {
		ok := rf.sendAppendEntries(server, args, reply)
		if ok == false {
			rf.W.Printf("failed to send %d entries to peer%d in term %d", len(entries), server, thisTerm)
			return
		}
		if reply.Success {
			//rf.T.Printf("replicated %d entries to peer%d in term %d", len(entries), server, thisTerm)
			rf.mu.Lock()
			rf.setNextIdxUnlocked(server, entries[len(entries)-1].Index+1)
			rf.setMatchIdxUnlocked(server, entries[len(entries)-1].Index)
			rf.mu.Unlock()
			rf.advanceCommit(thisTerm)
			//rf.replicateSingle(server, thisTerm)
		} else {
			rf.I.Printf("peer%d rejected AppendEntries request for entries %s, reply %v", server, args, reply)
			if reply.Term > rf.getCurTerm() {
				rf.I.Printf("newer term %d started, stepping down", reply.Term)
				rf.convertToFollower(reply.Term)
			} else { // If failed because of log inconsistency, take a step back and retry.
				rf.mu.Lock()
				curRole := rf.getRoleUnlocked()
				curTerm := rf.getCurTermUnlocked()
				rf.mu.Unlock()
				if curRole == Leader && curTerm == thisTerm {
					rf.mu.Lock()
					// follower is lagged behind, start recovery
					nextIdx := rf.getNextIdxUnlocked(server)
					var newNextIdx uint64
					if reply.FirstIndex < 0 {
						newNextIdx = uint64(-reply.FirstIndex + 1)
					} else if reply.FirstIndex == 0 {
						newNextIdx = nextIdx - 1
					} else {
						//the leader decrement nextIndex to bypass all of the conflicting entries in that term
						newNextIdx = args.PrevLogIdx
						for ; newNextIdx > rf.l.s.SnapshotLastIndex() && newNextIdx >= uint64(reply.FirstIndex); newNextIdx-- {
							ents, err := rf.l.s.Entries(newNextIdx, newNextIdx)
							if err == nil && ents[0].Term == reply.ConflictTerm {
								break
							}
						}
						newNextIdx++
					}
					if newNextIdx <= 0 {
						newNextIdx = 1
					}
					rf.setNextIdxUnlocked(server, newNextIdx)
					rf.mu.Unlock()
					rf.I.Printf("replicateSingle found peer%d's log inconsistent, nextidx %d --> %d", server, nextIdx, newNextIdx)

					rf.replicateSingle(server, thisTerm)
				}
			}
		}
	}()
}

func (rf *Raft) replicate(term uint64) {
	for serverId := range rf.peers {
		if serverId == rf.me {
			continue
		}
		rf.replicateSingle(serverId, term)
	}
	if len(rf.peers) == 1 { // single-node cluster
		rf.advanceCommit(term)
	}
}

func (rf *Raft) convertToLeader(term uint64) {
	rf.resetElectTimerCh <- 1 // reset election timer
	rf.mu.Lock()
	rf.setRoleUnlocked(Leader)
	_, lastLogIndex := rf.getLastLogEntryInfoUnlocked()
	rf.nextIdx = make(map[uint64]uint64, len(rf.peers))
	rf.matchIdx = make(map[uint64]uint64, len(rf.peers))
	for serverId := range rf.peers {
		rf.nextIdx[serverId] = lastLogIndex + 1
		rf.matchIdx[serverId] = 0
	}
	rf.snapshotSenderChs = make(map[uint64]chan int, len(rf.peers))
	for serverId := range rf.peers {
		rf.snapshotSenderChs[serverId] = make(chan int)
	}
	rf.I.Printf("intialized peers' nextIdx to %d", lastLogIndex+1)
	rf.mu.Unlock()
	go func() { // heartbeat go routine
		rf.I.Printf("sending out heartbeats")
		rf.sendHeartbeats(term)
		heartbeatTicker := time.NewTicker(rf.cfg.HeartbeatTimeout)
		for {
			select {
			case _ = <-heartbeatTicker.C:
				rf.mu.Lock()
				currentTerm := rf.getCurTermUnlocked()
				currentRole := rf.getRoleUnlocked()
				rf.mu.Unlock()
				if currentRole != Leader || currentTerm != term || rf.isStopped() {
					rf.I.Printf("no longer leader at term %d, committing suicide.", term)
					heartbeatTicker.Stop()
					return
				}
				go rf.sendHeartbeats(term)
			}
		}
	}()

	// check if me is still the leader at current term
	good := func() bool {
		rf.mu.Lock()
		currentTerm := rf.getCurTermUnlocked()
		currentRole := rf.getRoleUnlocked()
		rf.mu.Unlock()
		return currentRole == Leader && currentTerm == term && rf.isStopped() == false
	}

	go func() { //replication go routine
		snapshotSenderChs := rf.snapshotSenderChs
		for i := range snapshotSenderChs {
			go rf.snapshotSender(uint64(i), term, snapshotSenderChs[i])
		}
		rf.I.Printf("start servicing clients.")
		reps := 0
	loop:
		for good() {
			select {
			case _ = <-rf.repChan:
				reps++
			innerLoop:
				for {
					// drain the channel and replicate for all of the requests
					select {
					case _ = <-rf.repChan:
					default:
						break innerLoop
					}
				}
				if good() == false {
					break loop
				}

				rf.replicate(term)
			}
		}
		rf.I.Printf("done servicing clients in term %d, executed %d replications.", term, reps)
		for i := range snapshotSenderChs {
			close(snapshotSenderChs[i])
		}
	}()
}

// Election timer timedout without receiving any heartbeats,
// convert to candidate and start a new election.
// If this election failed to finish (either elected as a leader or
// received heartbeats from another leader) within the period of timeout,
// the election timer will soon time out again and start a new election.
// If there another sennds a heartbeat message to the server while it is in
// candidate role (which cause the server to convert to follower),
// the election will be aborted by checking the current role.
func (rf *Raft) convertToCandidate(timeout time.Duration) {
	votes := 1 // one vote from itself
	rf.mu.Lock()
	rf.setRoleUnlocked(Candidate)
	rf.setVotedForUnlocked(rf.me)
	currentTerm := rf.getCurTermUnlocked()
	newTerm := currentTerm + 1
	candidateId := rf.me
	lastLogTerm, lastLogIndex := rf.getLastLogEntryInfoUnlocked()
	// starts a new term and persists it
	rf.setTermUnlocked(newTerm)
	rf.setVotedForUnlocked(rf.me)
	rf.mu.Unlock()

	failedTimer := time.NewTimer(timeout)
	args := RequestVoteArgs{newTerm, candidateId, lastLogTerm, lastLogIndex}
	rf.I.Printf("starts new election with term %d, lastLogTerm:%d, lastLogIndex: %d", newTerm, lastLogTerm, lastLogIndex)

	type RpcReply struct {
		ok     bool
		server uint64
		reply  *RequestVoteReply
	}
	doneChan := make(chan *RpcReply)
	for server := range rf.peers {
		serverId := server
		if serverId == rf.me { // skip self
			continue
		}
		go func(ch chan *RpcReply) {
			reply := &RequestVoteReply{}
			//rf.T.Printf("sending request vote to peer%d in term %d", serverId, newTerm)
			ok := rf.sendRequestVote(serverId, args, reply)
			select {
			case ch <- &RpcReply{ok, serverId, reply}:
			case <-time.After(timeout):
				rf.T.Printf("sending request vote to peer%d in term %d timedout", serverId, newTerm)
			}
		}(doneChan)
	}

loop:
	for {
		// This handles single-node cluster leader election
		if votes > len(rf.peers)/2 {
			rf.I.Printf("got a majority of votes %d, establishing leadership in term %d, args %v", votes, newTerm, args)
			rf.convertToLeader(newTerm)
			break loop
		}
		select {
		case _ = <-failedTimer.C:
			rf.W.Printf("failed to be elected as leader in %s in term %d", timeout, newTerm)
			break loop
		case r := <-doneChan:
			if role := rf.getRole(); role != Candidate {
				rf.W.Printf("not in candidate role anymore, aborting")
				break loop
			}
			if r.ok == false {
				rf.W.Printf("rpc sendRequestVote to peer%d failed", r.server)
				continue
			}
			if r.reply.VoteGranted == true {
				votes++
				rf.I.Printf("got one vote from peer%d, now %d votes", r.server, votes)
			} else if r.reply.Term > newTerm { // there is already
				rf.I.Printf("there is already a leader in higher term %d, aborting election", r.reply.Term)
				rf.convertToFollower(r.reply.Term)
				break loop
			}
		}
	}
}

const (
	advanceCommitIdx  = 0
	applySnapshot     = 1
	stopCommitRoutine = 2
)

func (rf *Raft) commitRoutine() {
	startCommitIdx := rf.getCommitIdx()
	// return the msg to apply so that caller can send it through
	// the apply channel without holding the mutex to avoid
	// potential deadlock.
	doApplySnapshotUnlocked := func() *ApplyMsg {
		snapshot := rf.l.s.Snapshot()
		snapshotLastIdx := snapshot.Metadata.LastIdx
		snapshotLastTerm := snapshot.Metadata.LastTerm
		if snapshotLastIdx != rf.l.s.SnapshotLastIndex() || snapshotLastTerm != rf.l.s.SnapshotLastTerm() {
			rf.E.Fatalf("rf.snapshotLastIdx %v != snapshot.lastIdx %v !", rf.l.s.SnapshotLastIndex(), snapshotLastIdx)
		}
		msg := ApplyMsg{snapshotLastTerm, snapshotLastIdx, nil, true, snapshot.Data}
		oldCommitIdx := rf.commitIdx
		if rf.oldCommitIdx < snapshotLastIdx {
			rf.oldCommitIdx = snapshotLastIdx
			if rf.commitIdx < snapshotLastIdx {
				rf.commitIdx = snapshotLastIdx
			}
		}

		rf.I.Printf("applied snapshot[%d] at term %d, commitIdx %d --> %d", snapshotLastIdx, snapshotLastTerm, oldCommitIdx, rf.commitIdx)
		return &msg
	}

	processCommitUpdates := func(v int) {
		var snapshotMsg *ApplyMsg
		if v == stopCommitRoutine {
			return
		} else if v == advanceCommitIdx {
			var msgsToApply []*ApplyMsg
			rf.mu.Lock()
			oldCommitIdx := rf.oldCommitIdx + 1
			if oldCommitIdx <= rf.l.s.SnapshotLastIndex() {
				snapshotMsg = doApplySnapshotUnlocked()
				oldCommitIdx = rf.l.s.SnapshotLastIndex() + 1
			}
			newCommitIdx := rf.commitIdx
			if snapshotMsg != nil {
				msgsToApply = append(msgsToApply, snapshotMsg)
			}
			for ; oldCommitIdx <= newCommitIdx; oldCommitIdx++ {
				ents, err := rf.l.s.Entries(oldCommitIdx, oldCommitIdx)
				//Potential BUG!!!
				if err != nil {
					rf.E.Fatalf("oldCommitIdx %d snapshotLastIdx %d logs %v, err %v !!!!", oldCommitIdx, rf.l.s.SnapshotLastIndex(), rf.l, err)
				}
				entry := ents[0] // although log index starts at 1, slice's index starts from 0
				msg := ApplyMsg{entry.Term, entry.Index, entry.Data, false, nil}
				msgsToApply = append(msgsToApply, &msg)
			}
			rf.mu.Unlock()
			for _, msg := range msgsToApply {
				rf.applyCh <- *msg
			}
			// Since oldCommitIdx is used to determined the last entry applied to the state machine,
			// it MUST be updated after all ApplyMsg has been taken care of by the upper layer
			rf.mu.Lock()
			rf.oldCommitIdx = newCommitIdx
			rf.mu.Unlock()

		} else { // apply latest snapshot
			rf.mu.Lock()
			snapshotMsg = doApplySnapshotUnlocked()
			rf.mu.Unlock()
			rf.applyCh <- *snapshotMsg
		}
	}
outer:
	for {
		select {
		case v1 := <-rf.commitUpdateCh:
			updates := make([]int, 0, 2)
			updates = append(updates, v1)
			v2 := -1
		loop:
			for {
				select {
				case v2 = <-rf.commitUpdateCh: // combine updates of the same type
					if v1 != v2 {
						updates = append(updates, v2)
						break loop
					}
				default:
					break loop
				}
			}
			for _, v := range updates {
				if v == stopCommitRoutine {
					break outer
				}
				processCommitUpdates(v)
			}
		}
	}
	rf.I.Printf("commitRoutine exited, committed entries[%d-%d].", startCommitIdx, rf.commitIdx)
	rf.commitRoutineDoneCh <- true
}

func (rf *Raft) RaftStateSize() uint64 {
	if rf.persister != nil {
		return rf.persister.RaftStateSize()
	}
	return rf.l.s.LogSize()
}

//starts as a new server
func (rf *Raft) mainLoop() {
	go rf.commitRoutine()
	rf.T.Printf("raft main loop started")
forever:
	for {
		select {
		case _ = <-rf.quitCh:
			rf.T.Printf("raft is going down... ok.")
			break forever
		case _ = <-rf.elecTimer.C:
			//timedout, start a new election
			go rf.convertToCandidate(rf.resetElectionTimer(false))
		case _ = <-rf.resetElectTimerCh:
			rf.resetElectionTimer(true)
		}
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	atomic.StoreInt32(&rf.stopped, 1)
	rf.quitCh <- true
	rf.commitUpdateCh <- stopCommitRoutine
	_ = <-rf.commitRoutineDoneCh
}

func (rf *Raft) Cleanup() {
	rf.D.Printf("Cleanup getting the lock")
	rf.mu.Lock()
	rf.rpcServer.StopServer()
	err := rf.l.s.Cleanup()
	if err != nil {
		rf.E.Printf("error cleanup raft state, %v", err)
	}
	rf.mu.Unlock()
	rf.D.Printf("Cleanup releasing the lock")
}

func StartServer(cfg *Config, applyCh chan<- ApplyMsg) *Raft {
	cfg.I.Printf("raft cfg: %v", cfg)
	rf := &Raft{}
	rf.cfg = cfg
	rf.applyCh = applyCh
	rf.persister = cfg.Persister
	rf.me = cfg.Self.Id
	rf.peers = map[uint64]*PeerInfo{}
	for idx, _ := range cfg.Peers {
		rf.peers[cfg.Peers[idx].Id] = &cfg.Peers[idx]
	}
	rf.T = cfg.T
	rf.D = cfg.D
	rf.I = cfg.I
	rf.W = cfg.W
	rf.E = cfg.E

	var ss Storage
	switch cfg.StorageType {
	case "memory":
		ms := NewMemoryStorage(cfg)
		ss = ms
	case "stable":
		fs, err := NewFileStorage(cfg)
		if err != nil {
			rf.E.Panic(err)
		}
		ss = fs
	default:
		rf.E.Panic(fmt.Sprintf("unknown storage type %s, must be memory or stable.", cfg.StorageType))
	}
	rf.I.Printf("storage type: %s", cfg.StorageType)

	switch cfg.RpcType {
	case "emulated":
		rf.rpcServer = NewMockRpcServer()
		rf.rpcClient = NewMockRpcClient(cfg.RpcTypeData.(map[uint64]*mockrpc.ClientEnd))
	case "httprpc":
		rf.rpcServer = NewHttpRpcServer(&rf.cfg.Self, rf.T, rf.D, rf.I, rf.W, rf.E)
		rf.rpcClient = NewHttpRpcClient(rf.T, rf.D, rf.I, rf.W, rf.E)
	default:
		rf.E.Panic(fmt.Sprintf("unknown rpc type %s, must be emulated or httprpc.", cfg.RpcType))

	}
	rf.I.Printf("rpc type: %s", cfg.RpcType)
	err := rf.rpcServer.Register(rf)
	if err != nil {
		rf.E.Fatalf("failed to register Raft on rpc server: %v", err)
	}
	err = rf.rpcServer.StartServer()
	if err != nil {
		rf.E.Fatalf("failed to start rpc server: %v", err)
	}

	rf.l = newRaftLog(ss, cfg)
	rf.rg = rand.New(rand.NewSource(time.Now().UnixNano()))
	// Your initialization code here.
	// initialize from state persisted before a crash
	rf.commitUpdateCh = make(chan int, 1024)
	rf.commitRoutineDoneCh = make(chan bool)
	rf.readSnapshot(cfg.Persister.ReadSnapshot()) // read latest snapshot
	rf.readPersist(cfg.Persister.ReadRaftState()) // read partial logs

	rf.commitIdx = 0
	rf.oldCommitIdx = 0
	rf.setRole(Follower)
	rf.repChan = make(chan bool, 4096)
	rf.quitCh = make(chan bool, 1)
	rf.resetElectTimerCh = make(chan int, 128)
	rf.elecTimer = time.NewTimer(rf.nextElectionTimeout())
	rf.partialSnapshots = make(map[uint64]partialSnapshot)
	go rf.mainLoop()
	return rf
}
