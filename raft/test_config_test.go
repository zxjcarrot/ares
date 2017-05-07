// !build

package raft

//
// support for Raft tester.
//
// we will use the original config.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/zxjcarrot/ares/mockrpc"

	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"sync/atomic"
	"time"
)

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

type config struct {
	mu        sync.Mutex
	t         *testing.T
	net       *mockrpc.Network
	n         int
	done      int32 // tell internal threads to die
	rafts     []*Raft
	applyErr  []string // from apply channel readers
	connected []bool   // whether each server is on the net
	saved     []*Persister
	endnames  [][]string       // the port file names each sends to
	logs      []map[int][]byte // copy of each server's committed entries
}

func make_config(t *testing.T, n int, unreliable bool) *config {
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.net = mockrpc.MakeNetwork()
	cfg.n = n
	cfg.applyErr = make([]string, cfg.n)
	cfg.rafts = make([]*Raft, cfg.n)
	cfg.connected = make([]bool, cfg.n)
	cfg.saved = make([]*Persister, cfg.n)
	cfg.endnames = make([][]string, cfg.n)
	cfg.logs = make([]map[int][]byte, cfg.n)

	cfg.setunreliable(unreliable)

	cfg.net.LongDelays(true)

	// create a full set of Rafts.
	for i := 0; i < cfg.n; i++ {
		cfg.logs[i] = map[int][]byte{}
		cfg.start1(i)
	}

	// connect everyone
	for i := 0; i < cfg.n; i++ {
		cfg.connect(i)
	}

	return cfg
}

// shut down a Raft server but save its persistent state.
func (cfg *config) crash1(i int) {
	cfg.disconnect(i)
	cfg.net.DeleteServer(i) // disable client connections to the server.

	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	// a fresh persister, in case old instance
	// continues to update the Persister.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	}

	rf := cfg.rafts[i]
	if rf != nil {
		cfg.mu.Unlock()
		rf.Kill()
		cfg.mu.Lock()
		cfg.rafts[i] = nil
	}

	if cfg.saved[i] != nil {
		raftlog := cfg.saved[i].ReadRaftState()
		cfg.saved[i] = &Persister{}
		cfg.saved[i].SaveRaftState(raftlog)
	}
}

//
// start or re-start a Raft.
// if one already exists, "kill" it first.
// allocate new outgoing port file names, and a new
// state persister, to isolate previous instance of
// this server. since we cannot really kill it.
//
func (cfg *config) start1(i int) {
	cfg.crash1(i)

	// a fresh set of outgoing ClientEnd names.
	// so that old crashed instance's ClientEnds can't send.
	cfg.endnames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		cfg.endnames[i][j] = randstring(20)
	}

	// a fresh set of ClientEnds.
	ends := make([]*mockrpc.ClientEnd, cfg.n)
	endsMap := map[uint64]*mockrpc.ClientEnd{}
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endnames[i][j])
		endsMap[uint64(j)] = ends[j]
		cfg.net.Connect(cfg.endnames[i][j], j)
	}

	cfg.mu.Lock()

	// a fresh persister, so old instance doesn't overwrite
	// new instance's persisted state.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	} else {
		cfg.saved[i] = MakePersister()
	}

	cfg.mu.Unlock()

	// listen to messages from Raft indicating newly committed messages.
	applyCh := make(chan ApplyMsg)
	go func() {
		for m := range applyCh {
			err_msg := ""
			if m.UseSnapshot {
				// ignore the snapshot
			} else if v, ok := (m.Command).([]byte); ok {
				cfg.mu.Lock()
				for j := 0; j < len(cfg.logs); j++ {
					if old, oldok := cfg.logs[j][int(m.Index)]; oldok && SameByteSlice(old, v) == false {
						// some server has already committed a different value for this entry!
						err_msg = fmt.Sprintf("commit index=%v server=%v %v != server=%v %v",
							m.Index, i, m.Command, j, old)
					}
				}
				_, prevok := cfg.logs[i][int(m.Index-1)]
				cfg.logs[i][int(m.Index)] = v
				cfg.mu.Unlock()

				if m.Index > 1 && prevok == false {
					err_msg = fmt.Sprintf("server %v apply out of order %v", i, m.Index)
				}
			} else {
				err_msg = fmt.Sprintf("committed command %v is not an int", m.Command)
			}

			if err_msg != "" {
				log.Fatalf("apply error: %v\n", err_msg)
				cfg.applyErr[i] = err_msg
				// keep reading after error so that Raft doesn't block
				// holding locks...
			}
		}
	}()

	rf := StartTestServer(endsMap, uint64(i), cfg.saved[i], applyCh)

	cfg.mu.Lock()
	cfg.rafts[i] = rf
	cfg.mu.Unlock()

	svc := mockrpc.MakeService(rf)
	srv := mockrpc.MakeServer()
	srv.AddService(svc)
	cfg.net.AddServer(i, srv)
}

func (cfg *config) cleanup() {
	for i := 0; i < len(cfg.rafts); i++ {
		if cfg.rafts[i] != nil {
			cfg.rafts[i].Kill()
			cfg.rafts[i].Cleanup()
		}
	}
	atomic.StoreInt32(&cfg.done, 1)
}

// attach server i to the net.
func (cfg *config) connect(i int) {
	// fmt.Printf("connect(%d)\n", i)

	cfg.connected[i] = true

	// outgoing ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.connected[j] {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, true)
		}
	}

	// incoming ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.connected[j] {
			endname := cfg.endnames[j][i]
			cfg.net.Enable(endname, true)
		}
	}
}

// detach server i from the net.
func (cfg *config) disconnect(i int) {
	// fmt.Printf("disconnect(%d)\n", i)

	cfg.connected[i] = false

	// outgoing ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.endnames[i] != nil {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, false)
		}
	}

	// incoming ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.endnames[j] != nil {
			endname := cfg.endnames[j][i]
			cfg.net.Enable(endname, false)
		}
	}
}

func (cfg *config) rpcCount(server int) int {
	return cfg.net.GetCount(server)
}

func (cfg *config) setunreliable(unrel bool) {
	cfg.net.Reliable(!unrel)
}

func (cfg *config) setlongreordering(longrel bool) {
	cfg.net.LongReordering(longrel)
}

// check that there's exactly one leader.
// try a few times in case re-elections are needed.
func (cfg *config) checkOneLeader() int {
	for iters := 0; iters < 10; iters++ {
		time.Sleep(500 * time.Millisecond)
		leaders := make(map[int][]int)
		for i := 0; i < cfg.n; i++ {
			if cfg.connected[i] {
				if t, leader := cfg.rafts[i].GetState(); leader {
					leaders[int(t)] = append(leaders[int(t)], i)
				}
			}
		}

		lastTermWithLeader := -1
		for t, leaders := range leaders {
			if len(leaders) > 1 {
				cfg.t.Fatalf("term %d has %d (>1) leaders", t, len(leaders))
			}
			if t > lastTermWithLeader {
				lastTermWithLeader = t
			}
		}

		if len(leaders) != 0 {
			return leaders[lastTermWithLeader][0]
		}
	}
	cfg.t.Fatalf("expected one leader, got none")
	return -1
}

// check that everyone agrees on the term.
func (cfg *config) checkTerms() int {
	term := -1
	for i := 0; i < cfg.n; i++ {
		if cfg.connected[i] {
			xterm, _ := cfg.rafts[i].GetState()
			if term == -1 {
				term = int(xterm)
			} else if term != int(xterm) {
				cfg.t.Fatalf("servers disagree on term")
			}
		}
	}
	return term
}

// check that there's no leader
func (cfg *config) checkNoLeader() {
	for i := 0; i < cfg.n; i++ {
		if cfg.connected[i] {
			_, is_leader := cfg.rafts[i].GetState()
			if is_leader {
				cfg.t.Fatalf("expected no leader, but %v claims to be leader", i)
			}
		}
	}
}

// how many servers think a log entry is committed?
func (cfg *config) nCommitted(index int) (int, interface{}) {
	count := 0
	cmd := []byte(strconv.Itoa(-1))
	for i := 0; i < len(cfg.rafts); i++ {
		if cfg.applyErr[i] != "" {
			cfg.t.Fatal(cfg.applyErr[i])
		}

		cfg.mu.Lock()
		cmd1, ok := cfg.logs[i][index]
		cfg.mu.Unlock()

		if ok {
			if count > 0 && SameByteSlice(cmd, cmd1) == false {
				cfg.t.Fatalf("committed values do not match: index %v, %v, %v\n",
					index, cmd, cmd1)
			}
			count += 1
			cmd = cmd1
		}
	}
	return count, cmd
}

// wait for at least n servers to commit.
// but don't wait forever.
func (cfg *config) wait(index int, n int, startTerm int) interface{} {
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		nd, _ := cfg.nCommitted(index)
		if nd >= n {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if startTerm > -1 {
			for _, r := range cfg.rafts {
				if t, _ := r.GetState(); int(t) > startTerm {
					// someone has moved on
					// can no longer guarantee that we'll "win"
					return -1
				}
			}
		}
	}
	nd, cmd := cfg.nCommitted(index)
	if nd < n {
		cfg.t.Fatalf("only %d decided for index %d; wanted %d\n",
			nd, index, n)
	}
	return cmd
}

// do a complete agreement.
// it might choose the wrong leader initially,
// and have to re-submit after giving up.
// entirely gives up after about 10 seconds.
// indirectly checks that the servers agree on the
// same value, since nCommitted() checks this,
// as do the threads that read from applyCh.
// returns index.
func (cfg *config) one(cmd int, expectedServers int) int {
	cmtBytes := []byte(strconv.Itoa(cmd))
	t0 := time.Now()
	starts := 0
	for time.Since(t0).Seconds() < 10 {
		// try all the servers, maybe one is the leader.
		index := -1
		for si := 0; si < cfg.n; si++ {
			starts = (starts + 1) % cfg.n
			var rf *Raft
			cfg.mu.Lock()
			if cfg.connected[starts] {
				rf = cfg.rafts[starts]
			}
			cfg.mu.Unlock()
			if rf != nil {
				index1, _, ok := rf.Start(cmtBytes)
				if ok {
					index = int(index1)
					break
				}
			}
		}

		if index != -1 {
			// somebody claimed to be the leader and to have
			// submitted our command; wait a while for agreement.
			t1 := time.Now()
			for time.Since(t1).Seconds() < 2 {
				nd, cmd1 := cfg.nCommitted(index)
				if nd > 0 && nd >= expectedServers {
					// committed
					if cmd2, ok := cmd1.([]byte); ok && SameByteSlice(cmd2, cmtBytes) {
						// and it was the command we submitted.
						return index
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	cfg.t.Fatalf("one(%v) failed to reach agreement", cmd)
	return -1
}

func setupLoggers(cfg *Config) {
	dest := ioutil.Discard

	prefix := fmt.Sprintf("RaftCore id[%d] TRACE: ", cfg.Self.Id)
	if cfg.Verbosity <= VerbosityTrace {
		dest = os.Stdout
	}
	cfg.T = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)

	prefix = fmt.Sprintf("RaftCore id[%d] DEBUG: ", cfg.Self.Id)
	if cfg.Verbosity <= VerbosityDebug {
		dest = os.Stdout
	}
	cfg.D = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)

	prefix = fmt.Sprintf("RaftCore id[%d] INFO: ", cfg.Self.Id)
	if cfg.Verbosity <= VerbosityInfo {
		dest = os.Stdout
	}
	cfg.I = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)

	prefix = fmt.Sprintf("RaftCore id[%d] WARNING: ", cfg.Self.Id)
	if cfg.Verbosity <= VerbosityWarning {
		dest = os.Stdout
	}
	cfg.W = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)

	prefix = fmt.Sprintf("RaftCore id[%d] ERROR: ", cfg.Self.Id)
	if cfg.Verbosity <= VerbosityWarning {
		dest = os.Stdout
	}
	cfg.E = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartTestServer(peerMap map[uint64]*mockrpc.ClientEnd, me uint64,
	persister *Persister, applyCh chan<- ApplyMsg) *Raft {
	log.SetFlags(log.LstdFlags | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	peers := []PeerInfo{}
	for id, _ := range peerMap {
		host := "localhost"
		port := 1234 + int(id)
		raw := host + ":" + strconv.Itoa(port) + ":" + strconv.Itoa(int(id))
		peer := PeerInfo{host, port, id, raw}
		peers = append(peers, peer)
	}
	cfg := &Config{
		StorageType:          "stable",
		RpcType:              "emulated",
		RpcTypeData:          peerMap,
		WalDir:               fmt.Sprintf("wal%d", me),
		SegmentFileSize:      1024 * 1024,
		Self:                 PeerInfo{Host: "localhost", Port: 1234 + int(me), Id: me, Raw: strconv.Itoa(int(me))},
		Peers:                peers,
		Persister:            persister,
		Verbosity:            VerbosityInfo,
		HeartbeatTimeout:     DefaultHeartbeatTimeout,
		ElectionTimeoutLower: DefaultElectionTimeoutLower,
		ElectionTimeoutUpper: DefaultElectionTimeoutUpper,
		SnapshotChunkSize:    DefaultSnapshotChunkSize,
		StableStateFilename:  DefaultStableStateFilename,
		ReplicationUnit:      DefaultReplicationUnit,
	}
	setupLoggers(cfg)

	cfg.I.Printf("peers: %v", peers)

	return StartServer(cfg, applyCh)
}
