package shardkv

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/zxjcarrot/ares/mockrpc"
	"github.com/zxjcarrot/ares/raft"
	"github.com/zxjcarrot/ares/shardmaster"

	crand "crypto/rand"
	"encoding/base64"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"time"
)

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n] + ","
}

// Randomize server handles
func random_handles(kvh []*mockrpc.ClientEnd) []*mockrpc.ClientEnd {
	sa := make([]*mockrpc.ClientEnd, len(kvh))
	copy(sa, kvh)
	for i := range sa {
		j := rand.Intn(i + 1)
		sa[i], sa[j] = sa[j], sa[i]
	}
	return sa
}

type group struct {
	gid       int
	servers   []*ShardKV
	saved     []*raft.Persister
	endnames  [][]string
	mendnames [][]string
}

type config struct {
	mu  sync.Mutex
	t   *testing.T
	net *mockrpc.Network

	nmasters      int
	masterservers []*shardmaster.ShardMaster
	mck           *shardmaster.Clerk

	ngroups int
	n       int // servers per k/v group
	groups  []*group

	clerks       map[*Clerk][]string
	nextClientId int
	maxraftstate int
}

func (cfg *config) CleanupGroup(gi int) {
	for i := 0; i < cfg.n; i++ {
		cfg.ShutdownServerAndCleanup(gi, i, true)
	}
}
func (cfg *config) cleanup() {
	for gi := 0; gi < cfg.ngroups; gi++ {
		cfg.CleanupGroup(gi)
	}
	for i := 0; i < len(cfg.masterservers); i++ {
		cfg.masterservers[i].Kill()
		cfg.masterservers[i].Cleanup()
	}
}

// check that no server's log is too big.
func (cfg *config) checklogs() {
	for gi := 0; gi < cfg.ngroups; gi++ {
		for i := 0; i < cfg.n; i++ {
			raft := int(cfg.groups[gi].saved[i].RaftStateSize())
			if cfg.maxraftstate >= 0 && raft > 2*cfg.maxraftstate {
				cfg.t.Fatalf("persister.RaftStateSize() %v, but maxraftstate %v",
					raft, cfg.maxraftstate)
			}
		}
	}
}

// master server name for mockrpc.
func (cfg *config) mastername(i int) string {
	return "master" + strconv.Itoa(i)
}

// shard server name for mockrpc.
// i'th server of group gid.
func (cfg *config) servername(gid int, i int) string {
	return "server-" + strconv.Itoa(gid) + "-" + strconv.Itoa(i)
}

func (cfg *config) makeClient() *Clerk {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	// ClientEnds to talk to master service.
	ends := make([]*mockrpc.ClientEnd, cfg.nmasters)
	endnames := make([]string, cfg.n)
	for j := 0; j < cfg.nmasters; j++ {
		endnames[j] = randstring(20)
		ends[j] = cfg.net.MakeEnd(endnames[j])
		cfg.net.Connect(endnames[j], cfg.mastername(j))
		cfg.net.Enable(endnames[j], true)
	}

	ck := MakeMockClerk(ends, func(servername string) *mockrpc.ClientEnd {
		name := randstring(20)
		end := cfg.net.MakeEnd(name)
		cfg.net.Connect(name, servername)
		cfg.net.Enable(name, true)
		return end
	})
	cfg.clerks[ck] = endnames
	cfg.nextClientId++
	return ck
}

func (cfg *config) deleteClient(ck *Clerk) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	v := cfg.clerks[ck]
	for i := 0; i < len(v); i++ {
		os.Remove(v[i])
	}
	delete(cfg.clerks, ck)
}

func (cfg *config) ShutdownServerAndCleanup(gi int, i int, clean bool) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	gg := cfg.groups[gi]

	// prevent this server from sending
	for j := 0; j < len(gg.servers); j++ {
		name := gg.endnames[i][j]
		cfg.net.Enable(name, false)
	}
	for j := 0; j < len(gg.mendnames[i]); j++ {
		name := gg.mendnames[i][j]
		cfg.net.Enable(name, false)
	}

	// disable client connections to the server.
	// it's important to do this before creating
	// the new Persister in saved[i], to avoid
	// the possibility of the server returning a
	// positive reply to an Append but persisting
	// the result in the superseded Persister.
	cfg.net.DeleteServer(cfg.servername(gg.gid, i))
	time.Sleep(500 * time.Millisecond)
	// a fresh persister, in case old instance
	// continues to update the Persister.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if gg.saved[i] != nil {
		gg.saved[i] = gg.saved[i].Copy()
	}

	kv := gg.servers[i]
	if kv != nil {
		cfg.mu.Unlock()
		kv.Kill()
		if clean == true {
			//debug.PrintStack()
			kv.Cleanup()
		}
		cfg.mu.Lock()
		gg.servers[i] = nil
	}
}

// Shutdown i'th server of gi'th group, by isolating it
func (cfg *config) ShutdownServer(gi int, i int) {
	cfg.ShutdownServerAndCleanup(gi, i, false)
}

func (cfg *config) ShutdownGroup(gi int) {
	for i := 0; i < cfg.n; i++ {
		cfg.ShutdownServer(gi, i)
	}
}

// start i'th server in gi'th group
func (cfg *config) StartServer(gi int, i int) {
	cfg.mu.Lock()

	gg := cfg.groups[gi]

	// a fresh set of outgoing ClientEnd names
	// to talk to other servers in this group.
	gg.endnames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		gg.endnames[i][j] = randstring(20)
	}

	// and the connections to other servers in this group.
	ends := make([]*mockrpc.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(gg.endnames[i][j])
		cfg.net.Connect(gg.endnames[i][j], cfg.servername(gg.gid, j))
		cfg.net.Enable(gg.endnames[i][j], true)
	}

	// ends to talk to shardmaster service
	mends := make([]*mockrpc.ClientEnd, cfg.nmasters)
	gg.mendnames[i] = make([]string, cfg.nmasters)
	for j := 0; j < cfg.nmasters; j++ {
		gg.mendnames[i][j] = randstring(20)
		mends[j] = cfg.net.MakeEnd(gg.mendnames[i][j])
		cfg.net.Connect(gg.mendnames[i][j], cfg.mastername(j))
		cfg.net.Enable(gg.mendnames[i][j], true)
	}

	// a fresh persister, so old instance doesn't overwrite
	// new instance's persisted state.
	// give the fresh persister a copy of the old persister's
	// state, so that the spec is that we pass StartKVServer()
	// the last persisted state.
	if gg.saved[i] != nil {
		gg.saved[i] = gg.saved[i].Copy()
	} else {
		gg.saved[i] = raft.MakePersister()
	}
	cfg.mu.Unlock()

	gg.servers[i] = StartTestServer(ends, i, gg.saved[i], cfg.maxraftstate,
		gg.gid, mends,
		func(servername string) *mockrpc.ClientEnd {
			name := randstring(20)
			end := cfg.net.MakeEnd(name)
			cfg.net.Connect(name, servername)
			cfg.net.Enable(name, true)
			return end
		})

	kvsvc := mockrpc.MakeService(gg.servers[i])
	rfsvc := mockrpc.MakeService(gg.servers[i].rf)
	srv := mockrpc.MakeServer()
	srv.AddService(kvsvc)
	srv.AddService(rfsvc)
	cfg.net.AddServer(cfg.servername(gg.gid, i), srv)
}

func (cfg *config) StartGroup(gi int) {
	for i := 0; i < cfg.n; i++ {
		cfg.StartServer(gi, i)
	}
}

func (cfg *config) StartMasterServer(i int) {
	// ClientEnds to talk to other master replicas.
	ends := make([]*mockrpc.ClientEnd, cfg.nmasters)
	for j := 0; j < cfg.nmasters; j++ {
		endname := randstring(20)
		ends[j] = cfg.net.MakeEnd(endname)
		cfg.net.Connect(endname, cfg.mastername(j))
		cfg.net.Enable(endname, true)
	}

	p := raft.MakePersister()

	cfg.masterservers[i] = shardmaster.StartTestServer(ends, uint64(i), p)

	msvc := mockrpc.MakeService(cfg.masterservers[i])
	rfsvc := mockrpc.MakeService(cfg.masterservers[i].Raft())
	srv := mockrpc.MakeServer()
	srv.AddService(msvc)
	srv.AddService(rfsvc)
	cfg.net.AddServer(cfg.mastername(i), srv)
}

func (cfg *config) shardclerk() *shardmaster.Clerk {
	// ClientEnds to talk to master service.
	ends := make([]*mockrpc.ClientEnd, cfg.nmasters)
	for j := 0; j < cfg.nmasters; j++ {
		name := randstring(20)
		ends[j] = cfg.net.MakeEnd(name)
		cfg.net.Connect(name, cfg.mastername(j))
		cfg.net.Enable(name, true)
	}

	return shardmaster.MakeMockRpcClerk(ends)
}

// tell the shardmaster that a group is joining.
func (cfg *config) join(gi int) {
	cfg.joinm([]int{gi})
}

func (cfg *config) joinm(gis []int) {
	m := make(map[int][]string, len(gis))
	for _, g := range gis {
		gid := cfg.groups[g].gid
		servernames := make([]string, cfg.n)
		for i := 0; i < cfg.n; i++ {
			servernames[i] = cfg.servername(gid, i)
		}
		m[gid] = servernames
	}
	cfg.mck.Join(m)
}

// tell the shardmaster that a group is leaving.
func (cfg *config) leave(gi int) {
	cfg.leavem([]int{gi})
}

func (cfg *config) leavem(gis []int) {
	gids := make([]int, 0, len(gis))
	for _, g := range gis {
		gids = append(gids, cfg.groups[g].gid)
	}
	cfg.mck.Leave(gids)
}

func make_config(t *testing.T, n int, unreliable bool, maxraftstate int) *config {
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.maxraftstate = maxraftstate
	cfg.net = mockrpc.MakeNetwork()

	// master
	cfg.nmasters = 3
	cfg.masterservers = make([]*shardmaster.ShardMaster, cfg.nmasters)
	for i := 0; i < cfg.nmasters; i++ {
		cfg.StartMasterServer(i)
	}
	cfg.mck = cfg.shardclerk()

	cfg.ngroups = 3
	cfg.groups = make([]*group, cfg.ngroups)
	cfg.n = n
	for gi := 0; gi < cfg.ngroups; gi++ {
		gg := &group{}
		cfg.groups[gi] = gg
		gg.gid = 100 + gi
		gg.servers = make([]*ShardKV, cfg.n)
		gg.saved = make([]*raft.Persister, cfg.n)
		gg.endnames = make([][]string, cfg.n)
		gg.mendnames = make([][]string, cfg.nmasters)
		for i := 0; i < cfg.n; i++ {
			cfg.StartServer(gi, i)
		}
	}

	cfg.clerks = make(map[*Clerk][]string)
	cfg.nextClientId = cfg.n + 1000 // client ids start 1000 above the highest serverid

	cfg.net.Reliable(!unreliable)

	return cfg
}
func setupLoggers(kvCfg *ShardKVConfig) {
	dest := ioutil.Discard

	prefix := fmt.Sprintf("ShardKV id %d TRACE: ", kvCfg.RaftCfg.Self.Id)
	if kvCfg.Verbosity <= VerbosityTrace {
		dest = os.Stdout
	}
	kvCfg.T = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)

	prefix = fmt.Sprintf("ShardKV id %d DEBUG: ", kvCfg.RaftCfg.Self.Id)
	if kvCfg.Verbosity <= VerbosityDebug {
		dest = os.Stdout
	}
	kvCfg.D = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)

	prefix = fmt.Sprintf("ShardKV id %d INFO: ", kvCfg.RaftCfg.Self.Id)
	if kvCfg.Verbosity <= VerbosityInfo {
		dest = os.Stdout
	}
	kvCfg.I = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)

	prefix = fmt.Sprintf("ShardKV id %d WARNING: ", kvCfg.RaftCfg.Self.Id)
	if kvCfg.Verbosity <= VerbosityWarning {
		dest = os.Stdout
	}
	kvCfg.W = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)

	prefix = fmt.Sprintf("ShardKV id %d ERROR: ", kvCfg.RaftCfg.Self.Id)
	if kvCfg.Verbosity <= VerbosityWarning {
		dest = os.Stdout
	}
	kvCfg.E = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a mockrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartTestServer(servers []*mockrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*mockrpc.ClientEnd, makeEnd func(string) *mockrpc.ClientEnd) *ShardKV {
	peerMap := map[uint64]*mockrpc.ClientEnd{}
	for i := range servers {
		peerMap[uint64(gid*1000+i)] = servers[i]
	}
	log.SetFlags(log.LstdFlags | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	peers := []raft.PeerInfo{}
	for id, _ := range peerMap {
		host := "localhost"
		port := 1234 + int(id)
		raw := host + ":" + strconv.Itoa(port) + ":" + strconv.Itoa(int(id))
		peer := raft.PeerInfo{Host: host, Port: port, Id: id, Raw: raw}
		peers = append(peers, peer)
	}
	cfg := &ShardKVConfig{
		RaftCfg: &raft.Config{
			StorageType:          "stable",
			RpcType:              "emulated",
			RpcTypeData:          peerMap,
			WalDir:               fmt.Sprintf("wal%d", uint64(gid*1000+me)),
			SegmentFileSize:      1024 * 1024,
			Self:                 raft.PeerInfo{Host: "localhost", Port: 1234 + int(me), Id: uint64(gid*1000 + me), Raw: strconv.Itoa(gid*1000 + me)},
			Peers:                peers,
			Persister:            persister,
			Verbosity:            raft.VerbosityDebug,
			HeartbeatTimeout:     raft.DefaultHeartbeatTimeout,
			ElectionTimeoutLower: raft.DefaultElectionTimeoutLower,
			ElectionTimeoutUpper: raft.DefaultElectionTimeoutUpper,
			SnapshotChunkSize:    raft.DefaultSnapshotChunkSize,
			StableStateFilename:  raft.DefaultStableStateFilename,
			ReplicationUnit:      raft.DefaultReplicationUnit,
		},
		MaxRaftStateSize:    maxraftstate,
		Gid:                 gid,
		RpcType:             "emulated",
		RpcTypeData:         peerMap,
		MasterRpcType:       "emulated",
		MasterRpcTypeData:   masters,
		MakeEnd:             makeEnd,
		NShards:             shardmaster.DefaultNShards,
		Verbosity:           VerbosityDebug,
		SnapshotChkInterval: DefaultSnapshotChkInterval,
		CfgUpdateInterval:   DefaultConfigUpdateInterval,
		SnapshotThresh:      DefaultSnapshottingThresholdFactor,
	}
	setupLoggers(cfg)
	cfg.RaftCfg.T = cfg.T
	cfg.RaftCfg.D = cfg.D
	cfg.RaftCfg.I = cfg.I
	cfg.RaftCfg.W = cfg.W
	cfg.RaftCfg.E = cfg.E

	return StartServer(cfg)
}
