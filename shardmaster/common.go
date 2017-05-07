package shardmaster

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/zxjcarrot/ares/mockrpc"
	"github.com/zxjcarrot/ares/raft"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// A GID is a replica group ID. GIDs must be uniqe and > 0.
// Once a GID joins, and leaves, it should never join again.
//
// You will need to add fields to the RPC arguments.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimedout    = "ErrTimedout"
	ErrNotExist    = "ErrNotExist"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	ReqID   int64
	AckID   int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs  []int
	ReqID int64
	AckID int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	ReqID int64
	AckID int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num   int // desired config number
	ReqID int64
	AckID int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func GobJoinArgs(args JoinArgs) []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(args)
	return w.Bytes()
}

func UngobJoinArgs(b []byte) *JoinArgs {
	args := JoinArgs{}
	e := gob.NewDecoder(bytes.NewBuffer(b))
	e.Decode(&args)
	return &args
}
func GobLeaveArgs(args LeaveArgs) []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(args)
	return w.Bytes()
}
func UngobLeaveArgs(b []byte) LeaveArgs {
	args := LeaveArgs{}
	e := gob.NewDecoder(bytes.NewBuffer(b))
	e.Decode(&args)
	return args
}
func GobMoveArgs(args MoveArgs) []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(args)
	return w.Bytes()
}
func UngobMoveArgs(b []byte) MoveArgs {
	args := MoveArgs{}
	e := gob.NewDecoder(bytes.NewBuffer(b))
	e.Decode(&args)
	return args
}
func GobQueryArgs(args QueryArgs) []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(args)
	return w.Bytes()
}
func UngobQueryArgs(b []byte) QueryArgs {
	args := QueryArgs{}
	e := gob.NewDecoder(bytes.NewBuffer(b))
	e.Decode(&args)
	return args
}

type ShardConfigBalancer struct {
	cfg Config
}
type pair struct {
	group  int
	indice []int
}

func CopyConfig(cfg Config) Config {
	groups := map[int][]string{}
	for k, v := range cfg.Groups {
		groups[k] = v
	}
	cfg.Groups = groups
	return cfg
}
func NewShardConfigBalancer(oldCfg Config, newConfigNum int) *ShardConfigBalancer {
	// make a copy of old config's group map
	cfg := CopyConfig(oldCfg)
	cfg.Num = newConfigNum
	return &ShardConfigBalancer{cfg}
}

type sortBy func(pair, pair) bool

func (sb *ShardConfigBalancer) sortGSPairs(cmp sortBy) []pair {
	gscount := map[int][]int{}

	var pairs []pair
	for g, _ := range sb.cfg.Groups {
		gscount[g] = []int{}
	}
	for i := range sb.cfg.Shards {
		gscount[sb.cfg.Shards[i]] = append(gscount[sb.cfg.Shards[i]], i)
	}
	for k, v := range gscount {
		pairs = append(pairs, pair{k, v})
	}
	// buble sort
	for i := len(pairs) - 1; i > 0; i-- {
		for j := 0; j < i; j++ {
			if !cmp(pairs[j], pairs[j+1]) {
				t := pairs[j]
				pairs[j] = pairs[j+1]
				pairs[j+1] = t
			}
		}
	}
	return pairs
}

// Balance algorithm for addition of a single group:
// 	Let n = NShards / (# of groups after adding the group) (rounded down).
// 	Now n is the # of shards needed to transfer to the newly added group.
// 	While # of transferred shards < n:
// 		1. compute the group gmax with the maximum # of shards
// 		2. assign one of gmax's shards to gid
//      3. go to 1
func (sb *ShardConfigBalancer) AddSingleGroup(gid int, servers []string) {
	if _, ok := sb.cfg.Groups[gid]; ok {
		return
	}
	ngroups := len(sb.cfg.Groups) + 1

	n := NShards / ngroups
loop:
	// TODO optimize the maximum element retrieval by implementing a heap
	for cnt := 0; cnt < n; cnt++ {
		pairs := sb.sortGSPairs(func(p1, p2 pair) bool {
			// move group 0 to the head
			if p1.group == 0 {
				return true
			} else if p2.group == 0 {
				return false
			}
			return len(p1.indice) > len(p2.indice) || len(p1.indice) == len(p2.indice) && p1.group < p2.group
		})
		for _, p := range pairs {
			if gid == p.group {
				continue
			}
			if len(p.indice) > 0 {
				idx := p.indice[len(p.indice)-1]
				sb.cfg.Shards[idx] = gid // assign to the new group
			} else {
				break loop
			}
			break
		}
	}
	sb.cfg.Groups[gid] = servers
}

// Balance algorithm for deletion of a single group:
// 	For every removed shard s:
//  	1. compute the group gmin with the minimum # of shards
// 		2. assign s to gmin
//      3. go to 1
func (sb *ShardConfigBalancer) RemoveSingleGroup(gid int) {
	if _, ok := sb.cfg.Groups[gid]; !ok {
		return
	}
	var indiceToRemove []int
	for i := range sb.cfg.Shards {
		if sb.cfg.Shards[i] == gid {
			indiceToRemove = append(indiceToRemove, i)
			sb.cfg.Shards[i] = 0
		}
	}
	delete(sb.cfg.Groups, gid)
	s := len(indiceToRemove)
	// TODO optimize the minimum element retrieval by implementing a heap
	for ; s > 0; s-- {
		pairs := sb.sortGSPairs(func(p1, p2 pair) bool {
			// move group 0 to the tail
			if p1.group == 0 {
				return false
			} else if p2.group == 0 {
				return true
			}
			return len(p1.indice) < len(p2.indice) || len(p1.indice) == len(p2.indice) && p1.group < p2.group
		})
		p := pairs[0]
		// assign the removed shard to the group with the minimum # of shards
		sb.cfg.Shards[indiceToRemove[s-1]] = p.group
	}

}

func (sb *ShardConfigBalancer) MoveShard(sid, gid int) {
	cgid := sb.cfg.Shards[sid]
	if cgid == gid {
		return
	}
	sb.cfg.Shards[sid] = gid
}
func (sb *ShardConfigBalancer) AddGroups(grps map[int][]string) {
	for gid, servers := range grps {
		sb.AddSingleGroup(gid, servers)
	}
}
func (sb *ShardConfigBalancer) RemoveGroups(gids []int) {
	for _, gid := range gids {
		sb.RemoveSingleGroup(gid)
	}
}
func (sb *ShardConfigBalancer) GetConfig() Config {
	return sb.cfg
}

func setupLoggers(mCfg *ShardMasterConfig) {
	dest := ioutil.Discard

	prefix := fmt.Sprintf("ShardMaster id %d TRACE: ", mCfg.RaftCfg.Self.Id)
	if mCfg.Verbosity <= VerbosityTrace {
		dest = os.Stdout
	}
	mCfg.T = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)

	prefix = fmt.Sprintf("ShardMaster id %d DEBUG: ", mCfg.RaftCfg.Self.Id)
	if mCfg.Verbosity <= VerbosityDebug {
		dest = os.Stdout
	}
	mCfg.D = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)

	prefix = fmt.Sprintf("ShardMaster id %d INFO: ", mCfg.RaftCfg.Self.Id)
	if mCfg.Verbosity <= VerbosityInfo {
		dest = os.Stdout
	}
	mCfg.I = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)

	prefix = fmt.Sprintf("ShardMaster id %d WARNING: ", mCfg.RaftCfg.Self.Id)
	if mCfg.Verbosity <= VerbosityWarning {
		dest = os.Stdout
	}
	mCfg.W = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)

	prefix = fmt.Sprintf("ShardMaster id %d ERROR: ", mCfg.RaftCfg.Self.Id)
	if mCfg.Verbosity <= VerbosityWarning {
		dest = os.Stdout
	}
	mCfg.E = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartTestServer(servers []*mockrpc.ClientEnd, me uint64, persister *raft.Persister) *ShardMaster {
	peerMap := map[uint64]*mockrpc.ClientEnd{}
	for i := range servers {
		peerMap[uint64(i)] = servers[i]
	}
	log.SetFlags(log.LstdFlags | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	peers := []raft.PeerInfo{}
	for id, _ := range peerMap {
		host := "localhost"
		port := 4567 + int(id)
		raw := host + ":" + strconv.Itoa(port) + ":" + strconv.Itoa(int(id))
		peer := raft.PeerInfo{Host: host, Port: port, Id: id, Raw: raw}
		peers = append(peers, peer)
	}
	cfg := &ShardMasterConfig{
		RaftCfg: &raft.Config{
			StorageType:          "stable",
			RpcType:              "emulated",
			RpcTypeData:          peerMap,
			WalDir:               fmt.Sprintf("wal%d", uint64(me)),
			SegmentFileSize:      1024 * 1024,
			Self:                 raft.PeerInfo{Host: "localhost", Port: 1234 + int(me), Id: me, Raw: strconv.Itoa(int(me))},
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
		MaxRaftStateSize:    DefaultMaxRaftStateSize,
		RpcType:             "emulated",
		RpcTypeData:         peerMap,
		Persister:           persister,
		NShards:             DefaultNShards,
		SnapshotChkInterval: DefaultSnapshotChkInterval,
		SnapshotThresh:      DefaultSnapshottingThresholdFactor,
		Verbosity:           VerbosityDebug,
	}
	setupLoggers(cfg)
	cfg.RaftCfg.T = cfg.T
	cfg.RaftCfg.D = cfg.D
	cfg.RaftCfg.I = cfg.I
	cfg.RaftCfg.W = cfg.W
	cfg.RaftCfg.E = cfg.E

	return StartServer(cfg)
}
