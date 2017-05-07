package raft

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// The description of a peer, raw string format: "self = host:port:id"
type PeerInfo struct {
	Host string
	Port int
	Id   uint64
	Raw  string // host:port:id
}

//configuration for a Raft peer
type Config struct {
	StorageType          string        // underlying storage for the log, format: "storageType = memory | stable". By default stable storage.
	RpcType              string        // rpc server type , format "rpcType = emulated | httprpc"
	RpcTypeData          interface{}   // used by emulated rpc
	Self                 PeerInfo      // local peer
	WalDir               string        // directory containing data of the stable storage, format: "walDir = path". By default <working directory>/wal.
	StableStateFilename  string        // file to store raft's stable state, format: "ssFilename = fielname". By default stable.state.
	Peers                []PeerInfo    // the descrptions of the entire cluster, format: "peers = host1:port1:id1,host2:port2:id2..."
	HeartbeatTimeout     time.Duration // heartbeat interval, format: "hbInterval = interval"
	ElectionTimeoutUpper time.Duration // election timeout range, format: "elecTimeoutRange = lower-upper"
	ElectionTimeoutLower time.Duration
	SnapshotChunkSize    int // the chunk size of snapshot in ecah InstallSnapshot RPC call.
	SegmentFileSize      int64
	ReplicationUnit      uint64
	Verbosity            int
	Persister            *Persister
	T                    *log.Logger
	D                    *log.Logger
	I                    *log.Logger
	W                    *log.Logger
	E                    *log.Logger
}

const (
	DefaultElectionTimeoutUpper time.Duration = time.Duration(300) * time.Millisecond
	DefaultElectionTimeoutLower               = time.Duration(150) * time.Millisecond
	DefaultHeartbeatTimeout                   = time.Duration(50) * time.Millisecond
	DefaultBatchTimeout                       = time.Duration(10) * time.Millisecond
)

const (
	DefaultBatchCount        = 10
	DefaultSnapshotChunkSize = 2097152 // in bytes, 2MB
	DefaultReplicationUnit   = 128
	Unvoted                  = math.MaxUint64
	DefaultPort              = 22445
)

const (
	DefaultSegmentFileSize     = 16 * 1024 * 1024 // 16MB
	DefaultStableStateFilename = "stable.state"
)

const (
	VerbosityAll     = 0
	VerbosityTrace   = 1
	VerbosityDebug   = 2
	VerbosityInfo    = 3
	VerbosityWarning = 4
	VerbosityError   = 5
)

func ParsePeerInfo(raw string) (pi PeerInfo, err error) {
	raw = strings.Trim(raw, "\t ")
	parts := strings.Split(raw, ":")
	if len(parts) != 3 {
		return pi, errors.New("expecting 'host:port:id'")
	}
	host := parts[0]
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return pi, err
	}
	id, err := strconv.Atoi(parts[2])
	if err != nil {
		return pi, err
	}

	pi.Host = host
	pi.Port = port
	pi.Raw = raw
	pi.Id = uint64(id)
	return pi, nil
}
func ParseMultiplePeerInfo(raw string) ([]PeerInfo, error) {
	parts := strings.Split(raw, ",")
	peers := []PeerInfo{}
	for _, p := range parts {
		peer, err := ParsePeerInfo(p)
		if err != nil {
			panic(fmt.Sprintf("error parsing %s for PeerInfo, %v", raw, err))
		}
		peers = append(peers, peer)
	}
	return peers, nil
}
func ParseConfigStream(r io.ReadSeeker) *Config {
	vis := map[string]bool{}
	scanner := bufio.NewScanner(r)
	lineno := 0
	cfg := Config{
		StorageType:          "stable",
		RpcType:              "emulated",
		Verbosity:            VerbosityWarning,
		HeartbeatTimeout:     DefaultHeartbeatTimeout,
		ElectionTimeoutLower: DefaultElectionTimeoutLower,
		ElectionTimeoutUpper: DefaultElectionTimeoutUpper,
		SnapshotChunkSize:    DefaultSnapshotChunkSize,
		SegmentFileSize:      DefaultSegmentFileSize,
		StableStateFilename:  DefaultStableStateFilename,
		ReplicationUnit:      DefaultReplicationUnit,
	}
	r.Seek(0, io.SeekStart)
	for scanner.Scan() {
		lineno++
		line := scanner.Text()
		line = strings.Trim(line, "\t ")
		sharppos := strings.Index(line, "#")
		if sharppos != -1 { // ignoring comments
			line = line[0:sharppos]
		}
		if len(line) == 0 {
			continue
		}
		parts := strings.Split(line, "=")
		if len(parts) != 2 {
			panic(fmt.Sprintf("unrecognized format at line %d, expecting: 'raft.key = value'", lineno))
		}
		key := strings.Trim(parts[0], "\t ")
		value := strings.Trim(parts[1], "\t ")
		vis[key] = true
		switch key {
		case "raft.self":
			self, err := ParsePeerInfo(value)
			if err != nil {
				panic(fmt.Sprintf("error parsing line %d for PeerInfo, %v", lineno, err))
			}
			cfg.Self = self
		case "raft.peers":
			peers, err := ParseMultiplePeerInfo(value)
			if err != nil {
				panic(fmt.Sprintf("error parsing line %d for multiple PeerInfo, %v", lineno, err))
			}
			cfg.Peers = peers
		case "raft.storageType":
			if value != "memory" && value != "stable" {
				panic(fmt.Sprintf("error parsing line %d, expecting 'raft.storageType = memory|stable', got %s", lineno, line))
			}
			cfg.StorageType = value
		case "raft.rpcType":
			if value != "emulated" && value != "httprpc" {
				panic(fmt.Sprintf("error parsing line %d, expecting 'raft.rpcType = emulated | httprpc', got %s", lineno, line))
			}
			cfg.RpcType = value
		case "raft.walDir":
			cfg.WalDir = value
		case "raft.stableStateFilename":
			cfg.StableStateFilename = value
		case "raft.hbInterval":
			t, err := strconv.Atoi(value)
			if t < 0 || err != nil {
				panic(fmt.Sprintf("error parsing line %d, expecting 'raft.hbInterval = positive integer', got %s, err: %v", lineno, line, err))
			}
			cfg.HeartbeatTimeout = time.Duration(t) * time.Millisecond
		case "raft.elecTimeoutRange":
			parts := strings.Split(value, "-")
			if len(parts) != 2 {
				panic(fmt.Sprintf("error parsing line %d, expecting 'raft.elecTimeoutRange = lower-upper', got %s", lineno, line))
			}

			lower, err := strconv.Atoi(parts[0])
			if err != nil {
				panic(fmt.Sprintf("error parsing line %d, expecting lower to be positive integer, got %s, err: %v", lineno, parts[0], err))
			}

			upper, err := strconv.Atoi(parts[1])
			if err != nil {
				panic(fmt.Sprintf("error parsing line %d, expecting upper to be positive integer, got %s, err: %v", lineno, parts[1], err))
			}
			if upper < lower {
				panic(fmt.Sprintf("error parsing line %d, expecting lower <= upper, got %s-%s", lineno, parts[0], parts[1]))
			}
			cfg.ElectionTimeoutLower = time.Duration(lower) * time.Millisecond
			cfg.ElectionTimeoutUpper = time.Duration(upper) * time.Millisecond
		case "raft.snapshotChunkSize":
			size, err := strconv.Atoi(value)
			if size < 0 || err != nil {
				panic(fmt.Sprintf("error parsing line %d, expecting 'raft.snapshotChunkSize = size in bytes', got %s, err: %v", lineno, line, err))
			}
			cfg.SnapshotChunkSize = size
		case "raft.segmentFileSize":
			size, err := strconv.Atoi(value)
			if size < 0 || err != nil {
				panic(fmt.Sprintf("error parsing line %d, expecting 'raft.segmentFileSize = size in bytes', got %s, err: %v", lineno, line, err))
			}
			cfg.SegmentFileSize = int64(size)
		case "raft.replicationUnit":
			unitSize, err := strconv.ParseUint(value, 10, 64)
			if unitSize <= 0 || err != nil {
				panic(fmt.Sprintf("error parsing line %d, expecting 'raft.replicationUnit > 0', got %s, err: %v", lineno, line, err))
			}
			cfg.ReplicationUnit = unitSize
		case "raft.verbosity":
			value = strings.ToLower(value)
			switch value {
			case "all":
				cfg.Verbosity = VerbosityAll
			case "trace":
				cfg.Verbosity = VerbosityTrace
			case "debug":
				cfg.Verbosity = VerbosityDebug
			case "info":
				cfg.Verbosity = VerbosityInfo
			case "warning":
				cfg.Verbosity = VerbosityWarning
			case "error":
				cfg.Verbosity = VerbosityError
			default:
				panic(fmt.Sprintf("error parsing line %d, expecting raft.verbosity = all | trace | debug | info | warning | error, got %s", lineno, line))
			}
		}
	}

	if _, ok := vis["raft.peers"]; !ok {
		panic("missing raft.peers field")
	}
	if _, ok := vis["raft.self"]; !ok {
		panic("missing raft.self field")
	}
	has := false
	addrVis := map[string]int{}
	for i := range cfg.Peers {
		if cfg.Self.Raw == cfg.Peers[i].Raw {
			has = true
		}
		addr := cfg.Peers[i].Host + ":" + strconv.Itoa(cfg.Peers[i].Port)
		if prevIdx, ok := addrVis[addr]; ok == true {
			panic(fmt.Sprintf("peer%d's identity %s coincides with peer%d", i+1, addr, prevIdx+1))
		}
		addrVis[addr] = i
	}
	if has == false {
		panic(fmt.Sprintf("'peers' fields contains no self identity %s", cfg.Self.Raw))
	}
	if cfg.WalDir == "" {
		wd, err := os.Getwd()
		if err != nil {
			panic(fmt.Sprintf("os.Getwd() => (%s,%v)", wd, err))
		}
		cfg.WalDir = wd + "/wal-" + cfg.Self.Raw
	}
	return &cfg
}
