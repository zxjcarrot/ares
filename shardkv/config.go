package shardkv

import (
	"bufio"
	"fmt"
	"io"
	"log"

	"strconv"
	"strings"

	"time"

	"github.com/zxjcarrot/ares/mockrpc"
	"github.com/zxjcarrot/ares/raft"
	"github.com/zxjcarrot/ares/shardmaster"
)

type ShardKVConfig struct {
	RaftCfg *raft.Config
	// maximum size of raft state before snapshotting.
	// raw format: "shardkv.maxRaftStateSize= size"
	MaxRaftStateSize int
	// total number of shards in the cluster, this number won't change after the cluster is initiated
	// raw format: "shardkv.nShards = n"
	NShards int
	// raft state size limit checking interval.
	// raw format: "shardkv.snapshotChkInterval = time in ms"
	SnapshotChkInterval time.Duration
	// cluster configuration update interval.
	// raw format: "shardkv.cfgUpdateInterval = time in ms"
	CfgUpdateInterval time.Duration
	// threshold factor for snapshotting.
	// Snapshotting happens when raftStateSize * snapshotThresh >= MaxRaftStateSize.
	// raw format: "shardkv.snapshotThresh = real number between (0-1]". Default value: 0.9
	SnapshotThresh float64
	// group id.
	//raw format: "shardkv.gid = gid"
	Gid int
	// rpc type.
	// if raft.rpcType and shardkv.rpcType are both provided, shardkv.rpcType overrides raft.rpcType.
	//raw format: "shardkv.rpc = emualted | httprpc". Default value: emulated.
	RpcType     string
	RpcTypeData interface{}
	Masters     []raft.PeerInfo
	// master rpc type.
	// if master.rpcType and shardkv.rpcTypse are both provided, shardkv.rpcType overrides master.rpcType.
	//raw format: "shardkv.rpc = emualted | httprpc". Default value: emulated.
	MasterRpcType     string
	MasterRpcTypeData interface{}
	Verbosity         int
	MakeEnd           func(string) *mockrpc.ClientEnd
	LogFilename       string
	T, D, I, W, E     *log.Logger
}

const (
	VerbosityAll     = 0
	VerbosityTrace   = 1
	VerbosityDebug   = 2
	VerbosityInfo    = 3
	VerbosityWarning = 4
	VerbosityError   = 5
)

const (
	DefaultMaxRaftStateSize            = 128 * 1024 * 1024 // 96MB
	DefaultSnapshottingThresholdFactor = 0.9
	DefaultSnapshotChkInterval         = time.Duration(100) * time.Millisecond
	DefaultConfigUpdateInterval        = time.Duration(100) * time.Millisecond
)

func ParseConfigStream(reader io.ReadSeeker) *ShardKVConfig {
	cfg := &ShardKVConfig{
		RaftCfg:             raft.ParseConfigStream(reader),
		SnapshotChkInterval: DefaultSnapshotChkInterval,
		NShards:             shardmaster.DefaultNShards,
		MaxRaftStateSize:    DefaultMaxRaftStateSize,
		CfgUpdateInterval:   DefaultConfigUpdateInterval,
		SnapshotThresh:      DefaultSnapshottingThresholdFactor,
		RpcType:             "emulated",
		MasterRpcType:       "emulated",
		Verbosity:           VerbosityWarning,
	}
	reader.Seek(0, io.SeekStart)
	scanner := bufio.NewScanner(reader)
	lineno := 0
	vis := map[string]string{}
	for scanner.Scan() { // a line at a time
		lineno++
		line := scanner.Text()
		line = strings.Trim(line, "\t ")
		sharppos := strings.Index(line, "#")
		if sharppos != -1 { // ignore comments
			line = line[0:sharppos]
		}
		if len(line) == 0 {
			continue
		}
		parts := strings.Split(line, "=")
		if len(parts) != 2 {
			panic(fmt.Sprintf("unrecognized format at line %d, expecting form: 'key = value'", lineno))
		}
		key := strings.Trim(parts[0], "\t ")
		value := strings.Trim(parts[1], "\t ")
		vis[key] = value
		switch key {
		case "shardkv.gid":
			t, err := strconv.Atoi(value)
			if err != nil {
				panic(fmt.Sprintf("error parsing line %d, expecting 'shardkv.gid = integer', got %s, err: %v", lineno, line, err))
			}
			cfg.Gid = t
		case "shardkv.maxRaftStateSize":
			s, err := strconv.Atoi(value)
			if s < 0 || err != nil {
				panic(fmt.Sprintf("error parsing line %d, expecting 'shardkv.maxRaftStateSize = positive integer', got %s, err: %v", lineno, line, err))
			}
			cfg.MaxRaftStateSize = s
		case "master.nShards":
			ns, err := strconv.Atoi(value)
			if ns < 0 || err != nil {
				panic(fmt.Sprintf("error parsing line %d, expecting 'master.nShards = positive integer', got %s, err: %v", lineno, line, err))
			}
			cfg.NShards = ns
		case "shardkv.snapshotChkInterval":
			i, err := strconv.Atoi(value)
			if i < 0 || err != nil {
				panic(fmt.Sprintf("error parsing line %d, expecting 'shardkv.snapshotChkInterval = positive integer', got %s, err: %v", lineno, line, err))
			}
			cfg.SnapshotChkInterval = time.Duration(i) * time.Millisecond
		case "shardkv.cfgUpdateInterval":
			i, err := strconv.Atoi(value)
			if i < 0 || err != nil {
				panic(fmt.Sprintf("error parsing line %d, expecting 'shardkv.cfgUpdateInterval = positive integer', got %s, err: %v", lineno, line, err))
			}
			cfg.CfgUpdateInterval = time.Duration(i) * time.Millisecond
		case "shardkv.snapshotThresh":
			f, err := strconv.ParseFloat(value, 64)
			if !(f > 0 && f <= 1) || err != nil {
				panic(fmt.Sprintf("error parsing line %d, expecting 'shardkv.cfgUpdateInterval = real number in (0,1] ', got %s, err: %v", lineno, line, err))
			}
			cfg.SnapshotThresh = f
		case "shardkv.rpcType":
			if value != "emulated" && value != "httprpc" {
				panic(fmt.Sprintf("error parsing line %d, expecting 'shardkv.rpcType = emulated | httprpc', got %s", lineno, line))
			}
			cfg.RpcType = value
		case "shardkv.masters":
			parts := strings.Split(value, ",")
			masters := []raft.PeerInfo{}
			for i, p := range parts {
				master, err := raft.ParsePeerInfo(p)
				if err != nil {
					panic(fmt.Sprintf("error parsing line %d for peer%d's PeerInfo, %v", lineno, i+1, err))
				}
				masters = append(masters, master)
			}
			cfg.Masters = masters

		case "master.rpcType":
			if value != "emulated" && value != "httprpc" {
				panic(fmt.Sprintf("error parsing line %d, expecting 'master.rpcType = emulated | httprpc', got %s", lineno, line))
			}
			cfg.MasterRpcType = value
		case "shardkv.verbosity":
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
				panic(fmt.Sprintf("error parsing line %d, expecting shardkv.verbosity = all | trace | debug | info | warning | error, got %s", lineno, line))
			}
		case "shardkv.logfile":
			cfg.LogFilename = value
		}
	}
	if _, ok := vis["shardkv.gid"]; ok == false {
		panic("missing shardkv.gid field")
	}
	if _, ok := vis["shardkv.masters"]; ok == false {
		panic("missing shardkv.masters field")
	}
	fmt.Printf("vis:%v", vis)
	//overrides raft.rpcType
	cfg.RaftCfg.RpcType = cfg.RpcType
	return cfg
}
