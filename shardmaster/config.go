package shardmaster

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"time"

	"strconv"
	"strings"

	"github.com/zxjcarrot/ares/raft"
)

//configuration for a Raft peer
type ShardMasterConfig struct {
	RaftCfg             *raft.Config
	MaxRaftStateSize    int
	NShards             int
	SnapshotChkInterval time.Duration
	SnapshotThresh      float64
	RpcType             string
	RpcTypeData         interface{}
	Verbosity           int
	LogFilename         string
	Persister           *raft.Persister
	T, D, I, W, E       *log.Logger
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
	DefaultMaxRaftStateSize            = 16 * 1024 * 1024 // 16MB
	DefaultSnapshotChkInterval         = time.Duration(100) * time.Millisecond
	DefaultSnapshottingThresholdFactor = 0.9
	DefaultNShards                     = 10
)

func ParseConfigStream(reader io.ReadSeeker) *ShardMasterConfig {
	cfg := &ShardMasterConfig{
		RaftCfg:             raft.ParseConfigStream(reader),
		SnapshotChkInterval: DefaultSnapshotChkInterval,
		SnapshotThresh:      DefaultSnapshottingThresholdFactor,
		NShards:             DefaultNShards,
		MaxRaftStateSize:    DefaultMaxRaftStateSize,
		RpcType:             "emulated",
		Verbosity:           VerbosityWarning,
	}
	reader.Seek(0, io.SeekStart)
	scanner := bufio.NewScanner(reader)
	lineno := 0
	vis := map[string]bool{}
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
		vis[key] = true
		switch key {
		case "master.maxRaftStateSize":
			s, err := strconv.Atoi(value)
			if s < 0 || err != nil {
				panic(fmt.Sprintf("error parsing line %d, expecting 'master.maxRaftStateSize = positive integer', got %s, err: %v", lineno, line, err))
			}
			cfg.MaxRaftStateSize = s
		case "master.nShards":
			ns, err := strconv.Atoi(value)
			if ns < 0 || err != nil {
				panic(fmt.Sprintf("error parsing line %d, expecting 'master.nShards = positive integer', got %s, err: %v", lineno, line, err))
			}
			cfg.NShards = ns
		case "master.snapshotChkInterval":
			i, err := strconv.Atoi(value)
			if i < 0 || err != nil {
				panic(fmt.Sprintf("error parsing line %d, expecting 'master.snapshotChkInterval = positive integer', got %s, err: %v", lineno, line, err))
			}
			cfg.SnapshotChkInterval = time.Duration(i)
		case "master.snapshotThresh":
			f, err := strconv.ParseFloat(value, 64)
			if !(f > 0 && f <= 1) || err != nil {
				panic(fmt.Sprintf("error parsing line %d, expecting 'master.cfgUpdateInterval = real number in (0,1] ', got %s, err: %v", lineno, line, err))
			}
			cfg.SnapshotThresh = f
		case "master.rpcType":
			if value != "emulated" && value != "httprpc" {
				panic(fmt.Sprintf("error parsing line %d, expecting 'master.rpcType = emulated | httprpc', got %s", lineno, line))
			}
			cfg.RpcType = value
		case "master.verbosity":
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
				panic(fmt.Sprintf("error parsing line %d, expecting master.verbosity = all | trace | debug | info | warning | error, got %s", lineno, line))
			}
		case "master.logfile":
			cfg.LogFilename = value
		}
	}
	//overrides raft.rpcType
	cfg.RaftCfg.RpcType = cfg.RpcType
	return cfg
}
