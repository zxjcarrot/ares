package shardkv

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/zxjcarrot/ares/raft"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrTimedout    = "ErrTimedout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ReqID int64
	AckID int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ReqID int64
	AckID int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

func DefaultMakePeerInfo(s string) *raft.PeerInfo {
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		panic(fmt.Sprintf("expecting server config to be host:port:id, got %s", s))
	}
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		panic(fmt.Sprintf("expecting port to be unsigned integer, got %s %v ", parts[1], err))
	}
	id, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		panic(fmt.Sprintf("expecting id to be unsigned 64bit integer, got %s %v ", parts[2], err))
	}
	return &raft.PeerInfo{Host: parts[0], Port: port, Id: id, Raw: s}
}
