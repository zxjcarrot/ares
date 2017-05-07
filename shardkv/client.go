package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"log"

	"github.com/zxjcarrot/ares/mockrpc"
	"github.com/zxjcarrot/ares/raft"
	"github.com/zxjcarrot/ares/shardmaster"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string, nshards int) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= nshards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm           *shardmaster.Clerk
	config       shardmaster.Config
	rpcClient    raft.RpcClient
	makePeerInfo func(string) *raft.PeerInfo
	// You will have to modify this struct.
	ackID       int64
	groupMaster map[int]int
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a mockrpc.ClientEnd on which you can
// send RPCs.
//
func MakeMockClerk(masters []*mockrpc.ClientEnd, make_end func(string) *mockrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeMockRpcClerk(masters)
	ck.rpcClient = NewMockRpcClient(make_end)
	ck.makePeerInfo = func(s string) *raft.PeerInfo {
		return &raft.PeerInfo{Host: "", Port: 0, Id: 0, Raw: s}
	}
	ck.groupMaster = map[int]int{}
	return ck
}

func MakeHttpRpcClerk(masters []raft.PeerInfo, makePeerInfo func(string) *raft.PeerInfo, T, D, I, W, E *log.Logger) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeHttpRpcClerk(masters, T, D, I, W, E)
	ck.rpcClient = raft.NewHttpRpcClient(T, D, I, W, E)
	ck.makePeerInfo = makePeerInfo
	// You'll have to add code here.
	ck.groupMaster = map[int]int{}
	return ck
}

func (ck *Clerk) Close() error {
	ck.sm.Close()
	return ck.rpcClient.Close()
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	reqID := nrand()
	args := GetArgs{}
	args.Key = key
	args.ReqID = reqID
	args.AckID = ck.ackID
	defer func() {
		ck.ackID = reqID
	}()
	for {
		shard := key2shard(key, shardmaster.NShards)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			start := ck.groupMaster[gid]
			have := false
			if start >= len(servers) {
				start = 0
			}
			for si := start; si != start || have == false; si = (si + 1) % len(servers) {
				have = true
				var reply GetReply
				ok := ck.rpcClient.Call(ck.makePeerInfo(servers[si]), "ShardKV.Get", &args, &reply) == nil

				if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.groupMaster[gid] = si
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}

		//NOTE: 100ms fixed latency if the system experienced a configuration change involving this key
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
		ck.groupMaster[gid] = 0
	}

}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	reqID := nrand()
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ReqID = reqID
	args.AckID = ck.ackID
	defer func() {
		ck.ackID = reqID
	}()
	for {
		shard := key2shard(key, shardmaster.NShards)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			start := ck.groupMaster[gid]
			have := false
			if start >= len(servers) {
				start = 0
			}
			for si := start; si != start || have == false; si = (si + 1) % len(servers) {

				have = true
				var reply PutAppendReply
				ok := ck.rpcClient.Call(ck.makePeerInfo(servers[si]), "ShardKV.PutAppend", &args, &reply) == nil
				if ok && reply.WrongLeader == false && reply.Err == OK {
					ck.groupMaster[gid] = si

					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
			}
		}
		//NOTE: 100ms fixed latency if the system experienced a configuration change involving this key
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
		ck.groupMaster[gid] = 0
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
