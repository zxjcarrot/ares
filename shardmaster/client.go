package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"log"
	"math/big"
	"strconv"
	"time"

	"github.com/zxjcarrot/ares/mockrpc"
	"github.com/zxjcarrot/ares/raft"
)

type Clerk struct {
	rpcClient raft.RpcClient
	// Your data here.
	peers     []raft.PeerInfo
	curLeader int
	ackId     int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeMockRpcClerk(servers []*mockrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	peerMap := map[uint64]*mockrpc.ClientEnd{}
	for i := range servers {
		peerMap[uint64(i)] = servers[i]
		ck.peers = append(ck.peers, raft.PeerInfo{Host: "", Port: 0, Id: uint64(i), Raw: strconv.Itoa(i)})
	}

	// Your code here.
	ck.curLeader = 0
	ck.ackId = 0
	ck.rpcClient = raft.NewMockRpcClient(peerMap)
	return ck
}

func MakeHttpRpcClerk(peers []raft.PeerInfo, T, D, I, W, E *log.Logger) *Clerk {
	ck := new(Clerk)
	// Your code here.
	ck.curLeader = 0
	ck.ackId = 0
	ck.peers = peers
	ck.rpcClient = raft.NewHttpRpcClient(T, D, I, W, E)
	return ck
}

func (ck *Clerk) Close() error {
	return ck.rpcClient.Close()
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{}
	// Your code here.
	args.Num = num
	reqId := nrand()
	args.AckID = ck.ackId
	args.ReqID = reqId
	defer func() {
		ck.ackId = reqId
	}()

	for {
		// try each known server.
		cycled := false
		for i := ck.curLeader; cycled == false || i != ck.curLeader; i = (i + 1) % len(ck.peers) {
			cycled = true
			var reply QueryReply
			ok := ck.rpcClient.Call(&ck.peers[i], "ShardMaster.Query", args, &reply) == nil
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{}
	// Your code here.
	args.Servers = servers
	reqId := nrand()
	args.AckID = ck.ackId
	args.ReqID = reqId
	defer func() {
		ck.ackId = reqId
	}()
	for {
		// try each known server.
		cycled := false
		for i := ck.curLeader; cycled == false || i != ck.curLeader; i = (i + 1) % len(ck.peers) {
			cycled = true
			var reply JoinReply
			ok := ck.rpcClient.Call(&ck.peers[i], "ShardMaster.Join", args, &reply) == nil
			if ok && reply.WrongLeader == false {
				ck.curLeader = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	reqId := nrand()
	args.AckID = ck.ackId
	args.ReqID = reqId
	defer func() {
		ck.ackId = reqId
	}()
	for {
		cycled := false
		for i := ck.curLeader; cycled == false || i != ck.curLeader; i = (i + 1) % len(ck.peers) {
			cycled = true
			var reply LeaveReply
			ok := ck.rpcClient.Call(&ck.peers[i], "ShardMaster.Leave", args, &reply) == nil
			if ok && reply.WrongLeader == false {
				ck.curLeader = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	reqId := nrand()
	args.AckID = ck.ackId
	args.ReqID = reqId
	defer func() {
		ck.ackId = reqId
	}()
	for {
		// try each known server.
		cycled := false
		for i := ck.curLeader; cycled == false || i != ck.curLeader; i = (i + 1) % len(ck.peers) {
			cycled = true
			var reply MoveReply
			ok := ck.rpcClient.Call(&ck.peers[i], "ShardMaster.Move", args, &reply) == nil
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
