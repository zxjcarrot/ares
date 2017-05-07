package shardkv

// import "shardmaster"
import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"runtime"

	"github.com/smartystreets/go-disruptor"
	"github.com/zxjcarrot/ares/mockrpc"
	"github.com/zxjcarrot/ares/raft"
	"github.com/zxjcarrot/ares/shardmaster"
)

type sKVOpType int

const (
	getOp        sKVOpType = 0
	putOp        sKVOpType = 1
	appendOp     sKVOpType = 2
	pullShardsOp sKVOpType = 3
)

type entryType int

const (
	kvOpEntryType         = 0
	recfgEntryType        = 1
	pullShardsOpEntryType = 2
)

type SKVLogEntry struct {
	EType entryType
	Data  interface{}
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType sKVOpType
	Key    string
	Value  string
	ReqID  int64
	AckID  int64
}

type sKVOpResp int

const (
	kvOpRespOK          = 0
	kvOpRespWrongLeader = 1
	kvOpRespWrongGroup  = 2
)

type sKVOpReq struct {
	reqId  int64
	ackId  int64
	opType sKVOpType
	args   interface{}
	reply  interface{}
	respCh chan sKVOpResp
}

type KVStore struct {
	ShardIdx int
	Store    map[string]string
	ReqIdMap map[int64]bool
	Version  int // config number
	From     []string
}

const (
	ringSize = 256
	ringMask = ringSize - 1
)

type ShardKV struct {
	mu      sync.Mutex
	me      uint64
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	gid     int

	rpcServer    raft.RpcServer
	rpcClient    raft.RpcClient
	makePeerInfo func(string) *raft.PeerInfo
	// Your definitions here.
	smClerk        *shardmaster.Clerk
	quitCh         chan bool
	stopped        int32
	lastAppliedIdx uint64
	// datastores indexed by shard idx
	stores        []KVStore
	reqCh         chan sKVOpReq // for serilization of requests
	cfg           shardmaster.Config
	groupLeader   map[int]int
	T, D, I, W, E *log.Logger
	ring          [ringSize]chan sKVOpResp
	disruptor     disruptor.Disruptor
	kvCfg         *ShardKVConfig
}

func (kv *ShardKV) isStopped() bool {
	return atomic.LoadInt32(&kv.stopped) == 1
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	//kv.W.Printf("new get request(%d) %v", args.ReqID, args)
	respCh := make(chan sKVOpResp, 1)
	req := sKVOpReq{args.ReqID, args.AckID, getOp, args, reply, respCh}
	reply.WrongLeader = false
	reply.Err = OK
	kv.reqCh <- req
	select {
	case _ = <-respCh:
		break
	case _ = <-time.After(1 * time.Second):
		kv.T.Printf("Get(%v) timed out after 1s", args)
		reply.Err = ErrTimedout
	}
	//kv.W.Printf("get request(%d)'s reply %v", args.ReqID, reply)
	return nil
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	//kv.W.Printf("new putAppend request(%d) %v", args.ReqID, args)
	respCh := make(chan sKVOpResp, 1)
	req := sKVOpReq{args.ReqID, args.AckID, 0, args, reply, respCh}
	if args.Op == "Put" {
		req.opType = putOp
	} else { // "Append"
		req.opType = appendOp
	}

	reply.WrongLeader = false
	reply.Err = OK
	kv.reqCh <- req

	select {
	case _ = <-respCh:
		break
	case _ = <-time.After(1 * time.Second):
		kv.T.Printf("PutAppend(%v) timed out after 1s", args)
		reply.Err = ErrTimedout
	}
	//kv.W.Printf("putAppend request(%d)'s reply %v", args.ReqID, reply)
	return nil
}

type ReplyHelper struct {
	respCh chan sKVOpResp
	reply  interface{}
	data   interface{}
	term   uint64
	reqId  int64
}

// Test if server is responsible for a given key
func (kv *ShardKV) isRightGroup(key string) bool {
	s := key2shard(key, kv.kvCfg.NShards)
	return kv.cfg.Shards[s] == kv.gid
}

func (kv *ShardKV) Consume(lower, upper int64) {
	for lower <= upper {
		//kv.W.Printf("ring before receiving")
		c := kv.ring[lower&ringMask]
		//kv.W.Printf("ring after receiving")
		c <- 1
		lower++
	}
}

func (kv *ShardKV) handleApplyGetOp(applyMsg *raft.ApplyMsg, op *Op, reqId int64, ackId int64, idxToReplyHelperMap map[uint64]ReplyHelper, store KVStore) {
	idx, applyTerm := applyMsg.Index, applyMsg.Term
	key := op.Key
	reqIdMap := store.ReqIdMap
	// for duplicated Get requests, it is semantically correct to return the latest value
	//if _, ok := reqIdMap[reqId]; ok == true {
	//	kv.T.Printf("duplicate GetOp request, return the latest value", )
	//}
	//	kv.I.Printf("Applying GettOp %v at index %d", op, idx)
	if _, ok := reqIdMap[ackId]; ok {
		delete(reqIdMap, ackId) // delete acked request id to free memory
		kv.T.Printf("deleted processed request %v", ackId)
	}
	helper, exist := idxToReplyHelperMap[idx]
	if exist == false {
		//kv.T.Printf("Applying GettOp at index %d, helper doesn't exist", idx)
		return
	}
	delete(idxToReplyHelperMap, idx)
	if helper.reqId != reqId {
		kv.I.Printf("Applying GetOp at index %d, reqId %d for the helper mismatches with the one %d in the log", idx, helper.reqId, reqId)
		kv.publishRespRelease(helper.respCh)
		return
	}
	s := store.Store
	reply := helper.reply.(*GetReply)
	if kv.isRightGroup(key) == false {
		reply.Err = ErrWrongGroup
	} else if helper.term != applyTerm {
		// This is the case in which a leader has called Start() for a client RPC,
		// but lost its leadership before the request were committed to the log.
		kv.W.Printf("GetOp, lost leadership at term %d before committing the entry %#v", helper.term, applyMsg)
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	} else if value, has := s[key]; has == false {
		reply.Err = ErrNoKey
		reply.Value = ""
		//		kv.D.Printf("Applied GettOp, No such key %v", key)
	} else {
		//		kv.D.Printf("Applied GettOp, %v => %v", key, value)
		reply.Err = OK
		reply.Value = value
	}
	//TODO: remove bottleneck
	// All concurrent requests will be serialized and blocked on a
	// release channel waiting for their request to complete.
	// chansend on main thread could be a bottleneck if
	// there is a receiver waiting on the other side which is the case here.
	// In that case sender will send data directly to
	// the receiver incurring much more communication overhead
	// between goroutines than simply placing the value onto the buffer.
	kv.publishRespRelease(helper.respCh)
}

func (kv *ShardKV) handleApplyPutOp(applyMsg *raft.ApplyMsg, op *Op, reqId int64, ackId int64, idxToReplyHelperMap map[uint64]ReplyHelper, store KVStore) {
	reqIdMap := store.ReqIdMap
	idx, applyTerm := applyMsg.Index, applyMsg.Term
	key := op.Key
	rightGroup := kv.isRightGroup(key)
	if _, ok := reqIdMap[reqId]; ok == false {
		//exectute command only once
		if rightGroup {
			s := store.Store
			s[key] = op.Value
			//kv.I.Printf("Applied putOp %v at index %d", op, idx)
			reqIdMap[reqId] = true
		}
	} else {
		kv.D.Printf("duplicate PutOp request %d, return without execution", reqId)
	}
	if _, ok := reqIdMap[ackId]; ok {
		delete(reqIdMap, ackId) // delete acked request id to free memory
		//kv.T.Printf("deleted processed request %v" , ackId)
	}
	helper, exist := idxToReplyHelperMap[idx]
	if exist == false {
		//kv.T.Printf("Applying PutOp at index %d, helper doesn't exist", idx)
		return
	}
	delete(idxToReplyHelperMap, idx)

	if helper.reqId != reqId {
		kv.I.Printf("Applying PutOp at index %d reqId %d, but helper mismatches with the one %d in the log", idx, helper.reqId, reqId)

		kv.publishRespRelease(helper.respCh)
		return
	}

	reply := helper.reply.(*PutAppendReply)
	if rightGroup == false {
		reply.Err = ErrWrongGroup
	} else if helper.term != applyTerm {
		// This is the case in which a leader has called Start() for a client RPC,
		// but lost its leadership before the request were committed to the log.
		kv.W.Printf("PutOp, lost leadership at term %d before committing the entry %#v", helper.term, applyMsg)
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
	}
	kv.publishRespRelease(helper.respCh)
}

func (kv *ShardKV) publishRespRelease(c chan sKVOpResp) {
	writer := kv.disruptor.Writer()
	sequence := writer.Reserve(1)
	kv.ring[sequence&ringMask] = c
	writer.Commit(sequence, sequence)
}

func (kv *ShardKV) handleApplyAppendOp(applyMsg *raft.ApplyMsg, op *Op, reqId int64, ackId int64, idxToReplyHelperMap map[uint64]ReplyHelper, store KVStore) {
	reqIdMap := store.ReqIdMap
	idx, applyTerm := applyMsg.Index, applyMsg.Term
	key := op.Key
	rightGroup := kv.isRightGroup(key)
	if _, ok := reqIdMap[reqId]; ok == false {
		//exectute command only once
		if rightGroup {
			s := store.Store
			if _, ok = s[key]; ok == true {
				s[key] = s[key] + op.Value
			} else {
				s[key] = op.Value
			}
			kv.D.Printf("Applied appendOp %v", op)
			reqIdMap[reqId] = true
		}
	} else {
		//kv.T.Printf("duplicate AppendOp request %d, return without execution", reqId)
	}
	if _, ok := reqIdMap[ackId]; ok {
		delete(reqIdMap, ackId) // delete acked request id to free memory
	}
	helper, exist := idxToReplyHelperMap[idx]
	if exist == false {
		return
	}
	delete(idxToReplyHelperMap, idx)
	if helper.reqId != reqId {
		kv.I.Printf("Applying AppendOp at index %d, reqId %d for the helper mismatches with the one %d in the log", idx, helper.reqId, reqId)
		kv.publishRespRelease(helper.respCh)
		return
	}

	reply := helper.reply.(*PutAppendReply)
	if rightGroup == false {
		reply.Err = ErrWrongGroup
	} else if helper.term != applyTerm {
		// This is the case in which a leader has called Start() for a client RPC,
		// but lost its leadership before the request were committed to the log.
		kv.W.Printf("AppendOp, lost leadership at term %d before committing the entry %#v", helper.term, applyMsg)
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
	}

	kv.publishRespRelease(helper.respCh)
}

type PullShardsArgs struct {
	ConfigNum int
	Shards    []int
}

type PullShardsReply struct {
	Err         Err
	WrongLeader bool
	Shards      []KVStore
}

func (kv *ShardKV) getCurrentConfigNum() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.cfg.Num
}

func (kv *ShardKV) PullShards(args *PullShardsArgs, reply *PullShardsReply) error {
	if _, isLeader := kv.rf.GetState(); isLeader == false {
		reply.WrongLeader = true
		return nil
	}
	for i := 0; i < 3; i++ {
		currentConfigNum := kv.getCurrentConfigNum()
		kv.I.Printf("currentConfigNum %d, args %v", currentConfigNum, args)
		if currentConfigNum >= args.ConfigNum {
			break
		} else if i == 2 {
			reply.Err = ErrTimedout
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for i := 0; i < len(args.Shards); i++ {
		sidx := args.Shards[i]
		cp := copyKVStore(kv.stores[sidx])
		cp.From = append(cp.From, fmt.Sprintf("Group%d Me%d", kv.gid, kv.me))
		reply.Shards = append(reply.Shards, cp)
	}
	reply.WrongLeader = false
	reply.Err = OK
	return nil
}

const (
	rpcOK       = 0
	rpcFailed   = 1
	rpcTimedOut = 2
)

type rpcReplyCode int

func (kv *ShardKV) timedRPCCall(peer *raft.PeerInfo, rpcname string, args interface{}, reply interface{}, timeout time.Duration) rpcReplyCode {
	ch := make(chan bool)
	go func(ch chan bool) {
		ch <- kv.rpcClient.Call(peer, rpcname, args, reply) == nil
	}(ch)
	select {
	case ok := <-ch:
		if ok {
			return rpcOK
		} else {
			return rpcFailed
		}
	case <-time.After(timeout):
		break
	}
	return rpcTimedOut
}

const (
	queryConfigTimeout    = 1 * time.Second
	pullShardsTimeout     = 4 * time.Second
	pullShardsMaxBackoff  = 10000 * time.Millisecond
	pullShardsInitBackoff = 500 * time.Millisecond
)

func (kv *ShardKV) migrate(oldCfg *shardmaster.Config) {
	newCfg := &kv.cfg
	//pull shards from other groups
	groupShardsMap := map[int][]int{}
	for i := 0; i < kv.kvCfg.NShards; i++ {
		if fromGid := oldCfg.Shards[i]; newCfg.Shards[i] == kv.gid && fromGid != kv.gid {
			if fromGid == 0 {
				kv.I.Printf("got shard %d from group 0 immediately", i)
				continue
			}
			groupShardsMap[fromGid] = append(groupShardsMap[fromGid], i)
		}
	}

	type pullShardsDoneMsg struct {
		gid int
		// The index of server in servers array that responded OK
		lastLeaderIdx int
		reply         *PullShardsReply
	}

	doPullShards := func(cfgNum, fromGid, startIdx int, servers []string, shards []int, doneCh chan pullShardsDoneMsg) {
		args := PullShardsArgs{cfgNum, shards}
		reply := PullShardsReply{}
		trys := 1
		backoff := pullShardsInitBackoff
		if startIdx >= len(servers) {
			startIdx = 0
		}
		for !kv.isStopped() {
			have := false
			if trys > 1 {
				kv.W.Printf("PullShards trying the %dth time", trys)
			}
			for srvIdx := startIdx; srvIdx != startIdx || !have; srvIdx = (srvIdx + 1) % len(servers) {
				have = true
				res := kv.timedRPCCall(kv.makePeerInfo(servers[srvIdx]), "ShardKV.PullShards", &args, &reply, pullShardsTimeout)
				if res == rpcOK {
					if reply.Err == OK {
						//kv.I.Printf("PullShards, group%d leader(%d, %s) replied %v", fromGid, srvIdx, servers[srvIdx], reply)
						doneCh <- pullShardsDoneMsg{fromGid, srvIdx, &reply}
						return
					} else if reply.Err == ErrWrongLeader {
						kv.I.Printf("PullShards, server(%d,%s) is not the leader.", srvIdx, servers[srvIdx])
					} else if reply.Err == ErrTimedout {
						kv.W.Printf("RPC ShardKV.PullShards, server(%d,%s)'s config is lagging behind, retry after a while.!!", srvIdx, servers[srvIdx])
						startIdx = srvIdx
						break
					}
				} else if res == rpcFailed {
					kv.W.Printf("Failed to call ShardKV.PullShards at server(%d,%s)!!", srvIdx, servers[srvIdx])
				} else {
					kv.W.Printf("RPC ShardKV.PullShards at server(%d,%s) timed out after %v!!", srvIdx, servers[srvIdx], pullShardsTimeout)
				}
			}
			if kv.isStopped() {
				break
			}
			trys++
			kv.W.Printf("doPullShards() backing off for %v", backoff)
			time.Sleep(backoff)
			if backoff*2 > pullShardsMaxBackoff {
				backoff = pullShardsMaxBackoff
			} else {
				backoff *= 2
			}
		}
		kv.W.Printf("server stopped")
		doneCh <- pullShardsDoneMsg{-1, -1, nil}
	}
	doneCh := make(chan pullShardsDoneMsg)
	for gid, shards := range groupShardsMap {
		go doPullShards(kv.cfg.Num, gid, kv.groupLeader[gid], oldCfg.Groups[gid], shards, doneCh)
	}
	for i := 0; i < len(groupShardsMap); i++ {
		kv.mu.Unlock()
		msg := <-doneCh
		kv.mu.Lock()
		reply := msg.reply
		if reply == nil {
			kv.E.Printf("PullShards failed, system can not proceed!")
			continue
		}
		for j := 0; j < len(reply.Shards); j++ {
			kv.stores[reply.Shards[j].ShardIdx] = reply.Shards[j]
			kv.I.Printf("doPullShards reply %v", kv.stores[reply.Shards[j].ShardIdx])
			// cache the last leader index of a group to avoid unnecessary future trials
			kv.groupLeader[msg.gid] = msg.lastLeaderIdx
		}
	}
	kv.I.Printf("done migration, got shards from %v", groupShardsMap)
}

func (kv *ShardKV) updateCfg() {
	ch := make(chan shardmaster.Config)

	go func(cfgNum int, doneCh chan shardmaster.Config) {
		doneCh <- kv.smClerk.Query(cfgNum)
	}(kv.cfg.Num+1, ch)
	kv.mu.Unlock()
	select {
	case nextConfig := <-ch:
		kv.mu.Lock()
		if nextConfig.Num > kv.cfg.Num {
			//configuration change
			w := new(bytes.Buffer)
			e := gob.NewEncoder(w)
			e.Encode(SKVLogEntry{recfgEntryType, nextConfig})
			idx, term, isLeader := kv.rf.Start(w.Bytes())
			if isLeader == false {
				kv.E.Printf("error storing reconfiguration entry %v in the raft log, not leader.", e)
			} else {
				kv.I.Printf("stored reconfiguration entry %v at (%d, %d)", nextConfig, term, idx)
			}
		}
	case <-time.After(queryConfigTimeout):
		kv.W.Printf("Query for lastest config timed out")
		kv.mu.Lock()
		break
	}
}

func (kv *ShardKV) handleRecfg(newCfg *shardmaster.Config) {
	if newCfg.Num > kv.cfg.Num {
		oldCfg := kv.cfg
		kv.cfg = shardmaster.CopyConfig(*newCfg)
		kv.I.Printf("found new config%d %v, old confg %v, starting migration.", newCfg.Num, newCfg, oldCfg)
		kv.migrate(&oldCfg)
		kv.I.Printf("reconfiguration to config%d completed", newCfg.Num)
	}
}
func (kv *ShardKV) tryTakeSnapshot(forced bool) {
	if kv.rf.LatestSnapshotIdx() < kv.lastAppliedIdx && (forced || kv.kvCfg.MaxRaftStateSize != -1 &&
		float64(kv.rf.RaftStateSize())*kv.kvCfg.SnapshotThresh >= float64(kv.kvCfg.MaxRaftStateSize)) {
		kv.W.Printf("scaled raft state size %fMB, max state size %fMB", float64(kv.rf.RaftStateSize())*kv.kvCfg.SnapshotThresh/1024/1024, float64(kv.kvCfg.MaxRaftStateSize)/1024/1024)
		raft.PrintMemoryStatus("before snapshotting", kv.W)
		b := new(bytes.Buffer)
		e := gob.NewEncoder(b)
		e.Encode(kv.stores)
		e.Encode(kv.cfg)
		kv.mu.Unlock()
		kv.rf.SaveSnapshot(b.Bytes(), kv.lastAppliedIdx)
		raft.PrintMemoryStatus("after snapshotting", kv.W)
		kv.mu.Lock()
	}
}
func (kv *ShardKV) drainReuqests(firstReq sKVOpReq, idxToReplyHelperMap map[uint64]ReplyHelper) {
	// do batch appending
	reqs := []sKVOpReq{firstReq}
reqLoop:
	for {
		select {
		case req := <-kv.reqCh:
			reqs = append(reqs, req)
		default:
			break reqLoop
		}
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	commands := make([][]byte, 0, len(reqs))
	// reqIdxs stores candidcate request indexes in reqs that map to this group
	reqIdxs := make([]int, 0, len(reqs))
	for i, req := range reqs {
		switch req.opType {
		case getOp:
			args := req.args.(*GetArgs)
			reply := req.reply.(*GetReply)
			if kv.isRightGroup(args.Key) {
				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				e.Encode(SKVLogEntry{kvOpEntryType, Op{getOp, args.Key, "", req.reqId, req.ackId}}) // for detecting duplicates across term boundary
				commands = append(commands, w.Bytes())
				reqIdxs = append(reqIdxs, i)
			} else {
				kv.I.Printf("wrong group for %v, should go to group %d", args, kv.cfg.Shards[key2shard(args.Key, kv.kvCfg.NShards)])
				reply.Err = ErrWrongGroup
				req.respCh <- 1
			}
		case putOp:
			args := req.args.(*PutAppendArgs)
			reply := req.reply.(*PutAppendReply)
			if kv.isRightGroup(args.Key) {
				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				e.Encode(SKVLogEntry{kvOpEntryType, Op{putOp, args.Key, args.Value, req.reqId, req.ackId}}) // for detecting duplicates across term boundary
				commands = append(commands, w.Bytes())
				reqIdxs = append(reqIdxs, i)
				// for detecting duplicates across term boundary
			} else {
				kv.I.Printf("wrong group for %v, should go to group %d", args, kv.cfg.Shards[key2shard(args.Key, kv.kvCfg.NShards)])
				reply.Err = ErrWrongGroup
				req.respCh <- 1
			}
		case appendOp:
			args := req.args.(*PutAppendArgs)
			reply := req.reply.(*PutAppendReply)
			if kv.isRightGroup(args.Key) {
				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				e.Encode(SKVLogEntry{kvOpEntryType, Op{appendOp, args.Key, args.Value, req.reqId, req.ackId}}) // for detecting duplicates across term boundary
				commands = append(commands, w.Bytes())
				reqIdxs = append(reqIdxs, i)
			} else {
				kv.I.Printf("wrong group for %v, should go to group %d", args, kv.cfg.Shards[key2shard(args.Key, kv.kvCfg.NShards)])
				reply.Err = ErrWrongGroup
				req.respCh <- 1
			}
		}
	}
	if len(commands) == 0 {
		return
	}
	// append candidate commands to the log
	startIdx, term, isLeader := kv.rf.StartBatch(commands)
	// do leadership check & installing reply helper for candidates
	for i := 0; i < len(reqIdxs); i++ {
		req := reqs[reqIdxs[i]]
		idx := startIdx + uint64(i)
		switch req.opType {
		case getOp:
			args := req.args.(*GetArgs)
			reply := req.reply.(*GetReply)
			if isLeader == false {
				reply.Err = ErrWrongLeader
				reply.WrongLeader = true
				req.respCh <- 1
			} else {
				//kv.I.Printf("stored putOp %v at index %d, term %d, reqId %d ", args, idx, term, req.reqId)
				idxToReplyHelperMap[idx] = ReplyHelper{req.respCh, reply, Op{getOp, args.Key, "", req.reqId, req.ackId}, term, req.reqId}
			}
		case putOp:
			args := req.args.(*PutAppendArgs)
			reply := req.reply.(*PutAppendReply)
			if isLeader == false {
				reply.Err = ErrWrongLeader
				reply.WrongLeader = true
				req.respCh <- 1
			} else {
				//kv.I.Printf("stored putOp %v at index %d, term %d, reqId %d ", args, idx, term, req.reqId)
				idxToReplyHelperMap[idx] = ReplyHelper{req.respCh, reply, Op{putOp, args.Key, args.Value, req.reqId, req.ackId}, term, req.reqId}
			}
		case appendOp:
			args := req.args.(*PutAppendArgs)
			reply := req.reply.(*PutAppendReply)
			if isLeader == false {
				reply.Err = ErrWrongLeader
				reply.WrongLeader = true
				req.respCh <- 1
			} else {
				kv.I.Printf("stored appendOp %v at index %d, term %d, reqId %d ", args, idx, term, req.reqId)
				idxToReplyHelperMap[idx] = ReplyHelper{req.respCh, reply, Op{appendOp, args.Key, args.Value, req.reqId, req.ackId}, term, req.reqId}
			}
		}
	}
	//kv.tryTakeSnapshot(false)
}

func (kv *ShardKV) handleApplyOpGeneric(applyMsg *raft.ApplyMsg, idxToReplyHelperMap map[uint64]ReplyHelper, op Op) {
	reqId := op.ReqID
	ackId := op.AckID
	sidx := key2shard(op.Key, kv.kvCfg.NShards)
	if kv.stores[sidx].Store == nil {
		kv.stores[sidx].Store = map[string]string{}
		kv.stores[sidx].ReqIdMap = map[int64]bool{}
	}
	if kv.stores[sidx].Version > kv.cfg.Num {
		// Skip kv ops if the store's version is newer than the config we are in.
		// Maybe we got the shard from a node that holds more recent content.
		kv.I.Printf("skipping op %v, shard version %d > current version %d", op, kv.stores[sidx].Version, kv.cfg.Num)
		delete(kv.stores[sidx].ReqIdMap, op.AckID)
		return
	}
	//update the version to at least the current configuration version.
	kv.stores[sidx].Version = kv.cfg.Num

	/*
	* For followers, simply apply commands without answering any client requests.
	 */
	switch op.OpType {
	case getOp:
		kv.handleApplyGetOp(applyMsg, &op, reqId, ackId, idxToReplyHelperMap, kv.stores[sidx])
	case putOp:
		kv.handleApplyPutOp(applyMsg, &op, reqId, ackId, idxToReplyHelperMap, kv.stores[sidx])
	case appendOp:
		kv.handleApplyAppendOp(applyMsg, &op, reqId, ackId, idxToReplyHelperMap, kv.stores[sidx])
	}
}
func (kv *ShardKV) handleApplyMessage(applyMsg *raft.ApplyMsg, idxToReplyHelperMap map[uint64]ReplyHelper) {
	kv.mu.Lock()
	kv.lastAppliedIdx = applyMsg.Index
	if applyMsg.UseSnapshot {
		raft.PrintMemoryStatus("before restoring", kv.W)
		d := gob.NewDecoder(bytes.NewBuffer(applyMsg.Snapshot))
		for i := 0; i < kv.kvCfg.NShards; i++ {
			kv.stores[i] = KVStore{i, nil, nil, 0, nil}
		}
		d.Decode(&kv.stores)
		kv.cfg = shardmaster.Config{}
		d.Decode(&kv.cfg)
		kv.W.Printf("restored kv.stores with snapshot[%d], config %v", kv.lastAppliedIdx, kv.cfg)
		raft.PrintMemoryStatus("after restoring", kv.W)
	} else {
		helper, ok := idxToReplyHelperMap[applyMsg.Index]
		if ok {
			kv.handleApplyOpGeneric(applyMsg, idxToReplyHelperMap, helper.data.(Op))
		} else {
			e := SKVLogEntry{}
			r := bytes.NewBuffer(applyMsg.Command.([]byte))
			d := gob.NewDecoder(r)
			d.Decode(&e)
			if e.EType == kvOpEntryType {
				kv.handleApplyOpGeneric(applyMsg, idxToReplyHelperMap, e.Data.(Op))
			} else if e.EType == recfgEntryType {
				newCfg := e.Data.(shardmaster.Config)
				kv.handleRecfg(&newCfg)
				kv.tryTakeSnapshot(true)
			}
		}
	}
	//kv.tryTakeSnapshot(false)
	kv.mu.Unlock()
}
func (kv *ShardKV) drainApplyMessages(idxToReplyHelperMap map[uint64]ReplyHelper) {
	msgs := make([]*raft.ApplyMsg, 0, 128)
	for n := 128; n > 0; n-- {
		select {
		case applyMsg := <-kv.applyCh:
			msgs = append(msgs, &applyMsg)
		default:
			n = 0
			break
		}
	}
	for _, am := range msgs {
		kv.handleApplyMessage(am, idxToReplyHelperMap)
	}
}

func (kv *ShardKV) serve() {
	oldApplied := uint64(0)
	idxToReplyHelperMap := make(map[uint64]ReplyHelper)
	snapshotChkTimer := time.NewTicker(kv.kvCfg.SnapshotChkInterval)
	cfgUpdateTimer := time.NewTicker(kv.kvCfg.CfgUpdateInterval)
	statusReportTimer := time.NewTicker(10 * time.Second)
	for {
		if kv.lastAppliedIdx-oldApplied >= 10000 {
			kv.W.Printf("applied %d entries, now at index %d", kv.lastAppliedIdx-oldApplied, kv.lastAppliedIdx)
			oldApplied = kv.lastAppliedIdx
		}
		select {
		case _ = <-kv.quitCh:
			kv.I.Printf("ShardKV.serve() stopped")
			return
		case _ = <-statusReportTimer.C:
			raft.PrintMemoryStatus("Memory Usage", kv.W)
		case _ = <-cfgUpdateTimer.C:
			kv.mu.Lock()
			if _, isLeader := kv.rf.GetState(); isLeader {
				kv.T.Printf("checking lastest config")
				kv.updateCfg()
			}
			kv.mu.Unlock()
		case _ = <-snapshotChkTimer.C:
			kv.mu.Lock()
			kv.tryTakeSnapshot(false)
			kv.mu.Unlock()
		case applyMsg := <-kv.applyCh:
			kv.handleApplyMessage(&applyMsg, idxToReplyHelperMap)
			kv.drainApplyMessages(idxToReplyHelperMap)
		case req := <-kv.reqCh:
			kv.drainReuqests(req, idxToReplyHelperMap)
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	// Your code here, if desired.
	kv.W.Printf("Killed")
	atomic.StoreInt32(&kv.stopped, 1)
	kv.quitCh <- true
	kv.rf.Kill()
	kv.disruptor.Stop()
}

func (kv *ShardKV) Cleanup() {
	// Your code here, if desired.
	kv.D.Printf("before kv.rf.Cleanup()")
	kv.mu.Lock()
	kv.rf.Cleanup()
	kv.mu.Unlock()
	kv.D.Printf("after kv.rf.Cleanup()")
}

func StartServer(kvCfg *ShardKVConfig) *ShardKV {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
	kvCfg.I.Printf("shard config: %v", kvCfg)
	gob.Register(Op{})
	gob.Register(shardmaster.Config{})
	gob.Register(SKVLogEntry{})
	kv := new(ShardKV)
	kv.kvCfg = kvCfg
	kv.me = kvCfg.RaftCfg.Self.Id
	kv.gid = kvCfg.Gid
	kv.stores = make([]KVStore, kvCfg.NShards)
	kv.T = kvCfg.T
	kv.D = kvCfg.D
	kv.I = kvCfg.I
	kv.W = kvCfg.W
	kv.E = kvCfg.E

	switch kvCfg.MasterRpcType {
	case "emulated":
		kv.smClerk = shardmaster.MakeMockRpcClerk(kvCfg.MasterRpcTypeData.([]*mockrpc.ClientEnd))
	case "httprpc":
		kv.smClerk = shardmaster.MakeHttpRpcClerk(kvCfg.Masters, kvCfg.T, kvCfg.D, kvCfg.I, kvCfg.W, kvCfg.E)
	default:
		kv.E.Panic(fmt.Sprintf("unknown rpc type %s, must be emulated or httprpc.", kvCfg.RpcType))
	}

	switch kvCfg.RpcType {
	case "emulated":
		kv.rpcServer = raft.NewMockRpcServer()
		kv.rpcClient = NewMockRpcClient(kvCfg.MakeEnd)
		kv.makePeerInfo = func(s string) *raft.PeerInfo {
			return &raft.PeerInfo{Host: "", Port: 0, Id: 0, Raw: s}
		}
	case "httprpc":
		kv.rpcServer = raft.NewHttpRpcServer(&kvCfg.RaftCfg.Self, kv.T, kv.D, kv.I, kv.W, kv.E)
		kv.rpcClient = raft.NewHttpRpcClient(kv.T, kv.D, kv.I, kv.W, kv.E)
		kv.makePeerInfo = DefaultMakePeerInfo
	default:
		kv.E.Panic(fmt.Sprintf("unknown rpc type %s, must be emulated or httprpc.", kvCfg.RpcType))
	}
	err := kv.rpcServer.Register(kv)
	if err != nil {
		kv.E.Fatalf("failed to register ShardKV on rpc server: %v", err)
	}
	kv.applyCh = make(chan raft.ApplyMsg, 4096)
	kv.reqCh = make(chan sKVOpReq, 4096)
	kv.quitCh = make(chan bool)
	kv.groupLeader = make(map[int]int)
	for i := 0; i < kvCfg.NShards; i++ {
		kv.stores[i] = KVStore{i, nil, nil, 0, nil}
	}
	kv.rf = raft.StartServer(kvCfg.RaftCfg, kv.applyCh)
	kv.disruptor = disruptor.Configure(ringSize).WithConsumerGroup(kv).Build()
	kv.disruptor.Start()
	go kv.serve()
	return kv
}
