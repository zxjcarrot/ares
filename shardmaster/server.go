package shardmaster

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/zxjcarrot/ares/mockrpc"
	"github.com/zxjcarrot/ares/raft"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      uint64
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	rpcServer      raft.RpcServer
	rpcClient      raft.RpcClient
	reqCh          chan SMOpReq
	quitCh         chan bool
	lastAppliedIdx uint64
	persister      *raft.Persister
	configs        []Config // indexed by config num

	mCfg          *ShardMasterConfig
	T, D, I, W, E *log.Logger
}

// SMOpType is the type of the four RPCs
type SMOpType int

const (
	joinOP  SMOpType = 0
	leaveOP SMOpType = 1
	moveOP  SMOpType = 2
	queryOP SMOpType = 3
)

type Op struct {
	// Your data here.
	OPType SMOpType
	Data   []byte //gobbed args
}

type SMOpResp bool

type SMOpReq struct {
	reqID  int64
	ackID  int64
	opType SMOpType
	args   interface{}
	reply  interface{}
	respCh chan SMOpResp
}

func (sm *ShardMaster) Join(args JoinArgs, reply *JoinReply) error {
	// Your code here.
	respCh := make(chan SMOpResp, 1)
	req := SMOpReq{args.ReqID, args.AckID, joinOP, args, reply, respCh}
	sm.reqCh <- req
	select {
	case v := <-respCh:
		if v == false {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		}
	case _ = <-time.After(1 * time.Second):
		sm.I.Printf("Join(%v) timed out after 1s", args)
		reply.Err = ErrTimedout
	}
	return nil
}

func (sm *ShardMaster) Leave(args LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	respCh := make(chan SMOpResp, 1)
	req := SMOpReq{args.ReqID, args.AckID, leaveOP, args, reply, respCh}
	sm.reqCh <- req
	select {
	case v := <-respCh:
		if v == false {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		}
	case _ = <-time.After(1 * time.Second):
		sm.I.Printf("Leave(%v) timed out after 1s", args)
		reply.Err = ErrTimedout
	}
	return nil
}

func (sm *ShardMaster) Move(args MoveArgs, reply *MoveReply) error {
	// Your code here.
	respCh := make(chan SMOpResp, 1)
	req := SMOpReq{args.ReqID, args.AckID, moveOP, args, reply, respCh}
	sm.reqCh <- req
	select {
	case v := <-respCh:
		if v == false {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		}
	case _ = <-time.After(1 * time.Second):
		sm.I.Printf("Move(%v) timed out after 1s", args)
		reply.Err = ErrTimedout
	}
	return nil
}

func (sm *ShardMaster) Query(args QueryArgs, reply *QueryReply) error {
	// Your code here.
	respCh := make(chan SMOpResp, 1)
	req := SMOpReq{args.ReqID, args.AckID, queryOP, args, reply, respCh}
	sm.reqCh <- req
	select {
	case v := <-respCh:
		if v == false {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		}
	case _ = <-time.After(1 * time.Second):
		sm.I.Printf("Query(%v) timed out after 1s", args)
		reply.Err = ErrTimedout
	}
	return nil
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	// Your code here, if desired.
	sm.rf.Kill()
	sm.quitCh <- true
}

func (sm *ShardMaster) Cleanup() {
	sm.mu.Lock()
	sm.rf.Cleanup()
	sm.mu.Unlock()
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

type ReplyHelper struct {
	respCh chan SMOpResp
	reply  interface{}
	term   uint64
	reqID  int64
}

func (sm *ShardMaster) handleApplyJoinOp(applyMsg *raft.ApplyMsg, op *Op, reqId int64, ackId int64, idxToReplyHelperMap map[uint64]ReplyHelper, reqIdMap map[int64]bool) {
	idx, applyTerm := applyMsg.Index, applyMsg.Term

	if _, ok := reqIdMap[reqId]; ok == true {
		sm.T.Printf("duplicate Join request(%d, %d), return without executing the command", idx, applyTerm)
	} else {
		args := UngobJoinArgs(op.Data)
		//TODO implement Join semantics
		oldConfig := sm.configs[len(sm.configs)-1]
		sb := NewShardConfigBalancer(oldConfig, oldConfig.Num+1)
		sb.AddGroups(args.Servers)
		newConfig := sb.GetConfig()
		sm.configs = append(sm.configs, newConfig)
		sm.I.Printf("Join request %v, old config %v, new config %v", args, oldConfig, newConfig)
		reqIdMap[reqId] = true
	}
	if _, ok := reqIdMap[ackId]; ok {
		delete(reqIdMap, ackId) // delete acked request id to free memory
		//sm.T.Printf("deleted processed request %v", kv.me, ackId)
	}

	helper, exist := idxToReplyHelperMap[idx]

	if exist == false { // applied in a follower node, no need for reply
		//sm.I.Printf("Applying JoinOP at index %d, no helper found", kv.me, idx)
		return
	}
	delete(idxToReplyHelperMap, idx)
	if helper.reqID != reqId {
		sm.I.Printf("Applying joinOP at index %d, reqId %d for the helper mismatches with the one %d in the log", idx, helper.reqID, reqId)
		helper.respCh <- false
		return
	}

	reply := helper.reply.(*JoinReply)
	if helper.term != applyTerm {
		// This is the case in which a leader has called Start() for a client RPC
		// but lost its leadership before the request were committed to the log.
		sm.I.Printf("JoinOP, lost leadership at term %d before committing the entry %#v", helper.term, applyMsg)
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
		reply.WrongLeader = false
	}
	helper.respCh <- true
}

func (sm *ShardMaster) handleApplyLeaveOp(applyMsg *raft.ApplyMsg, op *Op, reqId int64, ackId int64, idxToReplyHelperMap map[uint64]ReplyHelper, reqIdMap map[int64]bool) {
	idx, applyTerm := applyMsg.Index, applyMsg.Term

	if _, ok := reqIdMap[reqId]; ok == true {
		sm.T.Printf("duplicate Leave request(%d, %d), return without executing the command", idx, applyTerm)
	} else {
		args := UngobLeaveArgs(op.Data)
		oldConfig := sm.configs[len(sm.configs)-1]
		sb := NewShardConfigBalancer(oldConfig, oldConfig.Num+1)
		sb.RemoveGroups(args.GIDs)
		newConfig := sb.GetConfig()
		sm.configs = append(sm.configs, newConfig)
		sm.I.Printf("Leave request %v, old config %v, new config %v", args, oldConfig, newConfig)
		reqIdMap[reqId] = true
	}
	if _, ok := reqIdMap[ackId]; ok {
		delete(reqIdMap, ackId) // delete acked request id to free memory
		//sm.T.Printf("deleted processed request %v", kv.me, ackId)
	}

	helper, exist := idxToReplyHelperMap[idx]

	if exist == false { // applied in a follower node, no need for reply
		//sm.I.Printf("Applying LeaveOP at index %d, no helper found", kv.me, idx)
		return
	}
	delete(idxToReplyHelperMap, idx)
	if helper.reqID != reqId {
		sm.I.Printf("Applying leaveOP at index %d, reqId %d for the helper mismatches with the one %d in the log", idx, helper.reqID, reqId)
		helper.respCh <- false
		return
	}

	reply := helper.reply.(*LeaveReply)
	if helper.term != applyTerm {
		// This is the case in which a leader has called Start() for a client RPC,
		// but lost its leadership before the request were committed to the log.
		sm.W.Printf("LeaveOP, lost leadership at term %d before committing the entry %#v", helper.term, applyMsg)
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
		reply.WrongLeader = false
	}
	helper.respCh <- true
}

func (sm *ShardMaster) handleApplyMoveOp(applyMsg *raft.ApplyMsg, op *Op, reqId int64, ackId int64, idxToReplyHelperMap map[uint64]ReplyHelper, reqIdMap map[int64]bool) {
	idx, applyTerm := applyMsg.Index, applyMsg.Term

	if _, ok := reqIdMap[reqId]; ok == true {
		sm.T.Printf("duplicate Move request(%d, %d), return without executing the command", idx, applyTerm)
	} else {
		args := UngobMoveArgs(op.Data)
		oldConfig := sm.configs[len(sm.configs)-1]
		sb := NewShardConfigBalancer(oldConfig, oldConfig.Num+1)
		sb.MoveShard(args.Shard, args.GID)
		newConfig := sb.GetConfig()
		sm.configs = append(sm.configs, newConfig)
		sm.I.Printf("Move request %v, old config %v, new config %v", args, oldConfig, newConfig)
	}
	if _, ok := reqIdMap[ackId]; ok {
		delete(reqIdMap, ackId) // delete acked request id to free memory
		//sm.T.Printf("deleted processed request %v", kv.me, ackId)
	}

	helper, exist := idxToReplyHelperMap[idx]

	if exist == false { // applied in a follower node, no need for reply
		//sm.I.Printf("Applying MoveOP at index %d, no helper found", kv.me, idx)
		return
	}
	delete(idxToReplyHelperMap, idx)
	if helper.reqID != reqId {
		sm.I.Printf("Applying MoveOP at index %d, reqId %d for the helper mismatches with the one %d in the log", idx, helper.reqID, reqId)
		helper.respCh <- false
		return
	}

	reply := helper.reply.(*MoveReply)
	if helper.term != applyTerm {
		// This is the case in which a leader has called Start() for a client RPC,
		// but lost its leadership before the request were committed to the log.
		sm.W.Printf("MoveOP, lost leadership at term %d before committing the entry %#v", helper.term, applyMsg)
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
		reply.WrongLeader = false
	}
	helper.respCh <- true
}

func (sm *ShardMaster) handleApplyQueryOp(applyMsg *raft.ApplyMsg, op *Op, reqId int64, ackId int64, idxToReplyHelperMap map[uint64]ReplyHelper, reqIdMap map[int64]bool) {
	idx, applyTerm := applyMsg.Index, applyMsg.Term

	if _, ok := reqIdMap[reqId]; ok == true {
		sm.T.Printf("duplicate Query request(%d, %d), return desired config anyway", idx, applyTerm)
	}
	if _, ok := reqIdMap[ackId]; ok {
		delete(reqIdMap, ackId) // delete acked request id to free memory
		//sm.T.Printf("deleted processed request %v", kv.me, ackId)
	}

	helper, exist := idxToReplyHelperMap[idx]

	if exist == false { // applied in a follower node, no need for reply
		//sm.T.Printf("Applying LeaveOP at index %d, no helper found", kv.me, idx)
		return
	}
	delete(idxToReplyHelperMap, idx)
	if helper.reqID != reqId {
		sm.I.Printf("Applying leaveOP at index %d, reqId %d for the helper mismatches with the one %d in the log", idx, helper.reqID, reqId)
		helper.respCh <- false
		return
	}

	reply := helper.reply.(*QueryReply)
	if helper.term != applyTerm {
		// This is the case in which a leader has called Start() for a client RPC,
		// but lost its leadership before the request were committed to the log.
		sm.W.Printf("LeaveOP, lost leadership at term %d before committing the entry %#v", helper.term, applyMsg)
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
	} else {
		reply.WrongLeader = false
		args := UngobQueryArgs(op.Data)
		lastConfig := sm.configs[len(sm.configs)-1]
		num := args.Num
		if num == -1 {
			num = lastConfig.Num
		}
		if 0 <= num && num <= lastConfig.Num {
			reply.Config = CopyConfig(sm.configs[num])
			reply.Err = OK
			sm.T.Printf("Query return config#%d %v", num, reply.Config)
		} else {
			reply.Config = CopyConfig(lastConfig)
			reply.Err = OK
			sm.I.Printf("Query request with num=%d, out of bound[0-%d], return the latest config!", num, lastConfig.Num)
		}
	}
	helper.respCh <- true

}

func (sm *ShardMaster) serve() {
	// for duplicate detection
	idxToReplyHelperMap := make(map[uint64]ReplyHelper)
	reqIdMap := make(map[int64]bool)
	snapshotChkTimer := time.NewTicker(sm.mCfg.SnapshotChkInterval)
	var tryTakeSnapshot func()
	if sm.mCfg.MaxRaftStateSize > 0 {
		tryTakeSnapshot = func() {
			if float64(sm.rf.RaftStateSize())*sm.mCfg.SnapshotThresh >= float64(sm.mCfg.MaxRaftStateSize) {
				b := new(bytes.Buffer)
				e := gob.NewEncoder(b)
				e.Encode(reqIdMap) // for duplicate detection across crushes
				e.Encode(sm.configs)
				sm.rf.SaveSnapshot(b.Bytes(), sm.lastAppliedIdx)
			}
		}
	} else {
		tryTakeSnapshot = func() {}
	}

	for {
		select {
		case _ = <-sm.quitCh:
			sm.I.Printf("ShardMaster.serve() stopped")
			return
		case _ = <-snapshotChkTimer.C:
			tryTakeSnapshot()
		case applyMsg := <-sm.applyCh:
			if applyMsg.UseSnapshot {
				d := gob.NewDecoder(bytes.NewBuffer(applyMsg.Snapshot))
				reqIdMap = make(map[int64]bool)
				d.Decode(&reqIdMap)
				sm.configs = make([]Config, 1)
				d.Decode(&sm.configs)
				sm.I.Printf("applied sm.configs with snapshot[%d]", sm.lastAppliedIdx)
			} else {
				op := Op{}
				reqId := int64(0)
				ackId := int64(0)
				r := bytes.NewBuffer(applyMsg.Command.([]byte))
				d := gob.NewDecoder(r)
				d.Decode(&op)
				d.Decode(&reqId)
				d.Decode(&ackId)
				/*
				* For followers, simply apply commands without answering any client requests.
				 */
				switch op.OPType {
				case joinOP:
					sm.handleApplyJoinOp(&applyMsg, &op, reqId, ackId, idxToReplyHelperMap, reqIdMap)
				case leaveOP:
					sm.handleApplyLeaveOp(&applyMsg, &op, reqId, ackId, idxToReplyHelperMap, reqIdMap)
				case moveOP:
					sm.handleApplyMoveOp(&applyMsg, &op, reqId, ackId, idxToReplyHelperMap, reqIdMap)
				case queryOP:
					sm.handleApplyQueryOp(&applyMsg, &op, reqId, ackId, idxToReplyHelperMap, reqIdMap)
				}
			}
			sm.lastAppliedIdx = applyMsg.Index
			tryTakeSnapshot()
		case req := <-sm.reqCh:
			switch req.opType {
			case joinOP:
				args := req.args.(JoinArgs)
				reply := req.reply.(*JoinReply)
				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				e.Encode(Op{joinOP, GobJoinArgs(args)})
				e.Encode(req.reqID) // for detecting duplicates across term boundary
				e.Encode(req.ackID)
				idx, term, isLeader := sm.rf.Start(w.Bytes())
				if isLeader == false {
					req.respCh <- SMOpResp(false)
				} else {
					sm.I.Printf("stored joinOP %v at index %d, term %d, reqId %d ", args, idx, term, req.reqID)
					idxToReplyHelperMap[idx] = ReplyHelper{req.respCh, reply, term, req.reqID}
				}
			case leaveOP:
				args := req.args.(LeaveArgs)
				reply := req.reply.(*LeaveReply)
				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				e.Encode(Op{leaveOP, GobLeaveArgs(args)})
				e.Encode(req.reqID) // for detecting duplicates across term boundary
				e.Encode(req.ackID)
				idx, term, isLeader := sm.rf.Start(w.Bytes())
				if isLeader == false {
					req.respCh <- SMOpResp(false)
				} else {
					sm.T.Printf("stored leaveOP %v at index %d, term %d, reqId %d ", args, idx, term, req.reqID)
					idxToReplyHelperMap[idx] = ReplyHelper{req.respCh, reply, term, req.reqID}
				}
			case moveOP:
				args := req.args.(MoveArgs)
				reply := req.reply.(*MoveReply)
				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				e.Encode(Op{moveOP, GobMoveArgs(args)})
				e.Encode(req.reqID) // for detecting duplicates across term boundary
				e.Encode(req.ackID)
				idx, term, isLeader := sm.rf.Start(w.Bytes())
				if isLeader == false {
					req.respCh <- SMOpResp(false)
				} else {
					sm.T.Printf("stored moveOP %v at index %d, term %d, reqId %d ", args, idx, term, req.reqID)
					idxToReplyHelperMap[idx] = ReplyHelper{req.respCh, reply, term, req.reqID}
				}
			case queryOP:
				args := req.args.(QueryArgs)
				reply := req.reply.(*QueryReply)
				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				e.Encode(Op{queryOP, GobQueryArgs(args)})
				e.Encode(req.reqID) // for detecting duplicates across term boundary
				e.Encode(req.ackID)
				idx, term, isLeader := sm.rf.Start(w.Bytes())
				if isLeader == false {
					req.respCh <- SMOpResp(false)
				} else {
					sm.T.Printf("stored queryOP %v at index %d, term %d, reqId %d ", args, idx, term, req.reqID)
					idxToReplyHelperMap[idx] = ReplyHelper{req.respCh, reply, term, req.reqID}
				}
			}
			tryTakeSnapshot()
		}
	}
}

func StartServer(mCfg *ShardMasterConfig) *ShardMaster {
	gob.Register(Op{})
	sm := new(ShardMaster)
	sm.me = mCfg.RaftCfg.Self.Id
	sm.mCfg = mCfg
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.T = mCfg.T
	sm.D = mCfg.D
	sm.I = mCfg.I
	sm.W = mCfg.W
	sm.E = mCfg.E
	switch mCfg.RpcType {
	case "emulated":
		sm.rpcServer = raft.NewMockRpcServer()
		sm.rpcClient = raft.NewMockRpcClient(mCfg.RpcTypeData.(map[uint64]*mockrpc.ClientEnd))
	case "httprpc":
		sm.rpcServer = raft.NewHttpRpcServer(&mCfg.RaftCfg.Self, sm.T, sm.D, sm.I, sm.W, sm.E)
		sm.rpcClient = raft.NewHttpRpcClient(sm.T, sm.D, sm.I, sm.W, sm.E)
	default:
		sm.E.Panic(fmt.Sprintf("unknown rpc type %s, must be emulated or httprpc.", mCfg.RpcType))
	}
	err := sm.rpcServer.Register(sm)
	if err != nil {
		sm.E.Fatalf("failed to register ShardMaster on rpc server: %v", err)
	}
	sm.applyCh = make(chan raft.ApplyMsg, 1024)
	sm.reqCh = make(chan SMOpReq)
	sm.quitCh = make(chan bool)
	sm.persister = mCfg.Persister
	sm.rf = raft.StartServer(mCfg.RaftCfg, sm.applyCh)
	go sm.serve()
	return sm
}
