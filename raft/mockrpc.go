package raft

import (
	"errors"
	"fmt"

	"github.com/zxjcarrot/ares/mockrpc"
)

type MockRpcServer struct {
}

type MockRpcClient struct {
	clientMap map[uint64]*mockrpc.ClientEnd
}

func (lrs *MockRpcServer) Register(rcvr interface{}) error {
	return nil
}

func (lrs *MockRpcServer) StartServer() error {
	return nil
}

func (hrs *MockRpcServer) StopServer() error {
	return nil
}

func NewMockRpcServer() *MockRpcServer {
	return &MockRpcServer{}
}

func NewMockRpcClient(peerMap map[uint64]*mockrpc.ClientEnd) *MockRpcClient {
	return &MockRpcClient{
		peerMap,
	}
}

func (lrs *MockRpcClient) Call(peer *PeerInfo, serviceMethod string, args, reply interface{}) error {
	ok := lrs.clientMap[peer.Id].Call(serviceMethod, args, reply)
	if ok {
		return nil
	} else {
		return errors.New(fmt.Sprintf("%s(%s) on %s failed", serviceMethod, args, peer.Raw))
	}
}

func (lrs *MockRpcClient) Close() error {
	return nil
}
