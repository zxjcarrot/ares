package shardkv

import (
	"errors"
	"fmt"

	"github.com/zxjcarrot/ares/mockrpc"
	"github.com/zxjcarrot/ares/raft"
)

type MockRpcClient struct {
	makeEnd func(string) *mockrpc.ClientEnd
}

func NewMockRpcClient(makeEnd func(string) *mockrpc.ClientEnd) *MockRpcClient {
	return &MockRpcClient{
		makeEnd,
	}
}

func (lrs *MockRpcClient) Call(peer *raft.PeerInfo, serviceMethod string, args, reply interface{}) error {
	ok := lrs.makeEnd(peer.Raw).Call(serviceMethod, args, reply)
	if ok {
		return nil
	} else {
		return errors.New(fmt.Sprintf("%s(%s) on %s failed", serviceMethod, args, peer.Raw))
	}
}

func (lrs *MockRpcClient) Close() error {
	return nil
}
