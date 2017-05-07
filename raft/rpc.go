package raft

type RpcServer interface {
	Register(rcvr interface{}) error
	StartServer() error
	StopServer() error
}

type RpcClient interface {
	Call(peer *PeerInfo, serviceMethod string, args, reply interface{}) error
	Close() error
}
