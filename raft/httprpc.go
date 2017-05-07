package raft

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"reflect"
	"strconv"
	"sync"
)

var mutex sync.Mutex
var regedTypes map[string]bool
var handledHttp bool

type HttpRpcServer struct {
	local         *PeerInfo
	ln            net.Listener
	T, D, I, W, E *log.Logger
	mu            sync.Mutex
}

type HttpRpcClient struct {
	clientMap     map[string]*rpc.Client
	T, D, I, W, E *log.Logger
	mu            sync.Mutex
}

func NewHttpRpcServer(local *PeerInfo, T, D, I, W, E *log.Logger) *HttpRpcServer {
	return &HttpRpcServer{
		local, nil,
		T, D, I, W, E,
		sync.Mutex{},
	}
}
func NewHttpRpcClient(T, D, I, W, E *log.Logger) *HttpRpcClient {
	return &HttpRpcClient{
		map[string]*rpc.Client{},
		T, D, I, W, E,
		sync.Mutex{},
	}
}

func (hrs *HttpRpcServer) Register(rcvr interface{}) error {
	mutex.Lock()
	defer mutex.Unlock()
	if regedTypes == nil {
		regedTypes = map[string]bool{}
	}
	typeName := reflect.TypeOf(rcvr).String()
	if _, exist := regedTypes[typeName]; exist == false {
		regedTypes[typeName] = true
		return rpc.Register(rcvr)
	} else {
		return nil
	}
}

func (hrc *HttpRpcClient) Call(peer *PeerInfo, serviceMethod string, args, reply interface{}) error {
	hrc.mu.Lock()
	client, ok := hrc.clientMap[peer.Raw]
	if ok == false {
		freshClient, err := rpc.DialHTTP("tcp", peer.Host+":"+strconv.Itoa(peer.Port))
		if err != nil {
			hrc.W.Printf("DailHttp to %s failed: %v.", peer.Host+":"+strconv.Itoa(peer.Port), err)
			hrc.mu.Unlock()
			return err
		}
		hrc.D.Printf("DailHttp to %s, ok.", peer.Host+":"+strconv.Itoa(peer.Port))
		hrc.clientMap[peer.Raw] = freshClient
		client = freshClient
	}
	hrc.mu.Unlock()
	err := client.Call(serviceMethod, args, reply)
	if err != nil {
		hrc.W.Printf("%s(%v) to %s failed: %v", serviceMethod, args, peer.Raw, err)
	}
	return err
}

func (hrc *HttpRpcClient) Close() error {
	hrc.mu.Lock()
	for _, c := range hrc.clientMap {
		c.Close()
	}
	hrc.clientMap = map[string]*rpc.Client{}
	hrc.mu.Unlock()
	return nil
}

func (hrs *HttpRpcServer) StartServer() error {
	mutex.Lock()
	defer mutex.Unlock()
	if handledHttp == false {
		rpc.HandleHTTP()
		handledHttp = true
	}

	ln, e := net.Listen("tcp", hrs.local.Host+":"+strconv.Itoa(hrs.local.Port))
	if e != nil {
		return e
	}
	hrs.ln = ln
	hrs.D.Printf("listening on %s", hrs.ln.Addr())
	go http.Serve(hrs.ln, nil)
	return nil
}

func (hrs *HttpRpcServer) StopServer() error {
	mutex.Lock()
	defer mutex.Unlock()
	if hrs.ln != nil {
		hrs.D.Printf("Stopping listening on %s", hrs.ln.Addr())
		return hrs.ln.Close()
	}
	return nil
}
