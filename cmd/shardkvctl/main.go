package main

import (
	"encoding/base64"
	"flag"
	"fmt"

	"log"
	"os"

	"strings"

	crand "crypto/rand"
	"strconv"

	"github.com/zxjcarrot/ares/raft"
	"github.com/zxjcarrot/ares/shardkv"
	"github.com/zxjcarrot/ares/shardmaster"
)

func usage() {
	fmt.Println()
	flag.Usage()
	os.Exit(1)
}

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func main() {
	masterAddrsPtr := flag.String("m", "", "master server addresses. Format: comma-separated host:port:id tuples")
	getOpPtr := flag.String("g", "", "get the key's value from the data store. Format: key")
	putOpPtr := flag.String("p", "", "put the k-v pair into the data store. Format: key=value")
	appendOpPtr := flag.String("a", "", "append value to a existing key. Format: key::value")
	gJoinOpPtr := flag.String("join", "", "add a shard group into the shardkv cluster. Format: gid=comma-separate host:port:id tuples")
	gLeaveOpPtr := flag.String("leave", "", "remove a shard group from the shardkv cluster. Format: gid")
	gQueryOpPtr := flag.String("query", "-1", "get shardkv cluster configuration. Format: config num, -1 to get the latest configuration.")
	randPutOpPtr := flag.Uint("r", 0, "put random k-v pairs into the datastore. Format: n, number of pairs")
	flag.Parse()
	l := log.New(os.Stdout, "shardkvctl ", log.LstdFlags)
	if *masterAddrsPtr == "" {
		l.Printf("Missing -m flag.")
		usage()
	}

	masters, err := raft.ParseMultiplePeerInfo(*masterAddrsPtr)
	if err != nil {
		panic(err)
	}

	shardKVCli := shardkv.MakeHttpRpcClerk(masters, shardkv.DefaultMakePeerInfo, l, l, l, l, l)
	masterCli := shardmaster.MakeHttpRpcClerk(masters, l, l, l, l, l)
	if *getOpPtr != "" {
		l.Printf("%s=%v", *getOpPtr, shardKVCli.Get(*getOpPtr))
	} else if *putOpPtr != "" {
		parts := strings.Split(*putOpPtr, "=")
		if len(parts) != 2 {
			l.Printf("Invalid format for -p")
			usage()
		}
		shardKVCli.Put(parts[0], parts[1])
	} else if *appendOpPtr != "" {
		parts := strings.Split(*appendOpPtr, ":")
		if len(parts) != 3 || parts[1] != "" {
			l.Printf("Invalid format for -a")
			usage()
		}
		shardKVCli.Append(parts[0], parts[2])
	} else if *gJoinOpPtr != "" {
		parts := strings.Split(*gJoinOpPtr, "=")
		if len(parts) != 2 {
			l.Printf("Invalid format for -join")
			usage()
		}
		gid, err := strconv.Atoi(parts[0])
		if err != nil {
			l.Printf("error parsing gid %s : %v", parts[0], err)
			usage()
		}
		peers, err := raft.ParseMultiplePeerInfo(parts[1])
		if err != nil {
			l.Printf("Invalid format for -join")
			usage()
		}
		servers := []string{}
		for i := range peers {
			servers = append(servers, peers[i].Raw)
		}
		masterCli.Join(map[int][]string{gid: servers})
	} else if *gLeaveOpPtr != "" {
		gid, err := strconv.Atoi(*gLeaveOpPtr)
		if err != nil {
			l.Printf("error parsing gid %s : %v", *gLeaveOpPtr, err)
			usage()
		}
		masterCli.Leave([]int{gid})
	} else if *gQueryOpPtr != "" {
		cfgNum, err := strconv.Atoi(*gQueryOpPtr)
		if err != nil {
			l.Printf("error parsing config num %s : %v", *gQueryOpPtr, err)
			usage()
		}
		l.Println(masterCli.Query(cfgNum))
	} else if *randPutOpPtr != 0 {
		for i := uint(0); i < *randPutOpPtr; i++ {
			key := randstring(16)
			value := randstring(16)
			shardKVCli.Put(key, value)
			l.Printf("put(%s, %s) => ok", key, value)
		}
	} else {
		l.Printf("At least one of  \"-g -p -a -r -join -leave -query\" is requried.")
		usage()
	}

}
