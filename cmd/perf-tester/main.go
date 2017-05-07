package main

import (
	"encoding/base64"
	"flag"
	"math"
	"os"
	"sort"
	"time"

	crand "crypto/rand"
	"io/ioutil"
	"log"

	"github.com/zxjcarrot/ares/raft"
	"github.com/zxjcarrot/ares/shardkv"
)

var masterAddrsPtr *string
var masters []raft.PeerInfo
var nclientsPtr *int
var nclients int
var nOpsPtr *int
var nOps int
var l *log.Logger
var d *log.Logger

func init() {
	masterAddrsPtr = flag.String("m", "", "master server addresses. Format: comma-separated host:port:id tuples")
	nclientsPtr = flag.Int("clients", 128, "number of conccurent clients")
	nOpsPtr = flag.Int("nops", 100000, "total number of operations")
	flag.Parse()
	if *masterAddrsPtr == "" {
		panic("-m is required")
	}
	masterInfos, err := raft.ParseMultiplePeerInfo(*masterAddrsPtr)
	if err != nil {
		panic(err)
	}
	masters = masterInfos
	nclients = *nclientsPtr
	nOps = *nOpsPtr
	l = log.New(os.Stdout, "perf ", log.LstdFlags)
	d = log.New(ioutil.Discard, "perf ", log.LstdFlags)
}

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func main() {
	runner := func(taskCh chan bool, doneCh chan map[int64]int) {
		shardKVCli := shardkv.MakeHttpRpcClerk(masters, shardkv.DefaultMakePeerInfo, d, d, l, l, l)
		defer shardKVCli.Close()
		latencyMap := map[int64]int{}
		for v := range taskCh {
			if v == false {
				break
			}
			start := time.Now()
			key := randstring(8)
			expValue := randstring(256)
			shardKVCli.Put(key, expValue)
			end := time.Now()
			delta := (end.UnixNano() - start.UnixNano()) / 1000
			latencyMap[delta] = latencyMap[delta] + 1
			/*
				value := shardKVCli.Get(key)

				end2 := time.Now()
				delta = (end2.UnixNano() - end.UnixNano()) / 1000
				latencyMap[delta] = latencyMap[delta] + 1

				if expValue != value {
					b.Fatalf("expected %s => %s, got %s => %s", key, expValue, key, value)
				}*/
		}
		doneCh <- latencyMap
	}
	taskCh := make(chan bool, nclients)
	doneCh := make(chan map[int64]int, nclients)
	for i := 0; i < nclients; i++ {
		go runner(taskCh, doneCh)
	}
	start := time.Now()
	for i := 0; i < nOps; i++ {
		taskCh <- true
	}
	for i := 0; i < nclients; i++ {
		taskCh <- false
	}

	latMaps := []map[int64]int{}
	maxLat := int64(math.MinInt64)
	minLat := int64(math.MaxInt64)
	count := int64(0)
	totalLat := int64(0)
	latMap := map[int64]int{}
	lats := []int{}
	for i := 0; i < nclients; i++ {
		m := <-doneCh
		latMaps = append(latMaps, m)
		l.Printf("client%d done", i)
	}
	l.Printf("all clients done")
	end := time.Now()
	for _, m := range latMaps {
		for k, v := range m {
			latMap[k] += v
			if k > maxLat {
				maxLat = k
			}
			if k < minLat {
				minLat = k
			}
			count += int64(v)
			totalLat += k * int64(v)
			for i := 0; i < v; i++ {
				lats = append(lats, int(k))
			}
		}
	}
	sort.Ints(lats)
	p80 := lats[int(0.80*float64(len(lats)))]
	p90 := lats[int(0.90*float64(len(lats)))]
	p95 := lats[int(0.95*float64(len(lats)))]
	p99 := lats[int(0.99*float64(len(lats)))]
	l.Printf("Min Latency: %vus, Max Latency: %vus, Avg Latency: %vus\n80th percentile: %vus, 90th percentile: %vus, 95th percentile: %vus, 99th percentile: %vus\n", minLat, maxLat, totalLat/count, p80, p90, p95, p99)
	elapsedTime := float64(end.UnixNano()-start.UnixNano()) / 1000000000.0
	l.Printf("took %vs, tsp: %v op/s\n", elapsedTime, float64(nOps)/elapsedTime)
}
