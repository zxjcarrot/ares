package main

import (
	"flag"
	"math"
	"os"
	"testing"
	"time"

	"log"

	"io/ioutil"

	"sort"

	"github.com/zxjcarrot/ares/raft"
	"github.com/zxjcarrot/ares/shardkv"
)

var masterAddrsPtr *string
var masters []raft.PeerInfo
var nclientsPtr *int
var nclients int
var l *log.Logger
var d *log.Logger

func init() {
	masterAddrsPtr = flag.String("m", "", "master server addresses. Format: comma-separated host:port:id tuples")
	nclientsPtr = flag.Int("n", 128, "number of conccurent clients")
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
	l = log.New(os.Stdout, "perf ", log.LstdFlags)
	d = log.New(ioutil.Discard, "perf ", log.LstdFlags)
}

func BenchmarkPutAndGet(b *testing.B) {
	l.Printf("b.N: %v", b.N)
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
			value := shardKVCli.Get(key)
			end2 := time.Now()
			delta = (end2.UnixNano() - end.UnixNano()) / 1000
			latencyMap[delta] = latencyMap[delta] + 1
			if expValue != value {
				b.Fatalf("expected %s => %s, got %s => %s", key, expValue, key, value)
			}
		}
		doneCh <- latencyMap
	}
	taskCh := make(chan bool, nclients)
	doneCh := make(chan map[int64]int, nclients)
	for i := 0; i < nclients; i++ {
		go runner(taskCh, doneCh)
	}
	for i := 0; i < b.N; i++ {
		taskCh <- true
	}
	for i := 0; i < nclients; i++ {
		taskCh <- false
	}

	maxLat := int64(math.MinInt64)
	minLat := int64(math.MaxInt64)
	count := int64(0)
	totalLat := int64(0)
	latMap := map[int64]int{}
	lats := []int{}
	for i := 0; i < nclients; i++ {
		m := <-doneCh

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
}

/*
func BenchmarkGet(b *testing.B) {
	runner := func(taskCh chan bool, doneCh chan bool) {
		shardKVCli := shardkv.MakeHttpRpcClerk(masters, shardkv.DefaultMakePeerInfo, d, d, l, l, l)
		defer shardKVCli.Close()
		for v := range taskCh {
			if v == false {
				break
			}
			key := randstring(8)
			shardKVCli.Get(key)
		}

		doneCh <- true
	}
	taskCh := make(chan bool, nclients)
	doneCh := make(chan bool, nclients)
	for i := 0; i < nclients; i++ {
		go runner(taskCh, doneCh)
	}
	for i := 0; i < b.N; i++ {
		taskCh <- true
	}
	for i := 0; i < nclients; i++ {
		taskCh <- false
	}
	for i := 0; i < nclients; i++ {
		_ = <-doneCh
	}
}
*/
/*
func BenchmarkBufferedChan(b *testing.B) {
	chans := make([]chan int, 0, b.N)
	for i := 0; i < b.N; i++ {
		chans = append(chans, make(chan int, 1))
	}
	for i := 0; i < b.N; i++ {
		chans[i] <- 1
	}
}

func BenchmarkLaunchGoRoutines(b *testing.B) {
	for i := 0; i < b.N; i++ {
		go func() int {
			i := 1
			j := i * 2
			return j
		}()
	}
}

*/
