package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"time"

	"io"
	"net/http"
	_ "net/http/pprof"

	"strconv"

	"github.com/pkg/profile"
	"github.com/zxjcarrot/ares/shardkv"
)

func setupLoggers(kvCfg *shardkv.ShardKVConfig, logDest io.Writer) {
	dest := ioutil.Discard

	prefix := fmt.Sprintf("ShardKV id %d TRACE: ", uint64(kvCfg.Gid*100)+kvCfg.RaftCfg.Self.Id)
	if kvCfg.Verbosity <= shardkv.VerbosityTrace {
		dest = logDest
	}
	kvCfg.T = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)

	prefix = fmt.Sprintf("ShardKV id %d DEBUG: ", uint64(kvCfg.Gid*100)+kvCfg.RaftCfg.Self.Id)
	if kvCfg.Verbosity <= shardkv.VerbosityDebug {
		dest = logDest
	}
	kvCfg.D = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)

	prefix = fmt.Sprintf("ShardKV id %d INFO: ", uint64(kvCfg.Gid*100)+kvCfg.RaftCfg.Self.Id)
	if kvCfg.Verbosity <= shardkv.VerbosityInfo {
		dest = logDest
	}
	kvCfg.I = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)

	prefix = fmt.Sprintf("ShardKV id %d WARNING: ", uint64(kvCfg.Gid*100)+kvCfg.RaftCfg.Self.Id)
	if kvCfg.Verbosity <= shardkv.VerbosityWarning {
		dest = logDest
	}
	kvCfg.W = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)

	prefix = fmt.Sprintf("ShardKV id %d ERROR: ", uint64(kvCfg.Gid*100)+kvCfg.RaftCfg.Self.Id)
	if kvCfg.Verbosity <= shardkv.VerbosityWarning {
		dest = logDest
	}
	kvCfg.E = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)
}

func main() {
	p := profile.Start(profile.ProfilePath("."), profile.NoShutdownHook)
	defer p.Stop()
	cfgFileNamePtr := flag.String("c", "shardkv.conf", "configuration file location")
	logDestPtr := flag.String("l", "", "log file location. This will override log file location provided in configuration file.")
	toStdoutPtr := flag.Bool("stdout", false, "Whether to write messages to stdout. This will override -l option.")
	flag.Parse()

	cfgSource, err := os.OpenFile(*cfgFileNamePtr, os.O_RDONLY, 0660)
	if err != nil {
		panic(fmt.Sprintf("error opening cfg file %s: %v", *cfgFileNamePtr, err))
	}
	cfg := shardkv.ParseConfigStream(cfgSource)

	var logDest io.Writer
	if *toStdoutPtr || *logDestPtr == "" && cfg.LogFilename == "" { // -stdout overrides -l and configruration file
		logDest = os.Stdout
	} else {
		logFilename := ""
		if *logDestPtr != "" { // -l overrides configruration file
			logFilename = *logDestPtr
		} else if cfg.LogFilename != "" {
			logFilename = cfg.LogFilename
		} else { // both not provided, fall back to default localtion
			logFilename = "shardkv.log"
		}
		logFile, err := os.OpenFile(logFilename, os.O_CREATE|os.O_APPEND, 0660)
		if err != nil {
			panic(fmt.Sprintf("error opening log file %s: %v", *logDestPtr, err))
		}
		fmt.Printf("log file set to %s", logFile.Name())
		logDest = logFile
	}
	//cfg.Verbosity = shardkv.VerbosityInfo
	setupLoggers(cfg, logDest)
	cfg.RaftCfg.T = cfg.T
	cfg.RaftCfg.D = cfg.D
	cfg.RaftCfg.I = cfg.I
	cfg.RaftCfg.W = cfg.W
	cfg.RaftCfg.E = cfg.E
	kv := shardkv.StartServer(cfg)
	c := make(chan os.Signal, 1)
	go http.ListenAndServe(":"+strconv.Itoa(cfg.Gid*100+int(cfg.RaftCfg.Self.Id)), http.DefaultServeMux)
	signal.Notify(c, os.Interrupt)
loop:
	for {
		select {
		case _ = <-c:
			log.Printf("signal Interrupt caught, stopping shardkv...")
			kv.Kill()
			log.Printf("shardkv stopped...")
			break loop
		case _ = <-time.After(10 * time.Second):
		}
	}
}
