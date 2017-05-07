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

	"github.com/zxjcarrot/ares/shardmaster"
)

func setupLoggers(mCfg *shardmaster.ShardMasterConfig, logDest io.Writer) {
	dest := ioutil.Discard

	prefix := fmt.Sprintf("Master id %d TRACE: ", mCfg.RaftCfg.Self.Id)
	if mCfg.Verbosity <= shardmaster.VerbosityTrace {
		dest = logDest
	}
	mCfg.T = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)

	prefix = fmt.Sprintf("Master id %d DEBUG: ", mCfg.RaftCfg.Self.Id)
	if mCfg.Verbosity <= shardmaster.VerbosityDebug {
		dest = logDest
	}
	mCfg.D = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)

	prefix = fmt.Sprintf("Master id %d INFO: ", mCfg.RaftCfg.Self.Id)
	if mCfg.Verbosity <= shardmaster.VerbosityInfo {
		dest = logDest
	}
	mCfg.I = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)

	prefix = fmt.Sprintf("Master id %d WARNING: ", mCfg.RaftCfg.Self.Id)
	if mCfg.Verbosity <= shardmaster.VerbosityWarning {
		dest = logDest
	}
	mCfg.W = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)

	prefix = fmt.Sprintf("Master id %d ERROR: ", mCfg.RaftCfg.Self.Id)
	if mCfg.Verbosity <= shardmaster.VerbosityWarning {
		dest = logDest
	}
	mCfg.E = log.New(dest, prefix, log.Ltime|log.Lshortfile|log.Lmicroseconds)
}

func main() {
	cfgFileNamePtr := flag.String("c", "master.conf", "master configuration file location")
	logDestPtr := flag.String("l", "", "log file location. This will override log file location provided in configuration file.")
	toStdoutPtr := flag.Bool("stdout", false, "Whether to write messages to stdout. This will override -l option.")
	flag.Parse()

	cfgSource, err := os.OpenFile(*cfgFileNamePtr, os.O_RDONLY, 0660)
	if err != nil {
		panic(fmt.Sprintf("error opening cfg file %s: %v", *cfgFileNamePtr, err))
	}
	cfg := shardmaster.ParseConfigStream(cfgSource)

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
			logFilename = "master.log"
		}
		logFile, err := os.OpenFile(logFilename, os.O_APPEND, 0660)
		if err != nil {
			panic(fmt.Sprintf("error opening log file %s: %v", *logDestPtr, err))
		}
		fmt.Printf("log file set to %s", logFile.Name())
		logDest = logFile
	}
	cfg.Verbosity = shardmaster.VerbosityDebug
	setupLoggers(cfg, logDest)
	cfg.RaftCfg.T = cfg.T
	cfg.RaftCfg.D = cfg.D
	cfg.RaftCfg.I = cfg.I
	cfg.RaftCfg.W = cfg.W
	cfg.RaftCfg.E = cfg.E
	kv := shardmaster.StartServer(cfg)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
loop:
	for {
		select {
		case _ = <-c:
			log.Printf("signal Interrupt caught, stopping master...")
			kv.Kill()
			log.Printf("master stopped...")
			break loop
		case _ = <-time.After(10 * time.Second):
		}
	}
}
