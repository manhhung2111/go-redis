package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/wiring"
)

func main() {
	cfg := config.NewConfig()

	flag.StringVar(&cfg.Host, "host", cfg.Host, "host")
	flag.IntVar(&cfg.Port, "port", cfg.Port, "port")
	flag.BoolVar(&cfg.SnapshotEnabled, "snapshot", cfg.SnapshotEnabled, "enable periodic snapshots")
	flag.IntVar(&cfg.SnapshotIntervalSec, "snapshot-interval", cfg.SnapshotIntervalSec, "seconds between snapshots")
	flag.StringVar(&cfg.SnapshotDir, "snapshot-dir", cfg.SnapshotDir, "directory for the snapshot file")
	flag.StringVar(&cfg.SnapshotFilename, "snapshot-file", cfg.SnapshotFilename, "snapshot file name")
	flag.BoolVar(&cfg.SnapshotLoadOnStart, "snapshot-load", cfg.SnapshotLoadOnStart, "load the snapshot file on startup")
	flag.Parse()

	server, err := wiring.InitializeServer(cfg)
	if err != nil {
		panic(err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	go server.WaitingForSignals(sigCh)
	if err := server.Start(sigCh); err != nil {
		log.Fatal(err)
	}
	log.Println("server shutdown complete")
}
