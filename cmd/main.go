package main

import (
	"flag"
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
	flag.Parse()

	server, err := wiring.InitializeServer(cfg)
	if err != nil {
		panic(err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	go server.WaitingForSignals(sigCh)
	server.Start(sigCh)
}
