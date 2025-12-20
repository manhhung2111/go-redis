package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/wiring"
)

func init() {
	flag.StringVar(&config.HOST, "host", "0.0.0.0", "host")
	flag.IntVar(&config.PORT, "port", 6379, "port")
	flag.Parse()
}

func main() {
	server, err := wiring.InitializeServer()
	if err != nil {
		panic(err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	go server.WaitingForSignals(sigCh)
	server.Start(sigCh)
}
