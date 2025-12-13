package main

import (
	"flag"
	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/server"
)

func init() {
	flag.StringVar(&config.HOST, "host", "0.0.0.0", "host")
	flag.IntVar(&config.PORT, "port", 6379, "port")
	flag.Parse()
}

func main() {
	server.StartServer()
}
