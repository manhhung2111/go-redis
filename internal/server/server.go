package server

import (
	"fmt"
	"io"
	"log"
	"net"
	"strconv"

	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/core"
)

func StartServer() {
	log.Println("starting a TCP server listening on", config.HOST, config.PORT)
	listener, err := net.Listen("tcp", config.HOST+":"+strconv.Itoa(config.PORT))
	if err != nil {
		panic(err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		log.Println("a new client connected with address:", conn.RemoteAddr())

		for {
			var buf []byte = make([]byte, 512)
			n, err := conn.Read(buf)

			if err != nil {
				panic(err)
			}

			redisCmd, err := core.ParseCmd(buf[:n])
			if err != nil {
				conn.Close()
				if err == io.EOF {
					break
				}
				log.Println("error occurred when trying to parse command", err)
			}

			err = core.HandleCommandAndResponse(redisCmd, conn)
			if err != nil {
				conn.Write([]byte(fmt.Sprintf("-%s%s", err, core.CRLF)))
			}
		}
	}
}
