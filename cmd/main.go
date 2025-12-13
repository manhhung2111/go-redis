package main

import (
	"fmt"
	"github.com/manhhung2111/go-redis/internal/core"
	"net"
)

func main() {
	fmt.Println("Listening on port :6379")

	// Create a new server
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}

	// Listen for connections
	conn, err := l.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}

	defer conn.Close()

	for {
		resp := core.NewResp(conn)
		value, err := resp.Read()

		if err != nil {
			fmt.Println(err)
			return
		}

		_ = value

		writer := core.NewWriter(conn)
		writer.Write(core.Value{
			Typ: "string",
			Str: "OK",
		})
	}
}
