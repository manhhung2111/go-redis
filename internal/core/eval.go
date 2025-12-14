package core

import (
	"errors"
	"fmt"
	"io"
)

func handlePINGCommand(args []string, conn io.ReadWriter) error {
	var buf []byte

	argsLen := len(args)
	if argsLen > 1 {
		return errors.New("'PING' command only takes 1 argument")
	}

	if argsLen == 0 {
		buf = Encode("PONG", true)
	} else {
		buf = Encode(args[0], false)
	}

	_, err := conn.Write(buf)
	return err
}

func HandleCommandAndResponse(cmd *RedisCmd, conn io.ReadWriter) error {
	switch cmd.Cmd {
	case "PING":
		return handlePINGCommand(cmd.Args, conn)
	}
	return errors.New(fmt.Sprintf("command not supported: %s", cmd.Cmd))
}
