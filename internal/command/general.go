package command

import (
	"github.com/manhhung2111/go-redis/internal/protocol"
	"github.com/manhhung2111/go-redis/internal/errors"
)

func (redis *redis) Ping(cmd protocol.RedisCmd) []byte {
	argsLen := len(cmd.Args)
	if argsLen > 1 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	if argsLen == 0 {
		return protocol.EncodeResp("PONG", true)
	}

	return protocol.EncodeResp(cmd.Args[0], false)
}
