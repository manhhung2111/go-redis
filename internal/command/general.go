package command

import (
	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/util"
)

func (redis *redis) Ping(cmd core.RedisCmd) []byte {
	argsLen := len(cmd.Args)
	if argsLen > 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	if argsLen == 0 {
		return core.EncodeResp("PONG", true)
	}

	return core.EncodeResp(cmd.Args[0], false)
}
