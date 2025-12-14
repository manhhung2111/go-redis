package command

import (
	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/util"
)

func HandleCommandAndResponse(cmd core.RedisCmd, redis IRedis) []byte {
	switch cmd.Cmd {
	case "PING":
		return redis.Ping(cmd)
	case "SET":
		return redis.Set(cmd)
	case "GET":
		return redis.Get(cmd)
	case "DEL":
		return redis.Del(cmd)
	}
	return core.EncodeResp(util.InvalidCommand(cmd.Cmd), false)
}
