package command

import (
	"strconv"

	"github.com/manhhung2111/go-redis/internal/constant"
	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/util"
)

/* Supports `GET key` */
func (redis *Redis) Get(cmd core.RedisCmd) []byte {
	argsLen := len(cmd.Args)
	if argsLen != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	value, exists := redis.Store.Get(cmd.Args[0])
	if !exists {
		return constant.RESP_NIL_BULK_STRING
	}

	return core.EncodeResp(value, false)
}

/* Supports `SET key value [EX seconds]` */
func (redis *Redis) Set(cmd core.RedisCmd) []byte {
	argsLen := len(cmd.Args)
	if argsLen < 2 || argsLen == 3 || argsLen > 4 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	args := cmd.Args
	key, value := args[0], args[1]

	if argsLen > 2 {
		option := args[2]

		switch option {
		case "EX":
			ttlSeconds, err := strconv.ParseInt(args[3], 10, 64)
			if err != nil {
				return core.EncodeResp(err, false)
			}

			redis.Store.SetEx(key, value, ttlSeconds)
		default:
			return core.EncodeResp(util.InvalidCommandOption(option, cmd.Cmd), false)
		}
	} else {
		redis.Store.Set(key, value)
	}

	return constant.RESP_OK
}

func (redis *Redis) Del(cmd core.RedisCmd) []byte {
	argsLen := len(cmd.Args)
	if argsLen < 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	var deletedKeys int64 = 0
	for i := range argsLen {
		if isDeleted := redis.Store.Del(cmd.Args[i]); isDeleted {
			deletedKeys++
		}
	}

	return core.EncodeResp(deletedKeys, false)
}
