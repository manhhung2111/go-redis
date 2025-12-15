package command

import (
	"strconv"
	"strings"
	"time"

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

	str, ok := value.(string)
	if !ok {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	return core.EncodeResp(str, false)
}

/* Supports `SET key value [NX | XX] [EX seconds]` */
func (redis *Redis) Set(cmd core.RedisCmd) []byte {
	argsLen := len(cmd.Args)
	if argsLen < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	args := cmd.Args
	key, value := args[0], args[1]

	var (
		nx        bool // SET if Not eXists
		xx        bool // SET if eXists
		expireSec int64
	)

	for i := 2; i < argsLen; i++ {
		opt := strings.ToUpper(args[i])
		switch opt {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "EX":
			if i+1 >= argsLen {
				return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
			}

			ttl, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil || ttl <= 0 {
				return core.EncodeResp(util.InvalidExpireTime(cmd.Cmd), false)
			}

			expireSec = ttl
			i++
		default:
			return core.EncodeResp(util.InvalidCommandOption(opt, cmd.Cmd), false)
		}
	}

	if nx && xx {
		return core.EncodeResp(util.InvalidCommandOption("NX|XX", cmd.Cmd), false)
	}

	entry := redis.Store.GetEntry(key)

	if nx && entry != nil {
		return constant.RESP_NIL_BULK_STRING
	}

	if xx && entry == nil {
		return constant.RESP_NIL_BULK_STRING
	}

	if expireSec > 0 {
		redis.Store.SetEx(key, value, expireSec)
	} else {
		redis.Store.Set(key, value)
	}

	return constant.RESP_OK
}

/* Supports `DEL key [key...]` */
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

/* Supports `TTL key` */
func (redis *Redis) TTL(cmd core.RedisCmd) []byte {
	argsLen := len(cmd.Args)
	if argsLen != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	entry := redis.Store.GetEntry(cmd.Args[0])
	if entry == nil {
		return constant.RESP_TTL_KEY_NOT_EXIST
	}

	if entry.ExpireAt == constant.NO_EXPIRE {
		return constant.RESP_TTL_KEY_EXIST_NO_EXPIRE
	}

	remainingTTLSeconds := (entry.ExpireAt - time.Now().UnixMilli()) / 1000
	return core.EncodeResp((int64)(remainingTTLSeconds), false)
}
