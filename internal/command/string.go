package command

import (
	"strconv"
	"strings"

	"github.com/manhhung2111/go-redis/internal/protocol"
	"github.com/manhhung2111/go-redis/internal/util"
)

/* Supports `GET key` */
func (redis *redis) Get(cmd protocol.RedisCmd) []byte {
	argsLen := len(cmd.Args)
	if argsLen != 1 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	value, err := redis.Store.Get(cmd.Args[0])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(value, false)
}

/* Supports `SET key value [NX | XX] [EX seconds]` */
func (redis *redis) Set(cmd protocol.RedisCmd) []byte {
	argsLen := len(cmd.Args)
	if argsLen < 2 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	args := cmd.Args
	key, value := args[0], args[1]

	var (
		nx        bool // SET if Not eXists
		xx        bool // SET if eXists
		expireSec uint64
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
				return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
			}

			ttl, err := strconv.ParseUint(args[i+1], 10, 64)
			if err != nil || ttl <= 0 {
				return protocol.EncodeResp(util.InvalidExpireTime(cmd.Cmd), false)
			}

			expireSec = ttl
			i++
		default:
			return protocol.EncodeResp(util.InvalidCommandOption(opt, cmd.Cmd), false)
		}
	}

	if nx && xx {
		return protocol.EncodeResp(util.InvalidCommandOption("NX|XX", cmd.Cmd), false)
	}

	exists := redis.Store.Exists(key)

	if nx && exists {
		return protocol.RespNilBulkString
	}

	if xx && !exists {
		return protocol.RespNilBulkString
	}

	if expireSec > 0 {
		redis.Store.SetEx(key, value, expireSec)
	} else {
		redis.Store.Set(key, value)
	}

	return protocol.RespOK
}

/* Supports `DEL key [key...]` */
func (redis *redis) Del(cmd protocol.RedisCmd) []byte {
	argsLen := len(cmd.Args)
	if argsLen < 1 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	var deletedKeys int64 = 0
	for i := range argsLen {
		if isDeleted := redis.Store.Del(cmd.Args[i]); isDeleted {
			deletedKeys++
		}
	}

	return protocol.EncodeResp(deletedKeys, false)
}

/* Support MGET key [key ...] */
func (redis *redis) MGet(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 1 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	res := make([]*string, len(args))

	for i := 0; i < len(args); i++ {
		str, _ := redis.Store.Get(args[i])
		res[i] = str
	}

	return protocol.EncodeResp(res, false)
}

/* Support MSET key value [key value ...] */
func (redis *redis) MSet(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) == 0 || len(args)&1 == 1 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	for i := 0; i < len(args); i += 2 {
		redis.Store.Set(args[i], args[i+1])
	}

	return protocol.RespOK
}

/* Support INCR key */
func (redis *redis) Incr(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.IncrBy(args[0], 1)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support INCRBY key increment */
func (redis *redis) IncrBy(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	increment, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return protocol.RespValueNotIntegerOrOutOfRange
	}

	result, err := redis.Store.IncrBy(args[0], increment)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support DECR key */
func (redis *redis) Decr(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.IncrBy(args[0], -1)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support DECRBY key decrement */
func (redis *redis) DecrBy(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	decrement, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return protocol.RespValueNotIntegerOrOutOfRange
	}

	result, err := redis.Store.IncrBy(args[0], -decrement)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}
