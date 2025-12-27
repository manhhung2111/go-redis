package command

import (
	"strconv"
	"strings"

	"github.com/manhhung2111/go-redis/internal/constant"
	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/util"
)

/* Supports `GET key` */
func (redis *redis) Get(cmd core.RedisCmd) []byte {
	argsLen := len(cmd.Args)
	if argsLen != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, exists := redis.Store.Get(cmd.Args[0])
	if !exists {
		return constant.RESP_NIL_BULK_STRING
	}

	value, ok := rObj.StringValue()
	if !ok {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	return core.EncodeResp(value, false)
}

/* Supports `SET key value [NX | XX] [EX seconds]` */
func (redis *redis) Set(cmd core.RedisCmd) []byte {
	argsLen := len(cmd.Args)
	if argsLen < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
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
				return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
			}

			ttl, err := strconv.ParseUint(args[i+1], 10, 64)
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

	_, exists := redis.Store.Get(key)

	if nx && exists {
		return constant.RESP_NIL_BULK_STRING
	}

	if xx && !exists {
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
func (redis *redis) Del(cmd core.RedisCmd) []byte {
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

/* Support MGET key [key ...] */
func (redis *redis) MGet(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	res := make([]*string, len(args))

	for i := 0; i < len(args); i++ {
		rObj, ok := redis.Store.Get(args[i])
		if !ok {
			res[i] = nil
			continue
		}

		if s, ok := rObj.StringValue(); ok {
			res[i] = &s
		} else {
			res[i] = nil
		}
	}

	return core.EncodeResp(res, false)
}

/* Support MSET key value [key value ...] */
func (redis *redis) MSet(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) == 0 || len(args)&1 == 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	for i := 0; i < len(args); i += 2 {
		redis.Store.Set(args[i], args[i+1])
	}

	return constant.RESP_OK
}

/* Support INCR key */
func (redis *redis) Incr(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	key := args[0]
	rObj, exists := redis.Store.Get(key)
	if exists && rObj != nil {
		res, succeeded := rObj.IncrBy(1)
		if !succeeded {
			return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
		}
		return core.EncodeResp(res, false)
	} else {
		var res int64 = 1
		redis.Store.Set(key, strconv.FormatInt(res, 10))
		return core.EncodeResp(res, false)
	}
}

/* Support INCRBY key increment */
func (redis *redis) IncrBy(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	key := args[0]
	increment, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
	}

	rObj, exists := redis.Store.Get(key)
	if exists && rObj != nil {
		res, succeeded := rObj.IncrBy(increment)
		if !succeeded {
			return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
		}
		return core.EncodeResp(res, false)
	} else {
		var res int64 = increment
		redis.Store.Set(key, strconv.FormatInt(res, 10))
		return core.EncodeResp(res, false)
	}
}

/* Support DECR key */
func (redis *redis) Decr(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	key := args[0]
	rObj, exists := redis.Store.Get(key)
	if exists && rObj != nil {
		res, succeeded := rObj.IncrBy(-1)
		if !succeeded {
			return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
		}
		return core.EncodeResp(res, false)
	} else {
		var res int64 = -1
		redis.Store.Set(key, strconv.FormatInt(res, 10))
		return core.EncodeResp(res, false)
	}
}

/* Support DECRBY key decrement */
func (redis *redis) DecrBy(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	key := args[0]
	decrement, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
	}

	rObj, exists := redis.Store.Get(key)
	if exists && rObj != nil {
		res, succeeded := rObj.IncrBy(-decrement)
		if !succeeded {
			return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
		}
		return core.EncodeResp(res, false)
	} else {
		var res int64 = -decrement
		redis.Store.Set(key, strconv.FormatInt(res, 10))
		return core.EncodeResp(res, false)
	}
}
