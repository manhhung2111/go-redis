package command

import (
	"strconv"

	"github.com/manhhung2111/go-redis/internal/constant"
	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/util"
)

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
