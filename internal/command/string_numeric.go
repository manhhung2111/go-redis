package command

import (
	"math"
	"strconv"

	"github.com/manhhung2111/go-redis/internal/constant"
	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/util"
)

/* Support INCR key */
func (redis *Redis) Incr(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	key := args[0]
	entryValue, exists := redis.Store.Get(key)
	if exists && entryValue != nil {
		valueStr, ok := entryValue.(string)
		if !ok {
			return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
		}

		res, err := strconv.ParseInt(valueStr, 10, 64)
		if err != nil || res == math.MaxInt64 {
			return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
		}

		res++
		redis.Store.SetValue(key, strconv.FormatInt(res, 10))
		return core.EncodeResp(res, false)
	} else {
		var res int64 = 1
		redis.Store.Set(key, strconv.FormatInt(res, 10))
		return core.EncodeResp(res, false)
	}
}

/* Support INCRBY key increment */
func (redis *Redis) IncrBy(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	key := args[0]
	increment, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
	}

	entryValue, exists := redis.Store.Get(key)
	if exists && entryValue != nil {
		valueStr, ok := entryValue.(string)
		if !ok {
			return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
		}

		res, err := strconv.ParseInt(valueStr, 10, 64)
		if err != nil || (increment > 0 && res > math.MaxInt64-increment) || (increment < 0 && res < math.MinInt64-increment) {
			return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
		}

		res += increment
		redis.Store.SetValue(key, strconv.FormatInt(res, 10))
		return core.EncodeResp(res, false)
	} else {
		var res int64 = increment
		redis.Store.Set(key, strconv.FormatInt(res, 10))
		return core.EncodeResp(res, false)
	}
}

/* Support DECR key */
func (redis *Redis) Decr(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	key := args[0]
	entryValue, exists := redis.Store.Get(key)
	if exists && entryValue != nil {
		valueStr, ok := entryValue.(string)
		if !ok {
			return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
		}

		res, err := strconv.ParseInt(valueStr, 10, 64)
		if err != nil || res == math.MinInt64 {
			return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
		}

		res--
		redis.Store.SetValue(key, strconv.FormatInt(res, 10))
		return core.EncodeResp(res, false)
	} else {
		var res int64 = -1
		redis.Store.Set(key, strconv.FormatInt(res, 10))
		return core.EncodeResp(res, false)
	}
}

/* Support DECRBY key decrement */
func (redis *Redis) DecrBy(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	key := args[0]
	decrement, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
	}

	entryValue, exists := redis.Store.Get(key)
	if exists && entryValue != nil {
		valueStr, ok := entryValue.(string)
		if !ok {
			return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
		}

		res, err := strconv.ParseInt(valueStr, 10, 64)
		if err != nil || (decrement > 0 && res < math.MinInt64+decrement) || (decrement < 0 && res > math.MaxInt64+decrement) {
			return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
		}

		res -= decrement
		redis.Store.SetValue(key, strconv.FormatInt(res, 10))
		return core.EncodeResp(res, false)
	} else {
		var res int64 = -decrement
		redis.Store.Set(key, strconv.FormatInt(res, 10))
		return core.EncodeResp(res, false)
	}
}
