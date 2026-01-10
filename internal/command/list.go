package command

import (
	"strconv"

	"github.com/manhhung2111/go-redis/internal/constant"
	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/util"
)

/* Support LPUSH key element [element ...] */
func (redis *redis) LPush(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.LPush(args[0], args[1:]...)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support LPOP key [count] */
func (redis *redis) LPop(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	var count uint32 = 1
	if len(args) == 2 {
		newCount, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || newCount < 0 {
			return constant.RESP_VALUE_IS_OUT_OF_RANGE_MUST_BE_POSITIVE
		}

		count = uint32(newCount)
	}

	result, err := redis.Store.LPop(args[0], count)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	if result == nil {
		return constant.RESP_NIL_BULK_STRING
	}

	if len(args) == 1 && len(result) > 0 {
		return core.EncodeResp(result[0], false)
	}

	return core.EncodeResp(result, false)
}

/* Support RPUSH key element [element ...] */
func (redis *redis) RPush(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.RPush(args[0], args[1:]...)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support RPOP key [count] */
func (redis *redis) RPop(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	var count uint32 = 1
	if len(args) == 2 {
		newCount, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || newCount < 0 {
			return constant.RESP_VALUE_IS_OUT_OF_RANGE_MUST_BE_POSITIVE
		}

		count = uint32(newCount)
	}

	result, err := redis.Store.RPop(args[0], count)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	if result == nil {
		return constant.RESP_NIL_BULK_STRING
	}

	if len(args) == 1 && len(result) > 0 {
		return core.EncodeResp(result[0], false)
	}

	return core.EncodeResp(result, false)
}

/* Support LRANGE key start stop */
func (redis *redis) LRange(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	start, err := strconv.ParseInt(args[1], 10, 32)
	if err != nil {
		return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
	}

	end, err := strconv.ParseInt(args[2], 10, 32)
	if err != nil {
		return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
	}

	result, err := redis.Store.LRange(args[0], int32(start), int32(end))
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support LINDEX key index */
func (redis *redis) LIndex(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	index, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
	}

	result, err := redis.Store.LIndex(args[0], int32(index))
	if err != nil {
		return core.EncodeResp(err, false)
	}

	if result == nil {
		return constant.RESP_NIL_BULK_STRING
	}

	return core.EncodeResp(result, false)
}

/* Support LLEN key */
func (redis *redis) LLen(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.LLen(args[0])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support LPUSHX key element [element ...] */
func (redis *redis) LPushX(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	exists := redis.Store.Exists(args[0])
	if !exists {
		return core.EncodeResp(0, false)
	}

	result, err := redis.Store.LPush(args[0], args[1:]...)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support LREM key count element */
func (redis *redis) LRem(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	count, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
	}

	result, err := redis.Store.LRem(args[0], int32(count), args[2])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support LSET key index element */
func (redis *redis) LSet(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	index, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
	}

	err = redis.Store.LSet(args[0], int32(index), args[2])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return constant.RESP_OK
}

/* Support LTRIM key start stop */
func (redis *redis) LTrim(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	start, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
	}

	end, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
	}

	if err = redis.Store.LTrim(args[0], int32(start), int32(end)); err != nil {
		return core.EncodeResp(err, false)
	}

	return constant.RESP_OK
}

/* Support RPUSHX key element [element ...] */
func (redis *redis) RPushX(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	exists := redis.Store.Exists(args[0])
	if !exists {
		return core.EncodeResp(0, false)
	}

	result, err := redis.Store.RPush(args[0], args[1:]...)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}
