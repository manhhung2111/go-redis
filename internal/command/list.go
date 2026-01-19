package command

import (
	"strconv"

	"github.com/manhhung2111/go-redis/internal/protocol"
	"github.com/manhhung2111/go-redis/internal/errors"
)

/* Support LPUSH key element [element ...] */
func (redis *redis) LPush(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.LPush(args[0], args[1:]...)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support LPOP key [count] */
func (redis *redis) LPop(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 1 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	var count uint32 = 1
	if len(args) == 2 {
		newCount, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || newCount < 0 {
			return protocol.RespValueOutOfRangeMustPositive
		}

		count = uint32(newCount)
	}

	result, err := redis.Store.LPop(args[0], count)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	if result == nil {
		return protocol.RespNilBulkString
	}

	if len(args) == 1 && len(result) > 0 {
		return protocol.EncodeResp(result[0], false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support RPUSH key element [element ...] */
func (redis *redis) RPush(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.RPush(args[0], args[1:]...)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support RPOP key [count] */
func (redis *redis) RPop(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 1 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	var count uint32 = 1
	if len(args) == 2 {
		newCount, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || newCount < 0 {
			return protocol.RespValueOutOfRangeMustPositive
		}

		count = uint32(newCount)
	}

	result, err := redis.Store.RPop(args[0], count)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	if result == nil {
		return protocol.RespNilBulkString
	}

	if len(args) == 1 && len(result) > 0 {
		return protocol.EncodeResp(result[0], false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support LRANGE key start stop */
func (redis *redis) LRange(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	start, err := strconv.ParseInt(args[1], 10, 32)
	if err != nil {
		return protocol.RespValueNotIntegerOrOutOfRange
	}

	end, err := strconv.ParseInt(args[2], 10, 32)
	if err != nil {
		return protocol.RespValueNotIntegerOrOutOfRange
	}

	result, err := redis.Store.LRange(args[0], int32(start), int32(end))
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support LINDEX key index */
func (redis *redis) LIndex(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	index, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return protocol.RespValueNotIntegerOrOutOfRange
	}

	result, err := redis.Store.LIndex(args[0], int32(index))
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	if result == nil {
		return protocol.RespNilBulkString
	}

	return protocol.EncodeResp(result, false)
}

/* Support LLEN key */
func (redis *redis) LLen(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.LLen(args[0])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support LPUSHX key element [element ...] */
func (redis *redis) LPushX(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	exists := redis.Store.Exists(args[0])
	if !exists {
		return protocol.EncodeResp(0, false)
	}

	result, err := redis.Store.LPush(args[0], args[1:]...)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support LREM key count element */
func (redis *redis) LRem(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	count, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return protocol.RespValueNotIntegerOrOutOfRange
	}

	result, err := redis.Store.LRem(args[0], int32(count), args[2])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support LSET key index element */
func (redis *redis) LSet(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	index, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return protocol.RespValueNotIntegerOrOutOfRange
	}

	err = redis.Store.LSet(args[0], int32(index), args[2])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.RespOK
}

/* Support LTRIM key start stop */
func (redis *redis) LTrim(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	start, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return protocol.RespValueNotIntegerOrOutOfRange
	}

	end, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return protocol.RespValueNotIntegerOrOutOfRange
	}

	if err = redis.Store.LTrim(args[0], int32(start), int32(end)); err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.RespOK
}

/* Support RPUSHX key element [element ...] */
func (redis *redis) RPushX(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	exists := redis.Store.Exists(args[0])
	if !exists {
		return protocol.EncodeResp(0, false)
	}

	result, err := redis.Store.RPush(args[0], args[1:]...)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}
