package command

import (
	"strconv"

	"github.com/manhhung2111/go-redis/internal/protocol"
	"github.com/manhhung2111/go-redis/internal/errors"
)

/* Support HGET key field */
func (redis *redis) HGet(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.HGet(args[0], args[1])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	if result == nil {
		return protocol.RespNilBulkString
	}

	return protocol.EncodeResp(result, false)
}

/* Support HGETALL key */
func (redis *redis) HGetAll(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.HGetAll(args[0])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support HMGET key field [field ...] */
func (redis *redis) HMGet(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.HMGet(args[0], args[1:])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support HINCRBY key field increment */
func (redis *redis) HIncrBy(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	increment, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return protocol.RespValueNotIntegerOrOutOfRange
	}

	result, err := redis.Store.HIncrBy(args[0], args[1], increment)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* HKEYS key */
func (redis *redis) HKeys(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.HKeys(args[0])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support HVALS key */
func (redis *redis) HVals(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.HVals(args[0])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support HLEN key */
func (redis *redis) HLen(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.HLen(args[0])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support HSET key field value [field value ...] */
func (redis *redis) HSet(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 3 || (len(args)&1) == 0 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	fieldValue := make(map[string]string)
	for i := 1; i < len(args); i += 2 {
		fieldValue[args[i]] = args[i+1]
	}

	result, err := redis.Store.HSet(args[0], fieldValue)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support HSETNX key field value */
func (redis *redis) HSetNx(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.HSetNx(args[0], args[1], args[2])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support HDEL key field [field ...] */
func (redis *redis) HDel(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.HDel(args[0], args[1:])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* HEXISTS key field */
func (redis *redis) HExists(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.HExists(args[0], args[1])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}
