package command

import (
	"strconv"

	"github.com/manhhung2111/go-redis/internal/constant"
	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/storage"
	"github.com/manhhung2111/go-redis/internal/util"
)

/* Support HGET key field */
func (redis *redis) HGet(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, existing := redis.Store.Get(args[0])
	if !existing {
		return constant.RESP_NIL_BULK_STRING
	}

	if rObj.Type != storage.ObjHash {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	result, existing := redis.Store.HGet(args[0], args[1])
	if !existing {
		return constant.RESP_NIL_BULK_STRING
	}

	return core.EncodeResp(result, false)
}

/* Support HGETALL key */
func (redis *redis) HGetAll(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, existing := redis.Store.Get(args[0])
	if !existing {
		return core.EncodeResp([]string{}, false)
	}

	if rObj.Type != storage.ObjHash {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	result := redis.Store.HGetAll(args[0])
	return core.EncodeResp(result, false)
}

/* Support HMGET key field [field ...] */
func (redis *redis) HMGet(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, existing := redis.Store.Get(args[0])
	if existing && rObj.Type != storage.ObjHash {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	result := redis.Store.HMGet(args[0], args[1:])
	return core.EncodeResp(result, false)
}

/* Support HINCRBY key field increment */
func (redis *redis) HIncrBy(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, existing := redis.Store.Get(args[0])
	if existing && rObj.Type != storage.ObjHash {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	increment, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
	}

	result, err := redis.Store.HIncrBy(args[0], args[1], increment)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* HKEYS key */
func (redis *redis) HKeys(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, existing := redis.Store.Get(args[0])
	if !existing {
		return core.EncodeResp([]string{}, false)
	}

	if rObj.Type != storage.ObjHash {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	result := redis.Store.HKeys(args[0])
	return core.EncodeResp(result, false)
}

/* Support HVALS key */
func (redis *redis) HVals(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, existing := redis.Store.Get(args[0])
	if !existing {
		return core.EncodeResp([]string{}, false)
	}

	if rObj.Type != storage.ObjHash {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	result := redis.Store.HVals(args[0])
	return core.EncodeResp(result, false)
}

/* Support HLEN key */
func (redis *redis) HLen(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, existing := redis.Store.Get(args[0])
	if !existing {
		return core.EncodeResp(uint32(0), false)
	}

	if rObj.Type != storage.ObjHash {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	result := redis.Store.HLen(args[0])
	return core.EncodeResp(result, false)
}

/* Support HSET key field value [field value ...] */
func (redis *redis) HSet(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 3 || (len(args)&1) == 0 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, existing := redis.Store.Get(args[0])
	if existing && rObj.Type != storage.ObjHash {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	fieldValue := make(map[string]string)
	for i := 1; i < len(args); i += 2 {
		fieldValue[args[i]] = args[i+1]
	}

	result := redis.Store.HSet(args[0], fieldValue)
	return core.EncodeResp(result, false)
}

/* Support HSETNX key field value */
func (redis *redis) HSetNx(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, existing := redis.Store.Get(args[0])
	if existing && rObj.Type != storage.ObjHash {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	result := redis.Store.HSetNx(args[0], args[1], args[2])
	return core.EncodeResp(result, false)
}

/* Support HDEL key field [field ...] */
func (redis *redis) HDel(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, existing := redis.Store.Get(args[0])
	if !existing {
		return core.EncodeResp(int64(0), false)
	}

	if existing && rObj.Type != storage.ObjHash {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	result := redis.Store.HDel(args[0], args[1:])
	return core.EncodeResp(result, false)
}

/* HEXISTS key field */
func (redis *redis) HExists(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, existing := redis.Store.Get(args[0])
	if !existing {
		return core.EncodeResp(int64(0), false)
	}

	if existing && rObj.Type != storage.ObjHash {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	result := redis.Store.HExists(args[0], args[1])
	return core.EncodeResp(result, false)
}
