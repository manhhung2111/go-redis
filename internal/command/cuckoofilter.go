package command

import (
	"strconv"
	"strings"

	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/constant"
	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/storage"
	"github.com/manhhung2111/go-redis/internal/util"
)

/* Support CF.ADD key item */
func (redis *redis) CFAdd(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, exists := redis.Store.Get(args[0])
	if exists && rObj.Type != storage.ObjCuckooFilter {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	return core.EncodeResp(redis.Store.CFAdd(args[0], args[1]), false)
}

/* Support CF.ADDNX key item */
func (redis *redis) CFAddNx(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, exists := redis.Store.Get(args[0])
	if exists && rObj.Type != storage.ObjCuckooFilter {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	return core.EncodeResp(redis.Store.CFAddNx(args[0], args[1]), false)
}

/* Support CF.COUNT key item */
func (redis *redis) CFCount(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, exists := redis.Store.Get(args[0])
	if exists && rObj.Type != storage.ObjCuckooFilter {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	return core.EncodeResp(redis.Store.CFCount(args[0], args[1]), false)
}

/* Support CF.DEL key item */
func (redis *redis) CFDel(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, exists := redis.Store.Get(args[0])
	if exists && rObj.Type != storage.ObjCuckooFilter {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	if !exists {
		return constant.RESP_NOT_FOUND
	}

	return core.EncodeResp(redis.Store.CFDel(args[0], args[1]), false)
}

/* Support CF.EXISTS key item */
func (redis *redis) CFExists(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, exists := redis.Store.Get(args[0])
	if exists && rObj.Type != storage.ObjCuckooFilter {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	return core.EncodeResp(redis.Store.CFExists(args[0], args[1]), false)
}

/* Support CF.INFO key */
func (redis *redis) CFInfo(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, exists := redis.Store.Get(args[0])

	if !exists {
		return constant.RESP_NOT_FOUND
	}

	if exists && rObj.Type != storage.ObjCuckooFilter {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	return core.EncodeResp(redis.Store.CFInfo(args[0]), false)
}

/* Support CF.MEXISTS key item [item ...] */
func (redis *redis) CFMExists(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, exists := redis.Store.Get(args[0])
	if exists && rObj.Type != storage.ObjCuckooFilter {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	return core.EncodeResp(redis.Store.CFMExists(args[0], args[1:]), false)
}

/* Support CF.RESERVE key capacity [BUCKETSIZE bucketsize] [MAXITERATIONS maxiterations] [EXPANSION expansion] */
func (redis *redis) CFReserve(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, exists := redis.Store.Get(args[0])
	if exists && rObj.Type != storage.ObjCuckooFilter {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	if exists {
		return constant.RESP_ITEM_EXISTS
	}

	capacity, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return constant.RESP_BAD_CAPACITY
	}

	if capacity < 1 || capacity > int64(config.CF_MAX_INITIAL_SIZE) {
		return constant.RESP_CAPACITY_INVALID_RANGE
	}

	bucketSize := config.CF_DEFAULT_BUCKET_SIZE
	maxIterations := config.CF_DEFAULT_MAX_ITERATIONS
	expansion := config.CF_DEFAULT_EXPANSION_FACTOR

	// Parse optional arguments
	i := 2
	for i < len(args) {
		option := strings.ToUpper(args[i])
		switch option {
		case "BUCKETSIZE":
			if i+1 >= len(args) {
				return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
			}
			bs, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return constant.RESP_BAD_BUCKET_SIZE
			}
			if bs < int64(config.CF_MIN_BUCKET_SIZE) || bs > int64(config.CF_MAX_BUCKET_SIZE) {
				return constant.RESP_BUCKET_SIZE_INVALID_RANGE
			}
			bucketSize = int(bs)
			i += 2
		case "MAXITERATIONS":
			if i+1 >= len(args) {
				return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
			}
			mi, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return constant.RESP_BAD_MAX_ITERATIONS
			}
			if mi < int64(config.CF_MIN_MAX_ITERATIONS) || mi > int64(config.CF_MAX_MAX_ITERATIONS) {
				return constant.RESP_MAX_ITERATIONS_INVALID_RANGE
			}
			maxIterations = int(mi)
			i += 2
		case "EXPANSION":
			if i+1 >= len(args) {
				return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
			}
			exp, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return constant.RESP_BAD_EXPANSION
			}
			if exp < int64(config.CF_MIN_EXPANSION_FACTOR) || exp > int64(config.CF_MAX_EXPANSION_FACTOR) {
				return constant.RESP_EXPANSION_INVALID_RANGE
			}
			expansion = int(exp)
			i += 2
		default:
			return constant.RESP_SYNTAX_ERROR
		}
	}

	err = redis.Store.CFReserve(args[0], uint64(capacity), uint64(bucketSize), uint64(maxIterations), expansion)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return constant.RESP_OK
}
