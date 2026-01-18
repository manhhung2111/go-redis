package command

import (
	"strconv"
	"strings"

	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/util"
)

/* Support CF.ADD key item */
func (redis *redis) CFAdd(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.CFAdd(args[0], args[1])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support CF.ADDNX key item */
func (redis *redis) CFAddNx(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.CFAddNx(args[0], args[1])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support CF.COUNT key item */
func (redis *redis) CFCount(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.CFCount(args[0], args[1])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support CF.DEL key item */
func (redis *redis) CFDel(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.CFDel(args[0], args[1])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support CF.EXISTS key item */
func (redis *redis) CFExists(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.CFExists(args[0], args[1])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support CF.INFO key */
func (redis *redis) CFInfo(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.CFInfo(args[0])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support CF.MEXISTS key item [item ...] */
func (redis *redis) CFMExists(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.CFMExists(args[0], args[1:])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support CF.RESERVE key capacity [BUCKETSIZE bucketsize] [MAXITERATIONS maxiterations] [EXPANSION expansion] */
func (redis *redis) CFReserve(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	capacity, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return core.RespBadCapacity
	}

	if capacity < 1 || capacity > int64(config.CF_MAX_INITIAL_SIZE) {
		return core.RespCapacityInvalidRange
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
				return core.RespBadBucketSize
			}
			if bs < int64(config.CF_MIN_BUCKET_SIZE) || bs > int64(config.CF_MAX_BUCKET_SIZE) {
				return core.RespBucketSizeInvalidRange
			}
			bucketSize = int(bs)
			i += 2
		case "MAXITERATIONS":
			if i+1 >= len(args) {
				return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
			}
			mi, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return core.RespBadMaxIterations
			}
			if mi < int64(config.CF_MIN_MAX_ITERATIONS) || mi > int64(config.CF_MAX_MAX_ITERATIONS) {
				return core.RespMaxIterationsInvalidRange
			}
			maxIterations = int(mi)
			i += 2
		case "EXPANSION":
			if i+1 >= len(args) {
				return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
			}
			exp, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return core.RespBadExpansion
			}
			if exp < int64(config.CF_MIN_EXPANSION_FACTOR) || exp > int64(config.CF_MAX_EXPANSION_FACTOR) {
				return core.RespExpansionInvalidRange
			}
			expansion = int(exp)
			i += 2
		default:
			return core.RespSyntaxError
		}
	}

	err = redis.Store.CFReserve(args[0], uint64(capacity), uint64(bucketSize), uint64(maxIterations), expansion)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.RespOK
}
