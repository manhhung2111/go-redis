package command

import (
	"strconv"
	"strings"

	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/protocol"
	"github.com/manhhung2111/go-redis/internal/util"
)

/* Support CF.ADD key item */
func (redis *redis) CFAdd(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.CFAdd(args[0], args[1])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support CF.ADDNX key item */
func (redis *redis) CFAddNx(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.CFAddNx(args[0], args[1])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support CF.COUNT key item */
func (redis *redis) CFCount(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.CFCount(args[0], args[1])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support CF.DEL key item */
func (redis *redis) CFDel(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.CFDel(args[0], args[1])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support CF.EXISTS key item */
func (redis *redis) CFExists(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.CFExists(args[0], args[1])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support CF.INFO key */
func (redis *redis) CFInfo(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.CFInfo(args[0])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support CF.MEXISTS key item [item ...] */
func (redis *redis) CFMExists(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.CFMExists(args[0], args[1:])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support CF.RESERVE key capacity [BUCKETSIZE bucketsize] [MAXITERATIONS maxiterations] [EXPANSION expansion] */
func (redis *redis) CFReserve(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	capacity, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return protocol.RespBadCapacity
	}

	if capacity < 1 || capacity > int64(config.CF_MAX_INITIAL_SIZE) {
		return protocol.RespCapacityInvalidRange
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
				return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
			}
			bs, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return protocol.RespBadBucketSize
			}
			if bs < int64(config.CF_MIN_BUCKET_SIZE) || bs > int64(config.CF_MAX_BUCKET_SIZE) {
				return protocol.RespBucketSizeInvalidRange
			}
			bucketSize = int(bs)
			i += 2
		case "MAXITERATIONS":
			if i+1 >= len(args) {
				return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
			}
			mi, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return protocol.RespBadMaxIterations
			}
			if mi < int64(config.CF_MIN_MAX_ITERATIONS) || mi > int64(config.CF_MAX_MAX_ITERATIONS) {
				return protocol.RespMaxIterationsInvalidRange
			}
			maxIterations = int(mi)
			i += 2
		case "EXPANSION":
			if i+1 >= len(args) {
				return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
			}
			exp, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return protocol.RespBadExpansion
			}
			if exp < int64(config.CF_MIN_EXPANSION_FACTOR) || exp > int64(config.CF_MAX_EXPANSION_FACTOR) {
				return protocol.RespExpansionInvalidRange
			}
			expansion = int(exp)
			i += 2
		default:
			return protocol.RespSyntaxError
		}
	}

	err = redis.Store.CFReserve(args[0], uint64(capacity), uint64(bucketSize), uint64(maxIterations), expansion)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.RespOK
}
