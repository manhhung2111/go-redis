package command

import (
	"github.com/manhhung2111/go-redis/internal/constant"
	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/storage"
	"github.com/manhhung2111/go-redis/internal/util"
)

/* Support PFADD key [element [element ...]] */
func (redis *redis) PFAdd(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	rObj, exists := redis.Store.Get(args[0])
	if exists && rObj.Type != storage.ObjHyperLogLog {
		return constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY
	}

	// Elements are optional - if not provided, just create the HLL
	elements := []string{}
	if len(args) > 1 {
		elements = args[1:]
	}

	return core.EncodeResp(redis.Store.PFAdd(args[0], elements), false)
}

/* Support PFCOUNT key [key ...] */
func (redis *redis) PFCount(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	count, err := redis.Store.PFCount(args)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(count, false)
}

/* Support PFMERGE destkey [sourcekey [sourcekey ...]] */
func (redis *redis) PFMerge(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	destKey := args[0]
	sourceKeys := []string{}
	if len(args) > 1 {
		sourceKeys = args[1:]
	}

	err := redis.Store.PFMerge(destKey, sourceKeys)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return constant.RESP_OK
}
