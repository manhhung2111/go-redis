package command

import (
	"github.com/manhhung2111/go-redis/internal/protocol"
	"github.com/manhhung2111/go-redis/internal/util"
)

/* Support PFADD key [element [element ...]] */
func (redis *redis) PFAdd(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 1 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	// Elements are optional - if not provided, just create the HLL
	elements := []string{}
	if len(args) > 1 {
		elements = args[1:]
	}

	result, err := redis.Store.PFAdd(args[0], elements)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support PFCOUNT key [key ...] */
func (redis *redis) PFCount(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 1 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	count, err := redis.Store.PFCount(args)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(count, false)
}

/* Support PFMERGE destkey [sourcekey [sourcekey ...]] */
func (redis *redis) PFMerge(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 1 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	destKey := args[0]
	sourceKeys := []string{}
	if len(args) > 1 {
		sourceKeys = args[1:]
	}

	err := redis.Store.PFMerge(destKey, sourceKeys)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.RespOK
}
