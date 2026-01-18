package command

import (
	"strconv"

	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/util"
)

/* Support CMS.INCRBY key item increment [item increment ...] */
func (redis *redis) CMSIncrBy(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 3 || len(args)%2 != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	// Parse item-increment pairs
	itemIncrement := make(map[string]uint64)
	for i := 1; i < len(args); i += 2 {
		item := args[i]
		increment, err := strconv.ParseInt(args[i+1], 10, 64)
		if err != nil || increment < 0 {
			return core.RespCMSBadIncrement
		}
		itemIncrement[item] += uint64(increment)
	}

	result, err := redis.Store.CMSIncrBy(args[0], itemIncrement)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support CMS.INFO key */
func (redis *redis) CMSInfo(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.CMSInfo(args[0])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support CMS.INITBYDIM key width depth */
func (redis *redis) CMSInitByDim(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	width, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || width < 1 {
		return core.RespCMSBadWidth
	}

	depth, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil || depth < 1 {
		return core.RespCMSBadDepth
	}

	err = redis.Store.CMSInitByDim(args[0], uint64(width), uint64(depth))
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.RespOK
}

/* Support CMS.INITBYPROB key error probability */
func (redis *redis) CMSInitByProb(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	errorRate, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return core.RespBadErrorRate
	}

	if errorRate <= 0 || errorRate >= 1 {
		return core.RespErrorRateInvalidRange
	}

	probability, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		return core.RespCMSBadProbability
	}

	if probability <= 0 || probability >= 1 {
		return core.RespCMSProbabilityInvalidRange
	}

	err = redis.Store.CMSInitByProb(args[0], errorRate, probability)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.RespOK
}

/* Support CMS.QUERY key item [item ...] */
func (redis *redis) CMSQuery(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.CMSQuery(args[0], args[1:])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}
