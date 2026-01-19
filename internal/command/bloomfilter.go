package command

import (
	"strconv"
	"strings"

	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/protocol"
	"github.com/manhhung2111/go-redis/internal/storage/types"
	"github.com/manhhung2111/go-redis/internal/errors"
)

/* Support BF.ADD key item */
func (redis *redis) BFAdd(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.BFAdd(args[0], args[1])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support BF.CARD key */
func (redis *redis) BFCard(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.BFCard(args[0])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support BF.EXISTS key item */
func (redis *redis) BFExists(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.BFExists(args[0], args[1])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support BF.INFO key [CAPACITY | SIZE | FILTERS | ITEMS | EXPANSION] */
func (redis *redis) BFInfo(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 && len(args) != 2 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	option := types.BloomFilterInfoAll
	if len(args) == 2 {
		switch strings.ToUpper(args[1]) {
		case "CAPACITY":
			option = types.BloomFilterInfoCapacity
		case "SIZE":
			option = types.BloomFilterInfoSize
		case "FILTERS":
			option = types.BloomFilterInfoFilters
		case "ITEMS":
			option = types.BloomFilterInfoItems
		case "EXPANSION":
			option = types.BloomFilterInfoExpansion
		default:
			return protocol.EncodeResp(errors.InvalidCommandOption(strings.ToUpper(args[1]), cmd.Cmd), false)
		}
	}

	result, err := redis.Store.BFInfo(args[0], option)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support BF.MADD key item [item ...] */
func (redis *redis) BFMAdd(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.BFMAdd(args[0], args[1:])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support BF.MEXISTS key item [item ...] */
func (redis *redis) BFMExists(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.BFMExists(args[0], args[1:])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support BF.RESERVE key error_rate capacity [EXPANSION expansion] */
func (redis *redis) BFReserve(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 && len(args) != 5 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	errorRate, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return protocol.RespBadErrorRate
	}

	if errorRate < 0 || errorRate > 1 {
		return protocol.RespErrorRateInvalidRange
	}

	capacity, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return protocol.RespBadCapacity
	}

	if capacity < int64(config.BFMinCapacity) || capacity > int64(config.BFMaxCapacity) {
		return protocol.RespCapacityInvalidRange
	}

	expansion := 2
	if len(args) == 5 {
		if strings.ToUpper(args[3]) != "EXPANSION" {
			return protocol.RespSyntaxError
		}

		newExpansion, err := strconv.ParseInt(args[4], 10, 64)
		if err != nil {
			return protocol.RespBadExpansion
		}

		if newExpansion < int64(config.BFMinExpansion) || newExpansion > int64(config.BFMaxExpansion) {
			return protocol.RespExpansionInvalidRange
		}

		expansion = int(newExpansion)
	}

	err = redis.Store.BFReserve(args[0], errorRate, uint32(capacity), uint32(expansion))
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.RespOK
}
