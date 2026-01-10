package command

import (
	"strconv"
	"strings"

	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/constant"
	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
	"github.com/manhhung2111/go-redis/internal/util"
)

/* Support BF.ADD key item */
func (redis *redis) BFAdd(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.BFAdd(args[0], args[1])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support BF.CARD key */
func (redis *redis) BFCard(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.BFCard(args[0])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support BF.EXISTS key item */
func (redis *redis) BFExists(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.BFExists(args[0], args[1])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support BF.INFO key [CAPACITY | SIZE | FILTERS | ITEMS | EXPANSION] */
func (redis *redis) BFInfo(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 && len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	option := data_structure.BloomFilterInfoAll
	if len(args) == 2 {
		switch strings.ToUpper(args[1]) {
		case "CAPACITY":
			option = data_structure.BloomFilterInfoCapacity
		case "SIZE":
			option = data_structure.BloomFilterInfoSize
		case "FILTERS":
			option = data_structure.BloomFilterInfoFilters
		case "ITEMS":
			option = data_structure.BloomFilterInfoItems
		case "EXPANSION":
			option = data_structure.BloomFilterInfoExpansion
		default:
			return core.EncodeResp(util.InvalidCommandOption(strings.ToUpper(args[1]), cmd.Cmd), false)
		}
	}

	result, err := redis.Store.BFInfo(args[0], option)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support BF.MADD key item [item ...] */
func (redis *redis) BFMAdd(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.BFMAdd(args[0], args[1:])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support BF.MEXISTS key item [item ...] */
func (redis *redis) BFMExists(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.BFMExists(args[0], args[1:])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support BF.RESERVE key error_rate capacity [EXPANSION expansion] */
func (redis *redis) BFReserve(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 && len(args) != 5 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	errorRate, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return constant.RESP_BAD_ERROR_RATE
	}

	if errorRate < 0 || errorRate > 1 {
		return constant.RESP_ERROR_RATE_INVALID_RANGE
	}

	capacity, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return constant.RESP_BAD_CAPACITY
	}

	if capacity < int64(config.BF_MIN_CAPACITY) || capacity > int64(config.BF_MAX_CAPACITY) {
		return constant.RESP_CAPACITY_INVALID_RANGE
	}

	expansion := config.BF_DEFAULT_EXPANSION
	if len(args) == 5 {
		if strings.ToUpper(args[3]) != "EXPANSION" {
			return constant.RESP_SYNTAX_ERROR
		}

		newExpansion, err := strconv.ParseInt(args[4], 10, 64)
		if err != nil {
			return constant.RESP_BAD_EXPANSION
		}

		if newExpansion < int64(config.BF_MIN_EXPANSION) || newExpansion > int64(config.BF_MAX_EXPANSION) {
			return constant.RESP_EXPANSION_INVALID_RANGE
		}

		expansion = int(newExpansion)
	}

	err = redis.Store.BFReserve(args[0], errorRate, uint32(capacity), uint32(expansion))
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return constant.RESP_OK
}
