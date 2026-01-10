package command

import (
	"errors"
	"strconv"
	"strings"

	"github.com/manhhung2111/go-redis/internal/constant"
	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
	"github.com/manhhung2111/go-redis/internal/util"
)

const (
	minLexString = ""
	maxLexString = "~"
)

/* Support ZADD key [NX | XX] [GT | LT] [CH] score member [score member...] */
func (redis *redis) ZAdd(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 3 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	options := data_structure.ZAddOptions{}

	i := 1
	for i < len(args) {
		if _, err := strconv.ParseFloat(args[i], 64); err == nil {
			break // first score found
		}

		switch strings.ToUpper(args[i]) {
		case "NX":
			options.NX = true
		case "XX":
			options.XX = true
		case "GT":
			options.GT = true
		case "LT":
			options.LT = true
		case "CH":
			options.CH = true
		default:
			return constant.RESP_SYNTAX_ERROR
		}
		i++
	}

	if options.NX && options.XX {
		return constant.RESP_XX_NX_NOT_COMPATIBLE
	}
	if (options.GT || options.LT) && options.NX {
		return constant.RESP_GT_LT_NX_NOT_COMPATIBLE
	}
	if options.GT && options.LT {
		return constant.RESP_GT_LT_NX_NOT_COMPATIBLE
	}

	remaining := len(args) - i
	if remaining == 0 || remaining%2 != 0 {
		return constant.RESP_SYNTAX_ERROR
	}

	scoreMember := make(map[string]float64, remaining/2)

	for i < len(args) {
		score, err := strconv.ParseFloat(args[i], 64)
		if err != nil {
			return constant.RESP_SYNTAX_ERROR
		}
		member := args[i+1]
		scoreMember[member] = score
		i += 2
	}

	result, err := redis.Store.ZAdd(args[0], scoreMember, options)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	if result == nil {
		return constant.RESP_SYNTAX_ERROR
	}

	return core.EncodeResp(*result, false)
}

/* Support ZCARD key */
func (redis *redis) ZCard(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.ZCard(args[0])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support ZCOUNT key min max */
func (redis *redis) ZCount(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	minValue, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return constant.RESP_VALUE_IS_NOT_VALID_FLOAT
	}

	maxValue, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		return constant.RESP_VALUE_IS_NOT_VALID_FLOAT
	}

	result, err := redis.Store.ZCount(args[0], minValue, maxValue)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support ZINCRBY key increment member */
func (redis *redis) ZIncrBy(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	increment, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return constant.RESP_VALUE_IS_NOT_VALID_FLOAT
	}

	result, err := redis.Store.ZIncrBy(args[0], args[2], increment)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support ZLEXCOUNT key min max */
func (redis *redis) ZLexCount(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	minValue, err := getLexString(args[1])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	maxValue, err := getLexString(args[2])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	result, err := redis.Store.ZLexCount(args[0], minValue, maxValue)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support ZMSCORE key member [member ...] */
func (redis *redis) ZMScore(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.ZMScore(args[0], args[1:])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support ZPOPMAX key [count] */
func (redis *redis) ZPopMax(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 && len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	count := 1
	if len(args) == 2 {
		newCount, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || newCount < 0 {
			return constant.RESP_VALUE_IS_OUT_OF_RANGE_MUST_BE_POSITIVE
		}
		count = int(newCount)
	}

	result, err := redis.Store.ZPopMax(args[0], count)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support ZPOPMIN key [count] */
func (redis *redis) ZPopMin(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 && len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	count := 1
	if len(args) == 2 {
		newCount, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || newCount < 0 {
			return constant.RESP_VALUE_IS_OUT_OF_RANGE_MUST_BE_POSITIVE
		}
		count = int(newCount)
	}

	result, err := redis.Store.ZPopMin(args[0], count)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support ZRANDMEMBER key [count [WITHSCORES]] */
func (redis *redis) ZRandMember(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 && len(args) != 2 && len(args) != 3 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	count := 1
	if len(args) >= 2 {
		newCount, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return constant.RESP_VALUE_IS_OUT_OF_RANGE_MUST_BE_POSITIVE
		}
		count = int(newCount)
	}

	withScores := false
	if len(args) == 3 {
		if strings.ToUpper(args[2]) == "WITHSCORES" {
			withScores = true
		} else {
			return constant.RESP_SYNTAX_ERROR
		}
	}

	result, err := redis.Store.ZRandMember(args[0], count, withScores)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	if len(args) == 1 {
		if len(result) == 0 {
			return constant.RESP_NIL_BULK_STRING
		}
		return core.EncodeResp(result[0], false)
	}
	return core.EncodeResp(result, false)
}

/* Support ZRANGE key start stop [BYSCORE | BYLEX] [REV] [WITHSCORES] */
func (redis *redis) ZRange(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 3 || len(args) > 6 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	var (
		byScore    bool
		byLex      bool
		rev        bool
		withScores bool
	)

	for i := 3; i < len(args); i++ {
		option := strings.ToUpper(args[i])
		switch option {
		case "BYSCORE":
			byScore = true
		case "BYLEX":
			byLex = true
		case "REV":
			rev = true
		case "WITHSCORES":
			withScores = true
		default:
			return constant.RESP_SYNTAX_ERROR
		}
	}

	if byScore && byLex {
		return constant.RESP_SYNTAX_ERROR
	}

	if byLex && withScores {
		return constant.RESP_WITHSCORES_NOT_SUPPORTED_WITH_BYLEX
	}

	if !byScore && !byLex {
		start, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
		}

		stop, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE
		}

		result := []string{}
		if rev {
			result, err = redis.Store.ZRevRangeByRank(args[0], int(start), int(stop), withScores)
		} else {
			result, err = redis.Store.ZRangeByRank(args[0], int(start), int(stop), withScores)
		}

		if err != nil {
			return core.EncodeResp(err, false)
		}

		return core.EncodeResp(result, false)
	}

	if byScore {
		start, err := strconv.ParseFloat(args[1], 64)
		if err != nil {
			return constant.RESP_MIN_OR_MAX_IS_NOT_FLOAT
		}

		stop, err := strconv.ParseFloat(args[2], 64)
		if err != nil {
			return constant.RESP_MIN_OR_MAX_IS_NOT_FLOAT
		}

		result := []string{}
		if rev {
			result, err = redis.Store.ZRevRangeByScore(args[0], start, stop, withScores)
		} else {
			result, err = redis.Store.ZRangeByScore(args[0], start, stop, withScores)
		}

		if err != nil {
			return core.EncodeResp(err, false)
		}

		return core.EncodeResp(result, false)
	}

	if byLex {
		start, err := getLexString(args[1])
		if err != nil {
			return core.EncodeResp(err, false)
		}

		stop, err := getLexString(args[2])
		if err != nil {
			return core.EncodeResp(err, false)
		}

		result := []string{}
		if rev {
			result, err = redis.Store.ZRevRangeByLex(args[0], start, stop)
		} else {
			result, err = redis.Store.ZRangeByLex(args[0], start, stop)
		}

		if err != nil {
			return core.EncodeResp(err, false)
		}

		return core.EncodeResp(result, false)
	}

	// This line should never be reached
	return constant.RESP_SYNTAX_ERROR
}

/* Support ZRANK key member [WITHSCORE] */
func (redis *redis) ZRank(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 && len(args) != 3 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	withScore := false
	if len(args) == 3 {
		if strings.ToUpper(args[2]) == "WITHSCORE" {
			withScore = true
		} else {
			return constant.RESP_SYNTAX_ERROR
		}
	}

	result, err := redis.Store.ZRank(args[0], args[1], withScore)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	if result == nil {
		return constant.RESP_NIL_BULK_STRING
	}
	if !withScore {
		return core.EncodeResp(result[0], false)
	}
	return core.EncodeResp(result, false)
}

/* Support ZREM key member [member ...] */
func (redis *redis) ZRem(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.ZRem(args[0], args[1:])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

/* Support ZREVRANK key member [WITHSCORE] */
func (redis *redis) ZRevRank(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 && len(args) != 3 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	withScore := false
	if len(args) == 3 {
		if strings.ToUpper(args[2]) == "WITHSCORE" {
			withScore = true
		} else {
			return constant.RESP_SYNTAX_ERROR
		}
	}

	result, err := redis.Store.ZRevRank(args[0], args[1], withScore)
	if err != nil {
		return core.EncodeResp(err, false)
	}

	if result == nil {
		return constant.RESP_NIL_BULK_STRING
	}
	if !withScore {
		return core.EncodeResp(result[0], false)
	}
	return core.EncodeResp(result, false)
}

/* Support ZSCORE key member */
func (redis *redis) ZScore(cmd core.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return core.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.ZScore(args[0], args[1])
	if err != nil {
		return core.EncodeResp(err, false)
	}

	return core.EncodeResp(result, false)
}

func getLexString(str string) (string, error) {
	if str == "-" {
		return minLexString, nil
	}

	if str == "+" {
		return maxLexString, nil
	}

	// Validate no special prefixes
	if len(str) > 0 && (str[0] == '[' || str[0] == '(') {
		return "", errors.New("range syntax not supported")
	}

	return str, nil
}
