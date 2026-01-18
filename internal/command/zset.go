package command

import (
	"errors"
	"strconv"
	"strings"

	"github.com/manhhung2111/go-redis/internal/protocol"
	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
	"github.com/manhhung2111/go-redis/internal/util"
)

const (
	minLexString = ""
	maxLexString = "~"
)

/* Support ZADD key [NX | XX] [GT | LT] [CH] score member [score member...] */
func (redis *redis) ZAdd(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 3 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
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
			return protocol.RespSyntaxError
		}
		i++
	}

	if options.NX && options.XX {
		return protocol.RespXXNXNotCompatible
	}
	if (options.GT || options.LT) && options.NX {
		return protocol.RespGTLTNXNotCompatible
	}
	if options.GT && options.LT {
		return protocol.RespGTLTNXNotCompatible
	}

	remaining := len(args) - i
	if remaining == 0 || remaining%2 != 0 {
		return protocol.RespSyntaxError
	}

	scoreMember := make(map[string]float64, remaining/2)

	for i < len(args) {
		score, err := strconv.ParseFloat(args[i], 64)
		if err != nil {
			return protocol.RespSyntaxError
		}
		member := args[i+1]
		scoreMember[member] = score
		i += 2
	}

	result, err := redis.Store.ZAdd(args[0], scoreMember, options)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	if result == nil {
		return protocol.RespSyntaxError
	}

	return protocol.EncodeResp(*result, false)
}

/* Support ZCARD key */
func (redis *redis) ZCard(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.ZCard(args[0])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support ZCOUNT key min max */
func (redis *redis) ZCount(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	minValue, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return protocol.RespValueNotValidFloat
	}

	maxValue, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		return protocol.RespValueNotValidFloat
	}

	result, err := redis.Store.ZCount(args[0], minValue, maxValue)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support ZINCRBY key increment member */
func (redis *redis) ZIncrBy(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	increment, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return protocol.RespValueNotValidFloat
	}

	result, err := redis.Store.ZIncrBy(args[0], args[2], increment)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support ZLEXCOUNT key min max */
func (redis *redis) ZLexCount(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 3 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	minValue, err := getLexString(args[1])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	maxValue, err := getLexString(args[2])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	result, err := redis.Store.ZLexCount(args[0], minValue, maxValue)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support ZMSCORE key member [member ...] */
func (redis *redis) ZMScore(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.ZMScore(args[0], args[1:])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support ZPOPMAX key [count] */
func (redis *redis) ZPopMax(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 && len(args) != 2 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	count := 1
	if len(args) == 2 {
		newCount, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || newCount < 0 {
			return protocol.RespValueOutOfRangeMustPositive
		}
		count = int(newCount)
	}

	result, err := redis.Store.ZPopMax(args[0], count)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support ZPOPMIN key [count] */
func (redis *redis) ZPopMin(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 && len(args) != 2 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	count := 1
	if len(args) == 2 {
		newCount, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil || newCount < 0 {
			return protocol.RespValueOutOfRangeMustPositive
		}
		count = int(newCount)
	}

	result, err := redis.Store.ZPopMin(args[0], count)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support ZRANDMEMBER key [count [WITHSCORES]] */
func (redis *redis) ZRandMember(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 1 && len(args) != 2 && len(args) != 3 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	count := 1
	if len(args) >= 2 {
		newCount, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return protocol.RespValueOutOfRangeMustPositive
		}
		count = int(newCount)
	}

	withScores := false
	if len(args) == 3 {
		if strings.ToUpper(args[2]) == "WITHSCORES" {
			withScores = true
		} else {
			return protocol.RespSyntaxError
		}
	}

	result, err := redis.Store.ZRandMember(args[0], count, withScores)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	if len(args) == 1 {
		if len(result) == 0 {
			return protocol.RespNilBulkString
		}
		return protocol.EncodeResp(result[0], false)
	}
	return protocol.EncodeResp(result, false)
}

/* Support ZRANGE key start stop [BYSCORE | BYLEX] [REV] [WITHSCORES] */
func (redis *redis) ZRange(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 3 || len(args) > 6 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
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
			return protocol.RespSyntaxError
		}
	}

	if byScore && byLex {
		return protocol.RespSyntaxError
	}

	if byLex && withScores {
		return protocol.RespWithScoresNotSupportedByLex
	}

	if !byScore && !byLex {
		start, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return protocol.RespValueNotIntegerOrOutOfRange
		}

		stop, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return protocol.RespValueNotIntegerOrOutOfRange
		}

		result := []string{}
		if rev {
			result, err = redis.Store.ZRevRangeByRank(args[0], int(start), int(stop), withScores)
		} else {
			result, err = redis.Store.ZRangeByRank(args[0], int(start), int(stop), withScores)
		}

		if err != nil {
			return protocol.EncodeResp(err, false)
		}

		return protocol.EncodeResp(result, false)
	}

	if byScore {
		start, err := strconv.ParseFloat(args[1], 64)
		if err != nil {
			return protocol.RespMinOrMaxNotFloat
		}

		stop, err := strconv.ParseFloat(args[2], 64)
		if err != nil {
			return protocol.RespMinOrMaxNotFloat
		}

		result := []string{}
		if rev {
			result, err = redis.Store.ZRevRangeByScore(args[0], start, stop, withScores)
		} else {
			result, err = redis.Store.ZRangeByScore(args[0], start, stop, withScores)
		}

		if err != nil {
			return protocol.EncodeResp(err, false)
		}

		return protocol.EncodeResp(result, false)
	}

	if byLex {
		start, err := getLexString(args[1])
		if err != nil {
			return protocol.EncodeResp(err, false)
		}

		stop, err := getLexString(args[2])
		if err != nil {
			return protocol.EncodeResp(err, false)
		}

		result := []string{}
		if rev {
			result, err = redis.Store.ZRevRangeByLex(args[0], start, stop)
		} else {
			result, err = redis.Store.ZRangeByLex(args[0], start, stop)
		}

		if err != nil {
			return protocol.EncodeResp(err, false)
		}

		return protocol.EncodeResp(result, false)
	}

	// This line should never be reached
	return protocol.RespSyntaxError
}

/* Support ZRANK key member [WITHSCORE] */
func (redis *redis) ZRank(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 && len(args) != 3 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	withScore := false
	if len(args) == 3 {
		if strings.ToUpper(args[2]) == "WITHSCORE" {
			withScore = true
		} else {
			return protocol.RespSyntaxError
		}
	}

	result, err := redis.Store.ZRank(args[0], args[1], withScore)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	if result == nil {
		return protocol.RespNilBulkString
	}
	if !withScore {
		return protocol.EncodeResp(result[0], false)
	}
	return protocol.EncodeResp(result, false)
}

/* Support ZREM key member [member ...] */
func (redis *redis) ZRem(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.ZRem(args[0], args[1:])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support ZREVRANK key member [WITHSCORE] */
func (redis *redis) ZRevRank(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 && len(args) != 3 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	withScore := false
	if len(args) == 3 {
		if strings.ToUpper(args[2]) == "WITHSCORE" {
			withScore = true
		} else {
			return protocol.RespSyntaxError
		}
	}

	result, err := redis.Store.ZRevRank(args[0], args[1], withScore)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	if result == nil {
		return protocol.RespNilBulkString
	}
	if !withScore {
		return protocol.EncodeResp(result[0], false)
	}
	return protocol.EncodeResp(result, false)
}

/* Support ZSCORE key member */
func (redis *redis) ZScore(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) != 2 {
		return protocol.EncodeResp(util.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.ZScore(args[0], args[1])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
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
