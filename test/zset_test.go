package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manhhung2111/go-redis/internal/core"
)

func TestZAddBasic(t *testing.T) {
	r := newTestRedis()

	resp := r.ZAdd(cmd("ZADD", "k", "1", "a", "2", "b"))
	assert.Equal(t, []byte(":2\r\n"), resp)

	resp = r.ZAdd(cmd("ZADD", "k", "1", "a"))
	assert.Equal(t, []byte(":0\r\n"), resp)
}

func TestZAddNX(t *testing.T) {
	r := newTestRedis()

	r.ZAdd(cmd("ZADD", "k", "1", "a"))
	resp := r.ZAdd(cmd("ZADD", "k", "NX", "2", "a"))
	assert.Equal(t, []byte(":0\r\n"), resp)
}

func TestZAddXX(t *testing.T) {
	r := newTestRedis()

	r.ZAdd(cmd("ZADD", "k", "1", "a"))
	resp := r.ZAdd(cmd("ZADD", "k", "XX", "2", "a"))
	assert.Equal(t, []byte(":0\r\n"), resp)
}

func TestZAddGT_LT(t *testing.T) {
	r := newTestRedis()

	r.ZAdd(cmd("ZADD", "k", "1", "a"))

	resp := r.ZAdd(cmd("ZADD", "k", "GT", "2", "a"))
	assert.Equal(t, []byte(":0\r\n"), resp)

	resp = r.ZAdd(cmd("ZADD", "k", "LT", "0", "a"))
	assert.Equal(t, []byte(":0\r\n"), resp)
}

func TestZAddWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.ZAdd(cmd("ZADD", "k", "1"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestZAddWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.ZAdd(cmd("ZADD", "k", "1", "a"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestZAddConflictingOptions(t *testing.T) {
	r := newTestRedis()

	resp := r.ZAdd(cmd("ZADD", "k", "NX", "XX", "1", "a"))
	assert.Equal(t, core.RespXXNXNotCompatible, resp)

	resp = r.ZAdd(cmd("ZADD", "k", "NX", "GT", "1", "a"))
	assert.Equal(t, core.RespGTLTNXNotCompatible, resp)

	resp = r.ZAdd(cmd("ZADD", "k", "GT", "LT", "1", "a"))
	assert.Equal(t, core.RespGTLTNXNotCompatible, resp)
}

func TestZAddInvalidScore(t *testing.T) {
	r := newTestRedis()

	resp := r.ZAdd(cmd("ZADD", "k", "x", "a"))
	assert.Equal(t, core.RespSyntaxError, resp)
}

func TestZAddOddArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.ZAdd(cmd("ZADD", "k", "1", "a", "2"))
	assert.Equal(t, core.RespSyntaxError, resp)
}

func TestZCard(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a", "2", "b"))

	resp := r.ZCard(cmd("ZCARD", "k"))
	assert.Equal(t, []byte(":2\r\n"), resp)
}

func TestZCardMissing(t *testing.T) {
	r := newTestRedis()

	resp := r.ZCard(cmd("ZCARD", "missing"))
	assert.Equal(t, []byte(":0\r\n"), resp)
}

func TestZCardWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.ZCard(cmd("ZCARD", "k", "a"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestZCount(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a", "2", "b", "3", "c"))

	resp := r.ZCount(cmd("ZCOUNT", "k", "1", "2"))
	assert.Equal(t, []byte(":2\r\n"), resp)
}

func TestZCountWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.ZCount(cmd("ZCOUNT", "k", "1"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestZCountWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.ZCount(cmd("ZCOUNT", "k", "1", "2"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestZCountInvalidFloat(t *testing.T) {
	r := newTestRedis()

	resp := r.ZCount(cmd("ZCOUNT", "k", "a", "b"))
	assert.Equal(t, core.RespValueNotValidFloat, resp)
}

func TestZIncrBy(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a"))

	resp := r.ZIncrBy(cmd("ZINCRBY", "k", "2", "a"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('$'), resp[0])
}

func TestZIncrByWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.ZIncrBy(cmd("ZINCRBY", "k", "1"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestZIncrByWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.ZIncrBy(cmd("ZINCRBY", "k", "1", "a"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestZIncrByNewMember(t *testing.T) {
	r := newTestRedis()

	resp := r.ZIncrBy(cmd("ZINCRBY", "k", "2", "a"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('$'), resp[0])
}

func TestZIncrByInvalid(t *testing.T) {
	r := newTestRedis()

	resp := r.ZIncrBy(cmd("ZINCRBY", "k", "x", "a"))
	assert.Equal(t, core.RespValueNotValidFloat, resp)
}

func TestZRangeByRank(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a", "2", "b", "3", "c"))

	resp := r.ZRange(cmd("ZRANGE", "k", "0", "1"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestZRangeByScore(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a", "2", "b"))

	resp := r.ZRange(cmd("ZRANGE", "k", "1", "2", "BYSCORE"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestZRangeWithScores(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a"))

	resp := r.ZRange(cmd("ZRANGE", "k", "0", "0", "WITHSCORES"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestZRangeInvalidOptions(t *testing.T) {
	r := newTestRedis()

	resp := r.ZRange(cmd("ZRANGE", "k", "0", "1", "BYSCORE", "BYLEX"))
	assert.Equal(t, core.RespSyntaxError, resp)

	resp = r.ZRange(cmd("ZRANGE", "k", "a", "b"))
	assert.Equal(t, core.RespValueNotIntegerOrOutOfRange, resp)
}

func TestZRangeWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.ZRange(cmd("ZRANGE", "k"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestZRangeWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.ZRange(cmd("ZRANGE", "k", "0", "1"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestZRangeByLexWithScores(t *testing.T) {
	r := newTestRedis()

	resp := r.ZRange(cmd("ZRANGE", "k", "-", "+", "BYLEX", "WITHSCORES"))
	assert.Equal(t, core.RespWithScoresNotSupportedByLex, resp)
}

func TestZRangeInvalidScoreRange(t *testing.T) {
	r := newTestRedis()

	resp := r.ZRange(cmd("ZRANGE", "k", "a", "b", "BYSCORE"))
	assert.Equal(t, core.RespMinOrMaxNotFloat, resp)
}

func TestZRangeByLex(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a", "1", "b", "1", "c"))

	resp := r.ZRange(cmd("ZRANGE", "k", "-", "+", "BYLEX"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestZRangeByScoreRev(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a", "2", "b"))

	resp := r.ZRange(cmd("ZRANGE", "k", "1", "2", "BYSCORE", "REV"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestZRank(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a", "2", "b"))

	resp := r.ZRank(cmd("ZRANK", "k", "a"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte(':'), resp[0])
}

func TestZRankNonExisting(t *testing.T) {
	r := newTestRedis()

	resp := r.ZRank(cmd("ZRANK", "k", "a"))
	assert.Equal(t, core.RespNilBulkString, resp)
}

func TestZRankWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.ZRank(cmd("ZRANK", "k"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestZRankWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.ZRank(cmd("ZRANK", "k", "a"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestZRankMemberNotFound(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a"))

	resp := r.ZRank(cmd("ZRANK", "k", "x"))
	assert.Equal(t, core.RespNilBulkString, resp)
}

func TestZRevRankWithScore(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a"))

	resp := r.ZRevRank(cmd("ZREVRANK", "k", "a", "WITHSCORE"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestZRevRankInvalidOption(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "member1"))

	resp := r.ZRevRank(cmd("ZREVRANK", "k", "a", "BAD"))
	assert.Equal(t, core.RespSyntaxError, resp)
}

func TestZScore(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1.5", "a"))

	resp := r.ZScore(cmd("ZSCORE", "k", "a"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('$'), resp[0])
}

func TestZScoreWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.ZScore(cmd("ZSCORE", "k"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestZScoreWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.ZScore(cmd("ZSCORE", "k", "a"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestZScoreMemberNotFound(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a"))

	resp := r.ZScore(cmd("ZSCORE", "k", "x"))
	assert.Equal(t, core.RespNilBulkString, resp)
}

func TestZMScore(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a", "2", "b"))

	resp := r.ZMScore(cmd("ZMSCORE", "k", "a", "x", "b"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestZMScoreWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.ZMScore(cmd("ZMSCORE", "k"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestZMScoreWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.ZMScore(cmd("ZMSCORE", "k", "a"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestZMScoreMissingKey(t *testing.T) {
	r := newTestRedis()

	resp := r.ZMScore(cmd("ZMSCORE", "k", "a"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestZRem(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a", "2", "b"))

	resp := r.ZRem(cmd("ZREM", "k", "a", "x"))
	assert.Equal(t, []byte(":1\r\n"), resp)
}

func TestZRemMissingKey(t *testing.T) {
	r := newTestRedis()

	resp := r.ZRem(cmd("ZREM", "k", "a"))
	assert.Equal(t, []byte(":0\r\n"), resp)
}

func TestZRemWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.ZRem(cmd("ZREM", "k"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestZRemWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.ZRem(cmd("ZREM", "k", "a"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestZPopMax(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a", "2", "b"))

	resp := r.ZPopMax(cmd("ZPOPMAX", "k"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestZPopMaxWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.ZPopMax(cmd("ZPOPMAX", "k", "1", "2"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestZPopMaxWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.ZPopMax(cmd("ZPOPMAX", "k"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestZPopMaxInvalidCount(t *testing.T) {
	r := newTestRedis()

	resp := r.ZPopMax(cmd("ZPOPMAX", "k", "abc"))
	assert.Equal(t, core.RespValueOutOfRangeMustPositive, resp)
}

func TestZPopMaxNonExisting(t *testing.T) {
	r := newTestRedis()

	resp := r.ZPopMax(cmd("ZPOPMAX", "missing"))
	assert.Equal(t, "*0\r\n", string(resp))
}

func TestZPopMinCount(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a", "2", "b", "3", "c"))

	resp := r.ZPopMin(cmd("ZPOPMIN", "k", "2"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestZPopMinInvalidCount(t *testing.T) {
	r := newTestRedis()

	resp := r.ZPopMin(cmd("ZPOPMIN", "k", "-1"))
	assert.Equal(t, core.RespValueOutOfRangeMustPositive, resp)
}

func TestZRandMember(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a", "2", "b"))

	resp := r.ZRandMember(cmd("ZRANDMEMBER", "k"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('$'), resp[0])
}

func TestZRandMemberMissing(t *testing.T) {
	r := newTestRedis()

	resp := r.ZRandMember(cmd("ZRANDMEMBER", "missing"))
	assert.Equal(t, core.RespNilBulkString, resp)
}

func TestZRandMemberZeroCount(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a"))

	resp := r.ZRandMember(cmd("ZRANDMEMBER", "k", "0"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestZRandMemberWithScores(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a"))

	resp := r.ZRandMember(cmd("ZRANDMEMBER", "k", "1", "WITHSCORES"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestZRandMemberInvalidWithScores(t *testing.T) {
	r := newTestRedis()

	resp := r.ZRandMember(cmd("ZRANDMEMBER", "k", "1", "BAD"))
	assert.Equal(t, core.RespSyntaxError, resp)
}

func TestZRandMemberInvalidCount(t *testing.T) {
	r := newTestRedis()

	resp := r.ZRandMember(cmd("ZRANDMEMBER", "k", "abc"))
	assert.Equal(t, core.RespValueOutOfRangeMustPositive, resp)
}

func TestZRandMemberPositiveCount(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a", "2", "b", "3", "c"))

	resp := r.ZRandMember(cmd("ZRANDMEMBER", "k", "2"))
	require.NotEmpty(t, resp)

	// Array reply
	assert.Equal(t, byte('*'), resp[0])
	assert.Equal(t, byte('2'), resp[1])
}

func TestZRandMemberCountGreaterThanSize(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a", "2", "b"))

	resp := r.ZRandMember(cmd("ZRANDMEMBER", "k", "10"))
	require.NotEmpty(t, resp)

	assert.Equal(t, byte('*'), resp[0])
	assert.Equal(t, byte('2'), resp[1])
}

func TestZRandMemberNegativeCount(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a", "2", "b"))

	resp := r.ZRandMember(cmd("ZRANDMEMBER", "k", "-3"))
	require.NotEmpty(t, resp)

	// Still array reply
	assert.Equal(t, byte('*'), resp[0])
	assert.Equal(t, byte('3'), resp[1])
}

func TestZRandMemberNegativeCountWithScores(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a", "2", "b"))

	resp := r.ZRandMember(cmd("ZRANDMEMBER", "k", "-2", "WITHSCORES"))
	require.NotEmpty(t, resp)

	assert.Equal(t, byte('*'), resp[0])
	assert.Equal(t, byte('4'), resp[1])
}

func TestZRandMemberPositiveCountWithScores(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a", "2", "b", "3", "c"))

	resp := r.ZRandMember(cmd("ZRANDMEMBER", "k", "2", "WITHSCORES"))
	require.NotEmpty(t, resp)

	assert.Equal(t, byte('*'), resp[0])
	assert.Equal(t, byte('4'), resp[1])
}

func TestZLexCount(t *testing.T) {
	r := newTestRedis()
	r.ZAdd(cmd("ZADD", "k", "1", "a", "1", "b", "1", "c"))

	resp := r.ZLexCount(cmd("ZLEXCOUNT", "k", "-", "+"))
	assert.Equal(t, []byte(":3\r\n"), resp)
}

func TestZLexCountInvalidRange(t *testing.T) {
	r := newTestRedis()

	resp := r.ZLexCount(cmd("ZLEXCOUNT", "k", "[a", "z"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestZLexCountWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.ZLexCount(cmd("ZLEXCOUNT", "k"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestZLexCountWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.ZLexCount(cmd("ZLEXCOUNT", "k", "-", "+"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}
