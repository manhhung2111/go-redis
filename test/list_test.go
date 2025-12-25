package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manhhung2111/go-redis/internal/constant"
)

func TestLPush(t *testing.T) {
	r := newTestRedis()

	resp := r.LPush(cmd("LPUSH", "mylist", "a", "b", "c"))
	assert.Equal(t, []byte(":3\r\n"), resp)

	resp = r.LPush(cmd("LPUSH", "mylist", "d"))
	assert.Equal(t, []byte(":4\r\n"), resp)
}

func TestLPushWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.LPush(cmd("LPUSH", "mylist"))
	assert.Equal(t, byte('-'), resp[0])

	resp = r.LPush(cmd("LPUSH"))
	assert.Equal(t, byte('-'), resp[0])
}

func TestLPushWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "mykey", "value"))

	resp := r.LPush(cmd("LPUSH", "mykey", "a"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestLPopSingle(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "mylist", "a", "b", "c"))

	resp := r.LPop(cmd("LPOP", "mylist"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('$'), resp[0])
}

func TestLPopWithCount(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "mylist", "a", "b", "c", "d", "e"))

	resp := r.LPop(cmd("LPOP", "mylist", "3"))
	assert.Equal(t, []byte("*3\r\n")[0], resp[0])
}

func TestLPopNil(t *testing.T) {
	r := newTestRedis()

	resp := r.LPop(cmd("LPOP", "missing"))
	assert.Equal(t, constant.RESP_NIL_BULK_STRING, resp)
}

func TestLPopWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.LPop(cmd("LPOP"))
	assert.Equal(t, byte('-'), resp[0])
}

func TestLPopInvalidCount(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "mylist", "a", "b"))

	resp := r.LPop(cmd("LPOP", "mylist", "abc"))
	assert.Equal(t, constant.RESP_VALUE_IS_OUT_OF_RANGE_MUST_BE_POSITIVE, resp)

	resp = r.LPop(cmd("LPOP", "mylist", "-1"))
	assert.Equal(t, constant.RESP_VALUE_IS_OUT_OF_RANGE_MUST_BE_POSITIVE, resp)
}

func TestLPopWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "mykey", "value"))

	resp := r.LPop(cmd("LPOP", "mykey"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestRPush(t *testing.T) {
	r := newTestRedis()

	resp := r.RPush(cmd("RPUSH", "mylist", "a", "b", "c"))
	assert.Equal(t, []byte(":3\r\n"), resp)

	resp = r.RPush(cmd("RPUSH", "mylist", "d", "e"))
	assert.Equal(t, []byte(":5\r\n"), resp)
}

func TestRPushWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.RPush(cmd("RPUSH", "mylist"))
	assert.Equal(t, byte('-'), resp[0])
}

func TestRPushWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "mykey", "value"))

	resp := r.RPush(cmd("RPUSH", "mykey", "a"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestRPopSingle(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "mylist", "a", "b", "c"))

	resp := r.RPop(cmd("RPOP", "mylist"))
	assert.Equal(t, byte('$'), resp[0])
}

func TestRPopWithCount(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "mylist", "a", "b", "c", "d", "e"))

	resp := r.RPop(cmd("RPOP", "mylist", "2"))
	assert.Equal(t, []byte("*2\r\n")[0], resp[0])
}

func TestRPopNil(t *testing.T) {
	r := newTestRedis()

	resp := r.RPop(cmd("RPOP", "missing"))
	assert.Equal(t, constant.RESP_NIL_BULK_STRING, resp)
}

func TestRPopWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.RPop(cmd("RPOP"))
	assert.Equal(t, byte('-'), resp[0])
}

func TestRPopInvalidCount(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "mylist", "a", "b"))

	resp := r.RPop(cmd("RPOP", "mylist", "invalid"))
	assert.Equal(t, constant.RESP_VALUE_IS_OUT_OF_RANGE_MUST_BE_POSITIVE, resp)

	resp = r.RPop(cmd("RPOP", "mylist", "-5"))
	assert.Equal(t, constant.RESP_VALUE_IS_OUT_OF_RANGE_MUST_BE_POSITIVE, resp)
}

func TestRPopWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "mykey", "value"))

	resp := r.RPop(cmd("RPOP", "mykey"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestLRange(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "mylist", "a", "b", "c", "d", "e"))

	resp := r.LRange(cmd("LRANGE", "mylist", "0", "-1"))
	assert.Equal(t, byte('*'), resp[0])
}

func TestLRangePartial(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "mylist", "a", "b", "c", "d", "e"))

	resp := r.LRange(cmd("LRANGE", "mylist", "1", "3"))
	assert.Equal(t, []byte("*3\r\n")[0], resp[0])
}

func TestLRangeNegativeIndices(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "mylist", "a", "b", "c", "d", "e"))

	resp := r.LRange(cmd("LRANGE", "mylist", "-3", "-1"))
	assert.Equal(t, []byte("*3\r\n")[0], resp[0])
}

func TestLRangeEmpty(t *testing.T) {
	r := newTestRedis()

	resp := r.LRange(cmd("LRANGE", "missing", "0", "-1"))
	assert.Equal(t, []byte("*0\r\n"), resp)
}

func TestLRangeOutOfBounds(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "mylist", "a", "b", "c"))

	resp := r.LRange(cmd("LRANGE", "mylist", "5", "1"))
	assert.Equal(t, []byte("*0\r\n"), resp)

	resp = r.LRange(cmd("LRANGE", "mylist", "10", "20"))
	assert.Equal(t, []byte("*0\r\n"), resp)
}

func TestLRangeWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.LRange(cmd("LRANGE", "mylist", "0"))
	assert.Equal(t, byte('-'), resp[0])

	resp = r.LRange(cmd("LRANGE", "mylist"))
	assert.Equal(t, byte('-'), resp[0])

	resp = r.LRange(cmd("LRANGE", "mylist", "0", "1", "x"))
	assert.Equal(t, byte('-'), resp[0])
}

func TestLRangeInvalidIndices(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "mylist", "a"))

	resp := r.LRange(cmd("LRANGE", "mylist", "abc", "2"))
	assert.Equal(t, constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE, resp)

	resp = r.LRange(cmd("LRANGE", "mylist", "0", "xyz"))
	assert.Equal(t, constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE, resp)
}

func TestLRangeWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "mykey", "value"))

	resp := r.LRange(cmd("LRANGE", "mykey", "0", "-1"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestLIndex(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "k", "c", "b", "a"))

	resp := r.LIndex(cmd("LINDEX", "k", "0"))
	assert.Equal(t, []byte("$1\r\na\r\n"), resp)

	resp = r.LIndex(cmd("LINDEX", "k", "2"))
	assert.Equal(t, []byte("$1\r\nc\r\n"), resp)
}

func TestLIndexNegative(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "k", "c", "b", "a"))

	resp := r.LIndex(cmd("LINDEX", "k", "-1"))
	assert.Equal(t, []byte("$1\r\nc\r\n"), resp)
}

func TestLIndexOutOfRange(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "k", "a"))

	resp := r.LIndex(cmd("LINDEX", "k", "10"))
	assert.Equal(t, constant.RESP_NIL_BULK_STRING, resp)
}

func TestLIndexNonExistentKey(t *testing.T) {
	r := newTestRedis()

	resp := r.LIndex(cmd("LINDEX", "missing", "0"))
	assert.Equal(t, constant.RESP_NIL_BULK_STRING, resp)
}

func TestLIndexWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.LIndex(cmd("LINDEX", "k", "0"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestLIndexInvalidIndex(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "k", "a"))

	resp := r.LIndex(cmd("LINDEX", "k", "notanumber"))
	assert.Equal(t, constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE, resp)
}

func TestLIndexWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.LIndex(cmd("LINDEX", "k"))
	assert.Equal(t, byte('-'), resp[0])
}

func TestLLen(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "k", "a", "b", "c"))

	resp := r.LLen(cmd("LLEN", "k"))
	assert.Equal(t, []byte(":3\r\n"), resp)
}

func TestLLenNonExistentKey(t *testing.T) {
	r := newTestRedis()

	resp := r.LLen(cmd("LLEN", "missing"))
	assert.Equal(t, []byte(":0\r\n"), resp)
}

func TestLLenWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.LLen(cmd("LLEN", "k"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestLLenWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.LLen(cmd("LLEN"))
	assert.Equal(t, byte('-'), resp[0])
}