package test

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/manhhung2111/go-redis/internal/constant"
	"github.com/manhhung2111/go-redis/internal/core"
)

func TestGet(t *testing.T) {
	r := newTestRedis()

	resp := r.Get(cmd("GET", "a"))
	assert.Equal(t, constant.RESP_NIL_BULK_STRING, resp)

	r.Set(cmd("SET", "a", "hello"))
	resp = r.Get(cmd("GET", "a"))
	expected := core.EncodeResp("hello", false)
	assert.Equal(t, expected, resp)

	resp = r.Get(cmd("GET", "b"))
	assert.Equal(t, constant.RESP_NIL_BULK_STRING, resp)
}

/* -------------------- SET -------------------- */

func TestSet(t *testing.T) {
	r := newTestRedis()

	resp := r.Set(cmd("SET", "a", "1"))
	assert.Equal(t, constant.RESP_OK, resp)

	r.Set(cmd("SET", "foo", "bar"))
	resp = r.Set(cmd("SET", "foo", "baz", "NX"))
	assert.Equal(t, constant.RESP_NIL_BULK_STRING, resp)

	resp = r.Set(cmd("SET", "foo", "bar", "XX"))
	assert.Equal(t, constant.RESP_OK, resp)
}

func TestDel(t *testing.T) {
	r := newTestRedis()

	r.Set(cmd("SET", "a", "1"))
	r.Set(cmd("SET", "b", "2"))

	resp := r.Del(cmd("DEL", "a", "b", "c"))
	expected := core.EncodeResp(int64(2), false)
	assert.Equal(t, expected, resp)
}

func TestIncr(t *testing.T) {
	r := newTestRedis()

	resp := r.Incr(cmd("INCR", "a"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	resp = r.Incr(cmd("INCR", "a"))
	assert.Equal(t, []byte(":2\r\n"), resp)

	r.Set(cmd("SET", "b", "foo"))
	resp = r.Incr(cmd("INCR", "b"))
	assert.Equal(t, constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE, resp)

	r.Set(cmd("SET", "c", "9223372036854775807"))
	resp = r.Incr(cmd("INCR", "c"))
	assert.Equal(t, constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE, resp)
}

func TestIncrBy(t *testing.T) {
	r := newTestRedis()

	resp := r.IncrBy(cmd("INCRBY", "a", "5"))
	assert.Equal(t, []byte(":5\r\n"), resp)

	resp = r.IncrBy(cmd("INCRBY", "a", "3"))
	assert.Equal(t, []byte(":8\r\n"), resp)

	resp = r.IncrBy(cmd("INCRBY", "a", "-2"))
	assert.Equal(t, []byte(":6\r\n"), resp)

	r.Set(cmd("SET", "b", "foo"))
	resp = r.IncrBy(cmd("INCRBY", "b", "1"))
	assert.Equal(t, constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE, resp)

	r.Set(cmd("SET", "c", "9223372036854775807"))
	resp = r.IncrBy(cmd("INCRBY", "c", "1"))
	assert.Equal(t, constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE, resp)
}

func TestDecr(t *testing.T) {
	r := newTestRedis()

	resp := r.Decr(cmd("DECR", "a"))
	assert.Equal(t, []byte(":-1\r\n"), resp)

	resp = r.Decr(cmd("DECR", "a"))
	assert.Equal(t, []byte(":-2\r\n"), resp)

	r.Set(cmd("SET", "b", "foo"))
	resp = r.Decr(cmd("DECR", "b"))
	assert.Equal(t, constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE, resp)

	r.Set(cmd("SET", "c", "-9223372036854775808"))
	resp = r.Decr(cmd("DECR", "c"))
	assert.Equal(t, constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE, resp)
}

func TestDecrBy(t *testing.T) {
	r := newTestRedis()

	resp := r.DecrBy(cmd("DECRBY", "a", "5"))
	assert.Equal(t, []byte(":-5\r\n"), resp)

	resp = r.DecrBy(cmd("DECRBY", "a", "3"))
	assert.Equal(t, []byte(":-8\r\n"), resp)

	resp = r.DecrBy(cmd("DECRBY", "a", "-2"))
	assert.Equal(t, []byte(":-6\r\n"), resp)

	r.Set(cmd("SET", "b", "foo"))
	resp = r.DecrBy(cmd("DECRBY", "b", "1"))
	assert.Equal(t, constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE, resp)

	r.Set(cmd("SET", "c", strconv.FormatInt(math.MinInt64, 10)))
	resp = r.DecrBy(cmd("DECRBY", "c", "1"))
	assert.Equal(t, constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE, resp)
}