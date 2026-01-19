package test

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/manhhung2111/go-redis/internal/protocol"
	"github.com/manhhung2111/go-redis/internal/errors"
)

func TestGet(t *testing.T) {
	r := newTestRedis()

	resp := r.Get(cmd("GET", "a"))
	assert.Equal(t, protocol.RespNilBulkString, resp)

	r.Set(cmd("SET", "a", "hello"))
	resp = r.Get(cmd("GET", "a"))
	expected := protocol.EncodeResp("hello", false)
	assert.Equal(t, expected, resp)

	resp = r.Get(cmd("GET", "b"))
	assert.Equal(t, protocol.RespNilBulkString, resp)
}

func TestGet_InvalidArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.Get(cmd("GET"))
	expected := protocol.EncodeResp(
		errors.InvalidNumberOfArgs("GET"),
		false,
	)

	assert.Equal(t, expected, resp)

	resp = r.Get(cmd("GET", "a", "b"))
	assert.Equal(t, expected, resp)
}

func TestMGet_InvalidArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.MGet(cmd("MGET"))
	expected := protocol.EncodeResp(
		errors.InvalidNumberOfArgs("MGET"),
		false,
	)

	assert.Equal(t, expected, resp)
}

func TestMGet_SingleKey(t *testing.T) {
	r := newTestRedis()

	r.Set(cmd("SET", "a", "hello"))

	resp := r.MGet(cmd("MGET", "a"))
	expected := protocol.EncodeResp([]string{"hello"}, false)

	assert.Equal(t, expected, resp)
}

func TestMGet_NonExistentKey(t *testing.T) {
	r := newTestRedis()

	resp := r.MGet(cmd("MGET", "a"))
	expected := protocol.EncodeResp([]*string{nil}, false)

	assert.Equal(t, expected, resp)
}

func TestMGet_WrongType(t *testing.T) {
	r := newTestRedis()

	// create non-string type
	r.SAdd(cmd("SADD", "a", "x"))

	resp := r.MGet(cmd("MGET", "a"))
	expected := protocol.EncodeResp([]*string{nil}, false)

	assert.Equal(t, expected, resp)
}

func TestSet(t *testing.T) {
	r := newTestRedis()

	resp := r.Set(cmd("SET", "a", "1"))
	assert.Equal(t, protocol.RespOK, resp)

	r.Set(cmd("SET", "foo", "bar"))
	resp = r.Set(cmd("SET", "foo", "baz", "NX"))
	assert.Equal(t, protocol.RespNilBulkString, resp)

	resp = r.Set(cmd("SET", "foo", "bar", "XX"))
	assert.Equal(t, protocol.RespOK, resp)
}

func TestSet_InvalidArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.Set(cmd("SET"))
	expected := protocol.EncodeResp(
		errors.InvalidNumberOfArgs("SET"),
		false,
	)

	assert.Equal(t, expected, resp)
}

func TestDel(t *testing.T) {
	r := newTestRedis()

	r.Set(cmd("SET", "a", "1"))
	r.Set(cmd("SET", "b", "2"))

	resp := r.Del(cmd("DEL", "a", "b", "c"))
	expected := protocol.EncodeResp(int64(2), false)
	assert.Equal(t, expected, resp)
}

func TestDel_InvalidArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.Del(cmd("DEL"))
	expected := protocol.EncodeResp(
		errors.InvalidNumberOfArgs("DEL"),
		false,
	)

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
	assert.Equal(t, protocol.RespValueNotIntegerOrOutOfRange, resp)

	r.Set(cmd("SET", "c", "9223372036854775807"))
	resp = r.Incr(cmd("INCR", "c"))
	assert.Equal(t, protocol.RespValueNotIntegerOrOutOfRange, resp)
}

func TestIncr_InvalidArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.Incr(cmd("INCR"))
	expected := protocol.EncodeResp(
		errors.InvalidNumberOfArgs("INCR"),
		false,
	)

	assert.Equal(t, expected, resp)

	resp = r.Incr(cmd("INCR", "a", "b"))
	assert.Equal(t, expected, resp)
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
	assert.Equal(t, protocol.RespValueNotIntegerOrOutOfRange, resp)

	r.Set(cmd("SET", "c", "9223372036854775807"))
	resp = r.IncrBy(cmd("INCRBY", "c", "1"))
	assert.Equal(t, protocol.RespValueNotIntegerOrOutOfRange, resp)
}

func TestIncrBy_InvalidArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.IncrBy(cmd("INCRBY", "a"))
	expected := protocol.EncodeResp(
		errors.InvalidNumberOfArgs("INCRBY"),
		false,
	)

	assert.Equal(t, expected, resp)
}

func TestIncrBy_InvalidIncrement(t *testing.T) {
	r := newTestRedis()

	resp := r.IncrBy(cmd("INCRBY", "a", "foo"))
	assert.Equal(t, protocol.RespValueNotIntegerOrOutOfRange, resp)

	resp = r.IncrBy(cmd("INCRBY", "a", "1.5"))
	assert.Equal(t, protocol.RespValueNotIntegerOrOutOfRange, resp)
}

func TestDecr(t *testing.T) {
	r := newTestRedis()

	resp := r.Decr(cmd("DECR", "a"))
	assert.Equal(t, []byte(":-1\r\n"), resp)

	resp = r.Decr(cmd("DECR", "a"))
	assert.Equal(t, []byte(":-2\r\n"), resp)

	r.Set(cmd("SET", "b", "foo"))
	resp = r.Decr(cmd("DECR", "b"))
	assert.Equal(t, protocol.RespValueNotIntegerOrOutOfRange, resp)

	r.Set(cmd("SET", "c", "-9223372036854775808"))
	resp = r.Decr(cmd("DECR", "c"))
	assert.Equal(t, protocol.RespValueNotIntegerOrOutOfRange, resp)
}

func TestDecr_InvalidArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.Decr(cmd("DECR"))
	expected := protocol.EncodeResp(
		errors.InvalidNumberOfArgs("DECR"),
		false,
	)

	assert.Equal(t, expected, resp)

	resp = r.Decr(cmd("DECR", "a", "b"))
	assert.Equal(t, expected, resp)
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
	assert.Equal(t, protocol.RespValueNotIntegerOrOutOfRange, resp)

	r.Set(cmd("SET", "c", strconv.FormatInt(math.MinInt64, 10)))
	resp = r.DecrBy(cmd("DECRBY", "c", "1"))
	assert.Equal(t, protocol.RespValueNotIntegerOrOutOfRange, resp)
}

func TestDecrBy_InvalidDecrement(t *testing.T) {
	r := newTestRedis()

	resp := r.DecrBy(cmd("DECRBY", "a", "foo"))
	assert.Equal(t, protocol.RespValueNotIntegerOrOutOfRange, resp)

	resp = r.DecrBy(cmd("DECRBY", "a", "1.5"))
	assert.Equal(t, protocol.RespValueNotIntegerOrOutOfRange, resp)
}

func TestDecrBy_InvalidArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.DecrBy(cmd("DECRBY", "a"))
	expected := protocol.EncodeResp(
		errors.InvalidNumberOfArgs("DECRBY"),
		false,
	)

	assert.Equal(t, expected, resp)
}

func TestMSet_EmptyArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.MSet(cmd("MSET"))
	expected := protocol.EncodeResp(
		errors.InvalidNumberOfArgs("MSET"),
		false,
	)

	assert.Equal(t, expected, resp)
}

func TestMSet_OddArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.MSet(cmd("MSET", "a", "1", "b"))
	expected := protocol.EncodeResp(
		errors.InvalidNumberOfArgs("MSET"),
		false,
	)

	assert.Equal(t, expected, resp)
}

func TestMSet_Valid(t *testing.T) {
	r := newTestRedis()

	resp := r.MSet(cmd("MSET", "a", "1", "b", "2"))
	assert.Equal(t, protocol.RespOK, resp)

	resp = r.MGet(cmd("MGET", "a", "b"))
	expected := protocol.EncodeResp([]string{"1", "2"}, false)

	assert.Equal(t, expected, resp)
}

func TestMSet_Overwrite(t *testing.T) {
	r := newTestRedis()

	r.MSet(cmd("MSET", "a", "1", "b", "2"))
	resp := r.MSet(cmd("MSET", "a", "10", "b", "20"))
	assert.Equal(t, protocol.RespOK, resp)

	resp = r.MGet(cmd("MGET", "a", "b"))
	expected := protocol.EncodeResp([]string{"10", "20"}, false)

	assert.Equal(t, expected, resp)
}
