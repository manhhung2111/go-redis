package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manhhung2111/go-redis/internal/protocol"
	"github.com/manhhung2111/go-redis/internal/errors"
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
	assert.Equal(t, protocol.RespWrongTypeOperation, resp)
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
	assert.Equal(t, protocol.RespNilBulkString, resp)
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
	assert.Equal(t, protocol.RespValueOutOfRangeMustPositive, resp)

	resp = r.LPop(cmd("LPOP", "mylist", "-1"))
	assert.Equal(t, protocol.RespValueOutOfRangeMustPositive, resp)
}

func TestLPopWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "mykey", "value"))

	resp := r.LPop(cmd("LPOP", "mykey"))
	assert.Equal(t, protocol.RespWrongTypeOperation, resp)
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
	assert.Equal(t, protocol.RespWrongTypeOperation, resp)
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
	assert.Equal(t, protocol.RespNilBulkString, resp)
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
	assert.Equal(t, protocol.RespValueOutOfRangeMustPositive, resp)

	resp = r.RPop(cmd("RPOP", "mylist", "-5"))
	assert.Equal(t, protocol.RespValueOutOfRangeMustPositive, resp)
}

func TestRPopWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "mykey", "value"))

	resp := r.RPop(cmd("RPOP", "mykey"))
	assert.Equal(t, protocol.RespWrongTypeOperation, resp)
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
	assert.Equal(t, protocol.RespValueNotIntegerOrOutOfRange, resp)

	resp = r.LRange(cmd("LRANGE", "mylist", "0", "xyz"))
	assert.Equal(t, protocol.RespValueNotIntegerOrOutOfRange, resp)
}

func TestLRangeWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "mykey", "value"))

	resp := r.LRange(cmd("LRANGE", "mykey", "0", "-1"))
	assert.Equal(t, protocol.RespWrongTypeOperation, resp)
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
	assert.Equal(t, protocol.RespNilBulkString, resp)
}

func TestLIndexNonExistentKey(t *testing.T) {
	r := newTestRedis()

	resp := r.LIndex(cmd("LINDEX", "missing", "0"))
	assert.Equal(t, protocol.RespNilBulkString, resp)
}

func TestLIndexWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.LIndex(cmd("LINDEX", "k", "0"))
	assert.Equal(t, protocol.RespWrongTypeOperation, resp)
}

func TestLIndexInvalidIndex(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "k", "a"))

	resp := r.LIndex(cmd("LINDEX", "k", "notanumber"))
	assert.Equal(t, protocol.RespValueNotIntegerOrOutOfRange, resp)
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
	assert.Equal(t, protocol.RespWrongTypeOperation, resp)
}

func TestLLenWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.LLen(cmd("LLEN"))
	assert.Equal(t, byte('-'), resp[0])
}

func TestLPushX_InvalidArity(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"no args", []string{"LPUSHX"}},
		{"only key", []string{"LPUSHX", "key"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRedis()
			resp := r.LPushX(cmd(tt.args[0], tt.args[1:]...))
			expected := protocol.EncodeResp(errors.InvalidNumberOfArgs("LPUSHX"), false)
			assert.Equal(t, expected, resp)
		})
	}
}

func TestLPushX_KeyNotExist(t *testing.T) {
	r := newTestRedis()
	resp := r.LPushX(cmd("LPUSHX", "nonexistent", "value"))

	val, _, err := protocol.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(0), val)
}

func TestLPushX_WrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "key", "value"))
	resp := r.LPushX(cmd("LPUSHX", "key", "element"))
	assert.Equal(t, protocol.RespWrongTypeOperation, resp)
}

func TestLPushX_SingleElement(t *testing.T) {
	r := newTestRedis()
	// Create list first with LPUSH
	r.LPush(cmd("LPUSH", "list", "initial"))
	resp := r.LPushX(cmd("LPUSHX", "list", "new"))

	val, _, err := protocol.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(2), val)

	// Verify order (new should be at head)
	rangeResp := r.LRange(cmd("LRANGE", "list", "0", "-1"))
	rangeVal, _, err := protocol.DecodeResp(rangeResp)
	require.NoError(t, err)
	result := rangeVal.([]interface{})
	assert.Equal(t, "new", result[0])
	assert.Equal(t, "initial", result[1])
}

func TestLPushX_MultipleElements(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "list", "initial"))
	resp := r.LPushX(cmd("LPUSHX", "list", "elem1", "elem2", "elem3"))

	val, _, err := protocol.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(4), val)

	// Verify order (pushed in order: elem1, elem2, elem3, so elem3 is at head)
	rangeResp := r.LRange(cmd("LRANGE", "list", "0", "-1"))
	rangeVal, _, err := protocol.DecodeResp(rangeResp)
	require.NoError(t, err)
	result := rangeVal.([]interface{})
	assert.Equal(t, "elem3", result[0])
	assert.Equal(t, "elem2", result[1])
	assert.Equal(t, "elem1", result[2])
	assert.Equal(t, "initial", result[3])
}

func TestLPushX_EmptyValue(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "list", "initial"))
	resp := r.LPushX(cmd("LPUSHX", "list", ""))

	val, _, err := protocol.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(2), val)
}

func TestLRem_InvalidArity(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"no args", []string{"LREM"}},
		{"only key", []string{"LREM", "key"}},
		{"only key and count", []string{"LREM", "key", "1"}},
		{"too many args", []string{"LREM", "key", "1", "elem", "extra"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRedis()
			resp := r.LRem(cmd(tt.args[0], tt.args[1:]...))
			expected := protocol.EncodeResp(errors.InvalidNumberOfArgs("LREM"), false)
			assert.Equal(t, expected, resp)
		})
	}
}

func TestLRem_KeyNotExist(t *testing.T) {
	r := newTestRedis()
	resp := r.LRem(cmd("LREM", "nonexistent", "0", "value"))

	val, _, err := protocol.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(0), val)
}

func TestLRem_WrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "key", "value"))
	resp := r.LRem(cmd("LREM", "key", "0", "element"))
	assert.Equal(t, protocol.RespWrongTypeOperation, resp)
}

func TestLRem_InvalidCount(t *testing.T) {
	tests := []struct {
		name  string
		count string
	}{
		{"not a number", "notanumber"},
		{"float", "3.14"},
		{"empty", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRedis()
			r.LPush(cmd("LPUSH", "list", "value"))
			resp := r.LRem(cmd("LREM", "list", tt.count, "value"))
			assert.Equal(t, protocol.RespValueNotIntegerOrOutOfRange, resp)
		})
	}
}

func TestLRem_RemoveAll(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "list", "a", "b", "a", "c", "a"))
	resp := r.LRem(cmd("LREM", "list", "0", "a"))

	val, _, err := protocol.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(3), val)

	// Verify remaining elements
	lenResp := r.LLen(cmd("LLEN", "list"))
	lenVal, _, err := protocol.DecodeResp(lenResp)
	require.NoError(t, err)
	assert.Equal(t, int64(2), lenVal)
}

func TestLRem_RemoveFromHead(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "list", "a", "b", "a", "c", "a"))
	resp := r.LRem(cmd("LREM", "list", "2", "a"))

	val, _, err := protocol.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(2), val)
}

func TestLRem_RemoveFromTail(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "list", "a", "b", "a", "c", "a"))
	resp := r.LRem(cmd("LREM", "list", "-2", "a"))

	val, _, err := protocol.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(2), val)
}

func TestLRem_ElementNotFound(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "list", "a", "b", "c"))
	resp := r.LRem(cmd("LREM", "list", "0", "notfound"))

	val, _, err := protocol.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(0), val)
}

func TestLRem_CountLargerThanOccurrences(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "list", "a", "b", "a"))
	resp := r.LRem(cmd("LREM", "list", "10", "a"))

	val, _, err := protocol.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(2), val)
}

func TestLSet_InvalidArity(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"no args", []string{"LSET"}},
		{"only key", []string{"LSET", "key"}},
		{"only key and index", []string{"LSET", "key", "0"}},
		{"too many args", []string{"LSET", "key", "0", "value", "extra"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRedis()
			resp := r.LSet(cmd(tt.args[0], tt.args[1:]...))
			expected := protocol.EncodeResp(errors.InvalidNumberOfArgs("LSET"), false)
			assert.Equal(t, expected, resp)
		})
	}
}

func TestLSet_WrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "key", "value"))
	resp := r.LSet(cmd("LSET", "key", "0", "newvalue"))
	assert.Equal(t, protocol.RespWrongTypeOperation, resp)
}

func TestLSet_InvalidIndex(t *testing.T) {
	tests := []struct {
		name  string
		index string
	}{
		{"not a number", "notanumber"},
		{"float", "3.14"},
		{"empty", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRedis()
			r.LPush(cmd("LPUSH", "list", "value"))
			resp := r.LSet(cmd("LSET", "list", tt.index, "newvalue"))
			assert.Equal(t, protocol.RespValueNotIntegerOrOutOfRange, resp)
		})
	}
}

func TestLSet_KeyNotExist(t *testing.T) {
	r := newTestRedis()
	resp := r.LSet(cmd("LSET", "nonexistent", "0", "value"))

	_, _, err := protocol.DecodeResp(resp)
	require.NoError(t, err)
	// Should return an error (likely "no such key" or similar)
	assert.NotEqual(t, protocol.RespOK, resp)
}

func TestLSet_IndexOutOfRange_Positive(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "list", "a", "b", "c"))
	resp := r.LSet(cmd("LSET", "list", "10", "newvalue"))

	_, _, err := protocol.DecodeResp(resp)
	require.NoError(t, err)
	// Should return error
	assert.NotEqual(t, protocol.RespOK, resp)
}

func TestLSet_IndexOutOfRange_Negative(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "list", "a", "b", "c"))
	resp := r.LSet(cmd("LSET", "list", "-10", "newvalue"))

	_, _, err := protocol.DecodeResp(resp)
	require.NoError(t, err)
	// Should return error
	assert.NotEqual(t, protocol.RespOK, resp)
}

func TestLSet_PositiveIndex(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "list", "a", "b", "c"))
	resp := r.LSet(cmd("LSET", "list", "1", "newvalue"))

	assert.Equal(t, protocol.RespOK, resp)

	// Verify the value was set
	indexResp := r.LIndex(cmd("LINDEX", "list", "1"))
	indexVal, _, err := protocol.DecodeResp(indexResp)
	require.NoError(t, err)
	assert.Equal(t, "newvalue", indexVal)
}

func TestLSet_NegativeIndex(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "list", "a", "b", "c"))
	resp := r.LSet(cmd("LSET", "list", "-1", "newvalue"))

	assert.Equal(t, protocol.RespOK, resp)

	// Verify the last element was set
	indexResp := r.LIndex(cmd("LINDEX", "list", "-1"))
	indexVal, _, err := protocol.DecodeResp(indexResp)
	require.NoError(t, err)
	assert.Equal(t, "newvalue", indexVal)
}

func TestLSet_IndexZero(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "list", "a", "b", "c"))
	resp := r.LSet(cmd("LSET", "list", "0", "newvalue"))

	assert.Equal(t, protocol.RespOK, resp)

	// Verify the first element was set
	indexResp := r.LIndex(cmd("LINDEX", "list", "0"))
	indexVal, _, err := protocol.DecodeResp(indexResp)
	require.NoError(t, err)
	assert.Equal(t, "newvalue", indexVal)
}

func TestLSet_EmptyValue(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "list", "a", "b", "c"))
	resp := r.LSet(cmd("LSET", "list", "0", ""))

	assert.Equal(t, protocol.RespOK, resp)

	indexResp := r.LIndex(cmd("LINDEX", "list", "0"))
	indexVal, _, err := protocol.DecodeResp(indexResp)
	require.NoError(t, err)
	assert.Equal(t, "", indexVal)
}

func TestLTrim_InvalidArity(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"no args", []string{"LTRIM"}},
		{"only key", []string{"LTRIM", "key"}},
		{"only key and start", []string{"LTRIM", "key", "0"}},
		{"too many args", []string{"LTRIM", "key", "0", "1", "extra"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRedis()
			resp := r.LTrim(cmd(tt.args[0], tt.args[1:]...))
			expected := protocol.EncodeResp(errors.InvalidNumberOfArgs("LTRIM"), false)
			assert.Equal(t, expected, resp)
		})
	}
}

func TestLTrim_KeyNotExist(t *testing.T) {
	r := newTestRedis()
	resp := r.LTrim(cmd("LTRIM", "nonexistent", "0", "-1"))
	assert.Equal(t, protocol.RespOK, resp)
}

func TestLTrim_WrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "key", "value"))
	resp := r.LTrim(cmd("LTRIM", "key", "0", "-1"))
	assert.Equal(t, protocol.RespWrongTypeOperation, resp)
}

func TestLTrim_InvalidStart(t *testing.T) {
	tests := []struct {
		name  string
		start string
	}{
		{"not a number", "notanumber"},
		{"float", "3.14"},
		{"empty", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRedis()
			r.LPush(cmd("LPUSH", "list", "a", "b", "c"))
			resp := r.LTrim(cmd("LTRIM", "list", tt.start, "1"))
			assert.Equal(t, protocol.RespValueNotIntegerOrOutOfRange, resp)
		})
	}
}

func TestLTrim_InvalidStop(t *testing.T) {
	tests := []struct {
		name string
		stop string
	}{
		{"not a number", "notanumber"},
		{"float", "3.14"},
		{"empty", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRedis()
			r.LPush(cmd("LPUSH", "list", "a", "b", "c"))
			resp := r.LTrim(cmd("LTRIM", "list", "0", tt.stop))
			assert.Equal(t, protocol.RespValueNotIntegerOrOutOfRange, resp)
		})
	}
}

func TestLTrim_KeepAll(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "list", "a", "b", "c", "d", "e"))
	resp := r.LTrim(cmd("LTRIM", "list", "0", "-1"))

	assert.Equal(t, protocol.RespOK, resp)

	lenResp := r.LLen(cmd("LLEN", "list"))
	lenVal, _, err := protocol.DecodeResp(lenResp)
	require.NoError(t, err)
	assert.Equal(t, int64(5), lenVal)
}

func TestLTrim_KeepMiddle(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "list", "a", "b", "c", "d", "e"))
	resp := r.LTrim(cmd("LTRIM", "list", "1", "3"))

	assert.Equal(t, protocol.RespOK, resp)

	lenResp := r.LLen(cmd("LLEN", "list"))
	lenVal, _, err := protocol.DecodeResp(lenResp)
	require.NoError(t, err)
	assert.Equal(t, int64(3), lenVal)
}

func TestLTrim_KeepFirst(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "list", "a", "b", "c", "d", "e"))
	resp := r.LTrim(cmd("LTRIM", "list", "0", "2"))

	assert.Equal(t, protocol.RespOK, resp)

	lenResp := r.LLen(cmd("LLEN", "list"))
	lenVal, _, err := protocol.DecodeResp(lenResp)
	require.NoError(t, err)
	assert.Equal(t, int64(3), lenVal)
}

func TestLTrim_KeepLast(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "list", "a", "b", "c", "d", "e"))
	resp := r.LTrim(cmd("LTRIM", "list", "-3", "-1"))

	assert.Equal(t, protocol.RespOK, resp)

	lenResp := r.LLen(cmd("LLEN", "list"))
	lenVal, _, err := protocol.DecodeResp(lenResp)
	require.NoError(t, err)
	assert.Equal(t, int64(3), lenVal)
}

func TestLTrim_EmptyList(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "list", "a", "b", "c"))
	resp := r.LTrim(cmd("LTRIM", "list", "10", "20"))

	assert.Equal(t, protocol.RespOK, resp)

	// List should be empty or deleted
	lenResp := r.LLen(cmd("LLEN", "list"))
	lenVal, _, err := protocol.DecodeResp(lenResp)
	require.NoError(t, err)
	assert.Equal(t, int64(0), lenVal)
}

func TestLTrim_StartGreaterThanStop(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "list", "a", "b", "c"))
	resp := r.LTrim(cmd("LTRIM", "list", "3", "1"))

	assert.Equal(t, protocol.RespOK, resp)

	lenResp := r.LLen(cmd("LLEN", "list"))
	lenVal, _, err := protocol.DecodeResp(lenResp)
	require.NoError(t, err)
	assert.Equal(t, int64(0), lenVal)
}

func TestLTrim_NegativeIndices(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "list", "a", "b", "c", "d", "e"))
	resp := r.LTrim(cmd("LTRIM", "list", "-4", "-2"))

	assert.Equal(t, protocol.RespOK, resp)

	lenResp := r.LLen(cmd("LLEN", "list"))
	lenVal, _, err := protocol.DecodeResp(lenResp)
	require.NoError(t, err)
	assert.Equal(t, int64(3), lenVal)
}

func TestRPushX_InvalidArity(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"no args", []string{"RPUSHX"}},
		{"only key", []string{"RPUSHX", "key"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestRedis()
			resp := r.RPushX(cmd(tt.args[0], tt.args[1:]...))
			expected := protocol.EncodeResp(errors.InvalidNumberOfArgs("RPUSHX"), false)
			assert.Equal(t, expected, resp)
		})
	}
}

func TestRPushX_KeyNotExist(t *testing.T) {
	r := newTestRedis()
	resp := r.RPushX(cmd("RPUSHX", "nonexistent", "value"))

	val, _, err := protocol.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(0), val)
}

func TestRPushX_WrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "key", "value"))
	resp := r.RPushX(cmd("RPUSHX", "key", "element"))
	assert.Equal(t, protocol.RespWrongTypeOperation, resp)
}

func TestRPushX_SingleElement(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "list", "initial"))
	resp := r.RPushX(cmd("RPUSHX", "list", "new"))

	val, _, err := protocol.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(2), val)

	// Verify order (new should be at tail)
	rangeResp := r.LRange(cmd("LRANGE", "list", "0", "-1"))
	rangeVal, _, err := protocol.DecodeResp(rangeResp)
	require.NoError(t, err)
	result := rangeVal.([]interface{})
	assert.Equal(t, "initial", result[0])
	assert.Equal(t, "new", result[1])
}

func TestRPushX_MultipleElements(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "list", "initial"))
	resp := r.RPushX(cmd("RPUSHX", "list", "elem1", "elem2", "elem3"))

	val, _, err := protocol.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(4), val)

	// Verify order
	rangeResp := r.LRange(cmd("LRANGE", "list", "0", "-1"))
	rangeVal, _, err := protocol.DecodeResp(rangeResp)
	require.NoError(t, err)
	result := rangeVal.([]interface{})
	assert.Equal(t, "initial", result[0])
	assert.Equal(t, "elem1", result[1])
	assert.Equal(t, "elem2", result[2])
	assert.Equal(t, "elem3", result[3])
}

func TestRPushX_EmptyValue(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "list", "initial"))
	resp := r.RPushX(cmd("RPUSHX", "list", ""))

	val, _, err := protocol.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(2), val)
}

func TestRPushX_AfterLPush(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "list", "head"))
	resp := r.RPushX(cmd("RPUSHX", "list", "tail"))

	val, _, err := protocol.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(2), val)

	// Verify order
	rangeResp := r.LRange(cmd("LRANGE", "list", "0", "-1"))
	rangeVal, _, err := protocol.DecodeResp(rangeResp)
	require.NoError(t, err)
	result := rangeVal.([]interface{})
	assert.Equal(t, "head", result[0])
	assert.Equal(t, "tail", result[1])
}