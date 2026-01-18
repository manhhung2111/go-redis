package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/util"
)

// HGET Tests
func TestHGet_InvalidArity_NoArgs(t *testing.T) {
	r := newTestRedis()
	resp := r.HGet(cmd("HGET", "key"))
	expected := core.EncodeResp(util.InvalidNumberOfArgs("HGET"), false)
	assert.Equal(t, expected, resp)
}

func TestHGet_InvalidArity_TooManyArgs(t *testing.T) {
	r := newTestRedis()
	resp := r.HGet(cmd("HGET", "key", "field1", "field2"))
	expected := core.EncodeResp(util.InvalidNumberOfArgs("HGET"), false)
	assert.Equal(t, expected, resp)
}

func TestHGet_KeyNotExist(t *testing.T) {
	r := newTestRedis()
	resp := r.HGet(cmd("HGET", "missing", "field"))
	assert.Equal(t, core.RespNilBulkString, resp)
}

func TestHGet_WrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "key", "value"))
	resp := r.HGet(cmd("HGET", "key", "field"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestHGet_FieldNotExist(t *testing.T) {
	r := newTestRedis()
	r.HSet(cmd("HSET", "key", "field1", "value1"))
	resp := r.HGet(cmd("HGET", "key", "field2"))
	assert.Equal(t, core.RespNilBulkString, resp)
}

func TestHGet_Success(t *testing.T) {
	r := newTestRedis()
	r.HSet(cmd("HSET", "key", "field1", "value1"))
	resp := r.HGet(cmd("HGET", "key", "field1"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, "value1", val)
}

// HGETALL Tests
func TestHGetAll_InvalidArity(t *testing.T) {
	r := newTestRedis()
	resp := r.HGetAll(cmd("HGETALL"))
	expected := core.EncodeResp(util.InvalidNumberOfArgs("HGETALL"), false)
	assert.Equal(t, expected, resp)
}

func TestHGetAll_KeyNotExist(t *testing.T) {
	r := newTestRedis()
	resp := r.HGetAll(cmd("HGETALL", "missing"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, []interface{}{}, val)
}

func TestHGetAll_WrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "key", "value"))
	resp := r.HGetAll(cmd("HGETALL", "key"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestHGetAll_Success(t *testing.T) {
	r := newTestRedis()
	r.HSet(cmd("HSET", "key", "field1", "value1", "field2", "value2"))
	resp := r.HGetAll(cmd("HGETALL", "key"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	
	result := val.([]interface{})
	assert.Len(t, result, 4)
	assert.Contains(t, result, "field1")
	assert.Contains(t, result, "value1")
	assert.Contains(t, result, "field2")
	assert.Contains(t, result, "value2")
}

// HMGET Tests
func TestHMGet_InvalidArity(t *testing.T) {
	r := newTestRedis()
	resp := r.HMGet(cmd("HMGET", "key"))
	expected := core.EncodeResp(util.InvalidNumberOfArgs("HMGET"), false)
	assert.Equal(t, expected, resp)
}

func TestHMGet_WrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "key", "value"))
	resp := r.HMGet(cmd("HMGET", "key", "field1"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestHMGet_KeyNotExist(t *testing.T) {
	r := newTestRedis()
	resp := r.HMGet(cmd("HMGET", "missing", "field1", "field2"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	
	result := val.([]interface{})
	assert.Len(t, result, 2)
	assert.Nil(t, result[0])
	assert.Nil(t, result[1])
}

func TestHMGet_MixExistingAndNonExisting(t *testing.T) {
	r := newTestRedis()
	r.HSet(cmd("HSET", "key", "field1", "value1", "field3", "value3"))
	resp := r.HMGet(cmd("HMGET", "key", "field1", "field2", "field3"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	
	result := val.([]interface{})
	assert.Len(t, result, 3)
	assert.Equal(t, "value1", result[0])
	assert.Nil(t, result[1])
	assert.Equal(t, "value3", result[2])
}

// HINCRBY Tests
func TestHIncrBy_InvalidArity_TooFew(t *testing.T) {
	r := newTestRedis()
	resp := r.HIncrBy(cmd("HINCRBY", "key", "field"))
	expected := core.EncodeResp(util.InvalidNumberOfArgs("HINCRBY"), false)
	assert.Equal(t, expected, resp)
}

func TestHIncrBy_InvalidArity_TooMany(t *testing.T) {
	r := newTestRedis()
	resp := r.HIncrBy(cmd("HINCRBY", "key", "field", "5", "extra"))
	expected := core.EncodeResp(util.InvalidNumberOfArgs("HINCRBY"), false)
	assert.Equal(t, expected, resp)
}

func TestHIncrBy_WrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "key", "value"))
	resp := r.HIncrBy(cmd("HINCRBY", "key", "field", "5"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestHIncrBy_InvalidIncrement(t *testing.T) {
	r := newTestRedis()
	resp := r.HIncrBy(cmd("HINCRBY", "key", "field", "notanumber"))
	assert.Equal(t, core.RespValueNotIntegerOrOutOfRange, resp)
}

func TestHIncrBy_FieldNotInteger(t *testing.T) {
	r := newTestRedis()
	r.HSet(cmd("HSET", "key", "field", "notanumber"))
	resp := r.HIncrBy(cmd("HINCRBY", "key", "field", "5"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	
	// Should return error message
	_, ok := val.(string)
	assert.True(t, ok)
}

func TestHIncrBy_NewField(t *testing.T) {
	r := newTestRedis()
	resp := r.HIncrBy(cmd("HINCRBY", "key", "counter", "10"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(10), val)
}

func TestHIncrBy_ExistingField(t *testing.T) {
	r := newTestRedis()
	r.HSet(cmd("HSET", "key", "counter", "5"))
	resp := r.HIncrBy(cmd("HINCRBY", "key", "counter", "3"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(8), val)
}

func TestHIncrBy_NegativeIncrement(t *testing.T) {
	r := newTestRedis()
	r.HSet(cmd("HSET", "key", "counter", "10"))
	resp := r.HIncrBy(cmd("HINCRBY", "key", "counter", "-3"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(7), val)
}

// HKEYS Tests
func TestHKeys_InvalidArity(t *testing.T) {
	r := newTestRedis()
	resp := r.HKeys(cmd("HKEYS"))
	expected := core.EncodeResp(util.InvalidNumberOfArgs("HKEYS"), false)
	assert.Equal(t, expected, resp)
}

func TestHKeys_KeyNotExist(t *testing.T) {
	r := newTestRedis()
	resp := r.HKeys(cmd("HKEYS", "missing"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, []interface{}{}, val)
}

func TestHKeys_WrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "key", "value"))
	resp := r.HKeys(cmd("HKEYS", "key"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestHKeys_Success(t *testing.T) {
	r := newTestRedis()
	r.HSet(cmd("HSET", "key", "field1", "value1", "field2", "value2"))
	resp := r.HKeys(cmd("HKEYS", "key"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	
	result := val.([]interface{})
	assert.Len(t, result, 2)
	assert.Contains(t, result, "field1")
	assert.Contains(t, result, "field2")
}

// HVALS Tests
func TestHVals_InvalidArity(t *testing.T) {
	r := newTestRedis()
	resp := r.HVals(cmd("HVALS"))
	expected := core.EncodeResp(util.InvalidNumberOfArgs("HVALS"), false)
	assert.Equal(t, expected, resp)
}

func TestHVals_KeyNotExist(t *testing.T) {
	r := newTestRedis()
	resp := r.HVals(cmd("HVALS", "missing"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, []interface{}{}, val)
}

func TestHVals_WrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "key", "value"))
	resp := r.HVals(cmd("HVALS", "key"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestHVals_Success(t *testing.T) {
	r := newTestRedis()
	r.HSet(cmd("HSET", "key", "field1", "value1", "field2", "value2"))
	resp := r.HVals(cmd("HVALS", "key"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	
	result := val.([]interface{})
	assert.Len(t, result, 2)
	assert.Contains(t, result, "value1")
	assert.Contains(t, result, "value2")
}

// HLEN Tests
func TestHLen_InvalidArity(t *testing.T) {
	r := newTestRedis()
	resp := r.HLen(cmd("HLEN"))
	expected := core.EncodeResp(util.InvalidNumberOfArgs("HLEN"), false)
	assert.Equal(t, expected, resp)
}

func TestHLen_KeyNotExist(t *testing.T) {
	r := newTestRedis()
	resp := r.HLen(cmd("HLEN", "missing"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(0), val)
}

func TestHLen_WrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "key", "value"))
	resp := r.HLen(cmd("HLEN", "key"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestHLen_Success(t *testing.T) {
	r := newTestRedis()
	r.HSet(cmd("HSET", "key", "field1", "value1", "field2", "value2", "field3", "value3"))
	resp := r.HLen(cmd("HLEN", "key"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(3), val)
}

// HSET Tests
func TestHSet_InvalidArity_TooFew(t *testing.T) {
	r := newTestRedis()
	resp := r.HSet(cmd("HSET", "key", "field"))
	expected := core.EncodeResp(util.InvalidNumberOfArgs("HSET"), false)
	assert.Equal(t, expected, resp)
}

func TestHSet_InvalidArity_EvenArgs(t *testing.T) {
	r := newTestRedis()
	resp := r.HSet(cmd("HSET", "key", "field1", "value1", "field2"))
	expected := core.EncodeResp(util.InvalidNumberOfArgs("HSET"), false)
	assert.Equal(t, expected, resp)
}

func TestHSet_WrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "key", "value"))
	resp := r.HSet(cmd("HSET", "key", "field", "value"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestHSet_NewFields(t *testing.T) {
	r := newTestRedis()
	resp := r.HSet(cmd("HSET", "key", "field1", "value1", "field2", "value2"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(2), val)
	
	// Verify fields were set
	getResp := r.HGet(cmd("HGET", "key", "field1"))
	getVal, _, err := core.DecodeResp(getResp)
	require.NoError(t, err)
	assert.Equal(t, "value1", getVal)
}

func TestHSet_UpdateExistingField(t *testing.T) {
	r := newTestRedis()
	r.HSet(cmd("HSET", "key", "field1", "value1"))
	resp := r.HSet(cmd("HSET", "key", "field1", "newvalue"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(0), val) // No new fields added
	
	// Verify field was updated
	getResp := r.HGet(cmd("HGET", "key", "field1"))
	getVal, _, err := core.DecodeResp(getResp)
	require.NoError(t, err)
	assert.Equal(t, "newvalue", getVal)
}

func TestHSet_MixNewAndExisting(t *testing.T) {
	r := newTestRedis()
	r.HSet(cmd("HSET", "key", "field1", "value1"))
	resp := r.HSet(cmd("HSET", "key", "field1", "newvalue", "field2", "value2"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(1), val) // Only field2 is new
}

// HSETNX Tests
func TestHSetNx_InvalidArity_TooFew(t *testing.T) {
	r := newTestRedis()
	resp := r.HSetNx(cmd("HSETNX", "key", "field"))
	expected := core.EncodeResp(util.InvalidNumberOfArgs("HSETNX"), false)
	assert.Equal(t, expected, resp)
}

func TestHSetNx_InvalidArity_TooMany(t *testing.T) {
	r := newTestRedis()
	resp := r.HSetNx(cmd("HSETNX", "key", "field", "value", "extra"))
	expected := core.EncodeResp(util.InvalidNumberOfArgs("HSETNX"), false)
	assert.Equal(t, expected, resp)
}

func TestHSetNx_WrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "key", "value"))
	resp := r.HSetNx(cmd("HSETNX", "key", "field", "value"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestHSetNx_NewField(t *testing.T) {
	r := newTestRedis()
	resp := r.HSetNx(cmd("HSETNX", "key", "field", "value"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(1), val)
	
	// Verify field was set
	getResp := r.HGet(cmd("HGET", "key", "field"))
	getVal, _, err := core.DecodeResp(getResp)
	require.NoError(t, err)
	assert.Equal(t, "value", getVal)
}

func TestHSetNx_ExistingField(t *testing.T) {
	r := newTestRedis()
	r.HSet(cmd("HSET", "key", "field", "value1"))
	resp := r.HSetNx(cmd("HSETNX", "key", "field", "value2"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(0), val)
	
	// Verify field was not updated
	getResp := r.HGet(cmd("HGET", "key", "field"))
	getVal, _, err := core.DecodeResp(getResp)
	require.NoError(t, err)
	assert.Equal(t, "value1", getVal)
}

// HDEL Tests
func TestHDel_InvalidArity(t *testing.T) {
	r := newTestRedis()
	resp := r.HDel(cmd("HDEL", "key"))
	expected := core.EncodeResp(util.InvalidNumberOfArgs("HDEL"), false)
	assert.Equal(t, expected, resp)
}

func TestHDel_KeyNotExist(t *testing.T) {
	r := newTestRedis()
	resp := r.HDel(cmd("HDEL", "missing", "field1"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(0), val)
}

func TestHDel_WrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "key", "value"))
	resp := r.HDel(cmd("HDEL", "key", "field"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestHDel_SingleField(t *testing.T) {
	r := newTestRedis()
	r.HSet(cmd("HSET", "key", "field1", "value1", "field2", "value2"))
	resp := r.HDel(cmd("HDEL", "key", "field1"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(1), val)
	
	// Verify field was deleted
	getResp := r.HGet(cmd("HGET", "key", "field1"))
	assert.Equal(t, core.RespNilBulkString, getResp)
}

func TestHDel_MultipleFields(t *testing.T) {
	r := newTestRedis()
	r.HSet(cmd("HSET", "key", "field1", "value1", "field2", "value2", "field3", "value3"))
	resp := r.HDel(cmd("HDEL", "key", "field1", "field3"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(2), val)
	
	// Verify remaining field
	lenResp := r.HLen(cmd("HLEN", "key"))
	lenVal, _, err := core.DecodeResp(lenResp)
	require.NoError(t, err)
	assert.Equal(t, int64(1), lenVal)
}

func TestHDel_NonExistentFields(t *testing.T) {
	r := newTestRedis()
	r.HSet(cmd("HSET", "key", "field1", "value1"))
	resp := r.HDel(cmd("HDEL", "key", "field2", "field3"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(0), val)
}

func TestHDel_RemovesKeyWhenEmpty(t *testing.T) {
	r := newTestRedis()
	r.HSet(cmd("HSET", "key", "field1", "value1"))
	r.HDel(cmd("HDEL", "key", "field1"))
	
	// Verify key was deleted
	getResp := r.HGet(cmd("HGET", "key", "field1"))
	assert.Equal(t, core.RespNilBulkString, getResp)
}

// HEXISTS Tests
func TestHExists_InvalidArity_TooFew(t *testing.T) {
	r := newTestRedis()
	resp := r.HExists(cmd("HEXISTS", "key"))
	expected := core.EncodeResp(util.InvalidNumberOfArgs("HEXISTS"), false)
	assert.Equal(t, expected, resp)
}

func TestHExists_InvalidArity_TooMany(t *testing.T) {
	r := newTestRedis()
	resp := r.HExists(cmd("HEXISTS", "key", "field", "extra"))
	expected := core.EncodeResp(util.InvalidNumberOfArgs("HEXISTS"), false)
	assert.Equal(t, expected, resp)
}

func TestHExists_KeyNotExist(t *testing.T) {
	r := newTestRedis()
	resp := r.HExists(cmd("HEXISTS", "missing", "field"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(0), val)
}

func TestHExists_WrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "key", "value"))
	resp := r.HExists(cmd("HEXISTS", "key", "field"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestHExists_FieldExists(t *testing.T) {
	r := newTestRedis()
	r.HSet(cmd("HSET", "key", "field", "value"))
	resp := r.HExists(cmd("HEXISTS", "key", "field"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(1), val)
}

func TestHExists_FieldNotExist(t *testing.T) {
	r := newTestRedis()
	r.HSet(cmd("HSET", "key", "field1", "value1"))
	resp := r.HExists(cmd("HEXISTS", "key", "field2"))
	
	val, _, err := core.DecodeResp(resp)
	require.NoError(t, err)
	assert.Equal(t, int64(0), val)
}