package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manhhung2111/go-redis/internal/protocol"
	"github.com/manhhung2111/go-redis/internal/errors"
)

func TestTTL_InvalidNumberOfArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.TTL(cmd("TTL"))
	assert.Equal(t, protocol.EncodeResp(errors.InvalidNumberOfArgs("TTL"), false), resp)
}

func TestTTL_NoKey(t *testing.T) {
	r := newTestRedis()

	resp := r.TTL(cmd("TTL", "missing"))
	assert.Equal(t, protocol.RespTTLKeyNotExist, resp)
}

func TestTTL_NoExpire(t *testing.T) {
	r := newTestRedis()

	r.Set(cmd("SET", "foo", "bar"))
	resp := r.TTL(cmd("TTL", "foo"))

	assert.Equal(t, protocol.RespTTLKeyExistNoExpire, resp)
}

func TestTTL_WithExpire(t *testing.T) {
	r := newTestRedis()

	r.Set(cmd("SET", "foo", "bar", "EX", "2"))
	resp := r.TTL(cmd("TTL", "foo"))

	val, _, err := protocol.DecodeResp(resp)
	require.NoError(t, err)

	ttl := val.(int64)
	assert.Greater(t, ttl, int64(0))
	assert.LessOrEqual(t, ttl, int64(2))
}

func TestExpire_InvalidArity(t *testing.T) {
	r := newTestRedis()

	resp := r.Expire(cmd("EXPIRE", "key"))
	expected := protocol.EncodeResp(errors.InvalidNumberOfArgs("EXPIRE"), false)

	assert.Equal(t, expected, resp)
}

func TestExpire_InvalidTTL_NonNumeric(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.Expire(cmd("EXPIRE", "k", "abc"))
	expected := protocol.EncodeResp(errors.InvalidExpireTime("EXPIRE"), false)

	assert.Equal(t, expected, resp)
}

func TestExpire_InvalidTTL_Zero(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.Expire(cmd("EXPIRE", "k", "0"))
	expected := protocol.EncodeResp(errors.InvalidExpireTime("EXPIRE"), false)

	assert.Equal(t, expected, resp)
}

func TestExpire_InvalidTTL_Negative(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.Expire(cmd("EXPIRE", "k", "-10"))
	expected := protocol.EncodeResp(errors.InvalidExpireTime("EXPIRE"), false)

	assert.Equal(t, expected, resp)
}

func TestExpire_InvalidOption(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.Expire(cmd("EXPIRE", "k", "10", "BAD"))
	expected := protocol.EncodeResp(errors.InvalidCommandOption("BAD", "EXPIRE"), false)

	assert.Equal(t, expected, resp)
}

func TestExpire_IncompatibleOptions_NX_XX(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.Expire(cmd("EXPIRE", "k", "10", "NX", "XX"))
	assert.Equal(t, protocol.RespExpireOptionsNotCompatible, resp)
}

func TestExpire_IncompatibleOptions_GT_LT(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.Expire(cmd("EXPIRE", "k", "10", "GT", "LT"))
	assert.Equal(t, protocol.RespExpireOptionsNotCompatible, resp)
}

func TestExpire_IncompatibleOptions_NX_GT(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.Expire(cmd("EXPIRE", "k", "10", "NX", "GT"))
	assert.Equal(t, protocol.RespExpireOptionsNotCompatible, resp)
}

func TestExpire_KeyNotExist(t *testing.T) {
	r := newTestRedis()

	resp := r.Expire(cmd("EXPIRE", "missing", "10"))
	assert.Equal(t, protocol.RespExpireTimeoutNotSet, resp)
}

func TestExpire_NX_FirstSet(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.Expire(cmd("EXPIRE", "k", "10", "NX"))
	assert.Equal(t, protocol.RespExpireTimeoutSet, resp)
}

func TestExpire_NX_RejectWhenTTLExists(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))
	r.Expire(cmd("EXPIRE", "k", "10"))

	resp := r.Expire(cmd("EXPIRE", "k", "20", "NX"))
	assert.Equal(t, protocol.RespExpireTimeoutNotSet, resp)
}

func TestExpire_XX_RejectWhenNoTTL(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.Expire(cmd("EXPIRE", "k", "10", "XX"))
	assert.Equal(t, protocol.RespExpireTimeoutNotSet, resp)
}

func TestExpire_XX_AcceptWhenTTLExists(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))
	r.Expire(cmd("EXPIRE", "k", "5"))

	resp := r.Expire(cmd("EXPIRE", "k", "10", "XX"))
	assert.Equal(t, protocol.RespExpireTimeoutSet, resp)
}

func TestExpire_GT_RejectSmallerTTL(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))
	r.Expire(cmd("EXPIRE", "k", "10"))

	resp := r.Expire(cmd("EXPIRE", "k", "5", "GT"))
	assert.Equal(t, protocol.RespExpireTimeoutNotSet, resp)
}

func TestExpire_GT_AcceptLargerTTL(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))
	r.Expire(cmd("EXPIRE", "k", "10"))

	resp := r.Expire(cmd("EXPIRE", "k", "20", "GT"))
	assert.Equal(t, protocol.RespExpireTimeoutSet, resp)
}

func TestExpire_LT_RejectLargerTTL(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))
	r.Expire(cmd("EXPIRE", "k", "10"))

	resp := r.Expire(cmd("EXPIRE", "k", "20", "LT"))
	assert.Equal(t, protocol.RespExpireTimeoutNotSet, resp)
}

func TestExpire_LT_AcceptSmallerTTL(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))
	r.Expire(cmd("EXPIRE", "k", "10"))

	resp := r.Expire(cmd("EXPIRE", "k", "5", "LT"))
	assert.Equal(t, protocol.RespExpireTimeoutSet, resp)
}