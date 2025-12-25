package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manhhung2111/go-redis/internal/constant"
)

func TestSAdd(t *testing.T) {
	r := newTestRedis()

	resp := r.SAdd(cmd("SADD", "k", "a", "b"))
	assert.Equal(t, []byte(":2\r\n"), resp)

	resp = r.SAdd(cmd("SADD", "k", "a"))
	assert.Equal(t, []byte(":0\r\n"), resp)
}

func TestSAddWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.SAdd(cmd("SADD", "k"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestSAddWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.SAdd(cmd("SADD", "k", "a"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestSCard(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "b"))

	resp := r.SCard(cmd("SCARD", "k"))
	assert.Equal(t, []byte(":2\r\n"), resp)
}

func TestSIsMember(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a"))

	resp := r.SIsMember(cmd("SISMEMBER", "k", "a"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	resp = r.SIsMember(cmd("SISMEMBER", "k", "b"))
	assert.Equal(t, []byte(":0\r\n"), resp)
}

func TestSMembers(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "b"))

	resp := r.SMembers(cmd("SMEMBERS", "k"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestSMIsMember(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "c"))

	resp := r.SMIsMember(cmd("SMISMEMBER", "k", "a", "b", "c"))
	expected := "*3\r\n:1\r\n:0\r\n:1\r\n"

	assert.Equal(t, expected, string(resp))
}

func TestSRem(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "b"))

	resp := r.SRem(cmd("SREM", "k", "a", "x"))
	assert.Equal(t, []byte(":1\r\n"), resp)
}

func TestSPopSingle(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "b"))

	resp := r.SPop(cmd("SPOP", "k"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('$'), resp[0])
}

func TestSPopCount(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "b", "c"))

	resp := r.SPop(cmd("SPOP", "k", "2"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestSPopNil(t *testing.T) {
	r := newTestRedis()

	resp := r.SPop(cmd("SPOP", "missing"))
	assert.Equal(t, constant.RESP_NIL_BULK_STRING, resp)
}

func TestSRandMemberSingle(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "b"))

	resp := r.SRandMember(cmd("SRANDMEMBER", "k"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('$'), resp[0])
}

func TestSRandMemberCountPositive(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "b", "c"))

	resp := r.SRandMember(cmd("SRANDMEMBER", "k", "2"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestSRandMemberCountNegative(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "b", "c"))

	resp := r.SRandMember(cmd("SRANDMEMBER", "k", "-10"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestSetWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.SAdd(cmd("SADD", "k", "a"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}