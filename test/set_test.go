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

func TestSCardWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.SCard(cmd("SCARD", "k", "a"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestSCardWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.SCard(cmd("SCARD", "k"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestSIsMember(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a"))

	resp := r.SIsMember(cmd("SISMEMBER", "k", "a"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	resp = r.SIsMember(cmd("SISMEMBER", "k", "b"))
	assert.Equal(t, []byte(":0\r\n"), resp)
}

func TestSIsMemberWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.SIsMember(cmd("SISMEMBER", "k", "a", "b"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestSIsMemberWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.SIsMember(cmd("SISMEMBER", "k", "a"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestSMembers(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "b"))

	resp := r.SMembers(cmd("SMEMBERS", "k"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestSMembersWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.SMembers(cmd("SMEMBERS", "k", "a"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestSMembersWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.SMembers(cmd("SMEMBERS", "k"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestSMIsMember(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "c"))

	resp := r.SMIsMember(cmd("SMISMEMBER", "k", "a", "b", "c"))
	expected := "*3\r\n:1\r\n:0\r\n:1\r\n"

	assert.Equal(t, expected, string(resp))
}

func TestSMIsMemberWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.SMIsMember(cmd("SMISMEMBER", "k"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestSMIsMemberWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.SMIsMember(cmd("SMISMEMBER", "k", "a"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestSRem(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "b"))

	resp := r.SRem(cmd("SREM", "k", "a", "x"))
	assert.Equal(t, []byte(":1\r\n"), resp)
}

func TestSRemWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.SRem(cmd("SREM", "k"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestSRemWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.SRem(cmd("SREM", "k", "a"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
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

func TestSPopWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.SPop(cmd("SPOP", "k", "1", "2"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestSPopWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.SPop(cmd("SPOP", "k"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestSPopInvalidValue(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "b"))

	resp := r.SPop(cmd("SPOP", "k", "-1"))
	assert.Equal(t, constant.RESP_VALUE_IS_OUT_OF_RANGE_MUST_BE_POSITIVE, resp)
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

func TestSRandMemberWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.SRandMember(cmd("SRANDMEMBER", "k", "1", "2"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestSRandMemberNonExisting(t *testing.T) {
	r := newTestRedis()

	resp := r.SRandMember(cmd("SRANDMEMBER", "k"))
	require.NotEmpty(t, resp)
	assert.Equal(t, []byte("*0"), resp[:2])
}

func TestSRandMemberWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.SRandMember(cmd("SRANDMEMBER", "k"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestSRandMemberInvalidValue(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "b"))

	resp := r.SRandMember(cmd("SRANDMEMBER", "k", "v"))
	assert.Equal(t, constant.RESP_VALUE_IS_OUT_OF_RANGE_MUST_BE_POSITIVE, resp)
}

func TestSetWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.SAdd(cmd("SADD", "k", "a"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}