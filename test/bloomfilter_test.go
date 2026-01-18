package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manhhung2111/go-redis/internal/protocol"
)

func TestBFAdd(t *testing.T) {
	r := newTestRedis()

	resp := r.BFAdd(cmd("BF.ADD", "bf", "item1"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	// Adding same item again should return 0
	resp = r.BFAdd(cmd("BF.ADD", "bf", "item1"))
	assert.Equal(t, []byte(":0\r\n"), resp)

	// Adding different item should return 1
	resp = r.BFAdd(cmd("BF.ADD", "bf", "item2"))
	assert.Equal(t, []byte(":1\r\n"), resp)
}

func TestBFAddWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.BFAdd(cmd("BF.ADD", "bf"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	resp = r.BFAdd(cmd("BF.ADD", "bf", "item1", "item2"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestBFAddWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.BFAdd(cmd("BF.ADD", "k", "item1"))
	assert.Equal(t, protocol.RespWrongTypeOperation, resp)
}

func TestBFCard(t *testing.T) {
	r := newTestRedis()

	// Non-existing key should return 0
	resp := r.BFCard(cmd("BF.CARD", "bf"))
	assert.Equal(t, []byte(":0\r\n"), resp)

	// Add items and check cardinality
	r.BFAdd(cmd("BF.ADD", "bf", "item1"))
	r.BFAdd(cmd("BF.ADD", "bf", "item2"))

	resp = r.BFCard(cmd("BF.CARD", "bf"))
	assert.Equal(t, []byte(":2\r\n"), resp)
}

func TestBFCardWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.BFCard(cmd("BF.CARD"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	resp = r.BFCard(cmd("BF.CARD", "bf", "extra"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestBFCardWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.BFCard(cmd("BF.CARD", "k"))
	assert.Equal(t, protocol.RespWrongTypeOperation, resp)
}

func TestBFExists(t *testing.T) {
	r := newTestRedis()

	// Non-existing key should return 0
	resp := r.BFExists(cmd("BF.EXISTS", "bf", "item1"))
	assert.Equal(t, []byte(":0\r\n"), resp)

	// Add item and check existence
	r.BFAdd(cmd("BF.ADD", "bf", "item1"))

	resp = r.BFExists(cmd("BF.EXISTS", "bf", "item1"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	resp = r.BFExists(cmd("BF.EXISTS", "bf", "item2"))
	assert.Equal(t, []byte(":0\r\n"), resp)
}

func TestBFExistsWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.BFExists(cmd("BF.EXISTS", "bf"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	resp = r.BFExists(cmd("BF.EXISTS", "bf", "item1", "item2"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestBFExistsWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.BFExists(cmd("BF.EXISTS", "k", "item1"))
	assert.Equal(t, protocol.RespWrongTypeOperation, resp)
}

func TestBFInfo(t *testing.T) {
	r := newTestRedis()

	// Create a bloom filter with BF.ADD
	r.BFAdd(cmd("BF.ADD", "bf", "item1"))

	// Get all info
	resp := r.BFInfo(cmd("BF.INFO", "bf"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestBFInfoCapacity(t *testing.T) {
	r := newTestRedis()
	r.BFAdd(cmd("BF.ADD", "bf", "item1"))

	resp := r.BFInfo(cmd("BF.INFO", "bf", "CAPACITY"))
	require.NotEmpty(t, resp)
	// Response is an array with label and value
	assert.Equal(t, byte('*'), resp[0])
}

func TestBFInfoSize(t *testing.T) {
	r := newTestRedis()
	r.BFAdd(cmd("BF.ADD", "bf", "item1"))

	resp := r.BFInfo(cmd("BF.INFO", "bf", "SIZE"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestBFInfoFilters(t *testing.T) {
	r := newTestRedis()
	r.BFAdd(cmd("BF.ADD", "bf", "item1"))

	resp := r.BFInfo(cmd("BF.INFO", "bf", "FILTERS"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestBFInfoItems(t *testing.T) {
	r := newTestRedis()
	r.BFAdd(cmd("BF.ADD", "bf", "item1"))

	resp := r.BFInfo(cmd("BF.INFO", "bf", "ITEMS"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestBFInfoExpansion(t *testing.T) {
	r := newTestRedis()
	r.BFAdd(cmd("BF.ADD", "bf", "item1"))

	resp := r.BFInfo(cmd("BF.INFO", "bf", "EXPANSION"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestBFInfoCaseInsensitive(t *testing.T) {
	r := newTestRedis()
	r.BFAdd(cmd("BF.ADD", "bf", "item1"))

	resp := r.BFInfo(cmd("BF.INFO", "bf", "capacity"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])

	resp = r.BFInfo(cmd("BF.INFO", "bf", "Capacity"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestBFInfoInvalidOption(t *testing.T) {
	r := newTestRedis()
	r.BFAdd(cmd("BF.ADD", "bf", "item1"))

	resp := r.BFInfo(cmd("BF.INFO", "bf", "INVALID"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestBFInfoWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.BFInfo(cmd("BF.INFO"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	resp = r.BFInfo(cmd("BF.INFO", "bf", "CAPACITY", "extra"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestBFInfoWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.BFInfo(cmd("BF.INFO", "k"))
	assert.Equal(t, protocol.RespWrongTypeOperation, resp)
}

func TestBFMAdd(t *testing.T) {
	r := newTestRedis()

	resp := r.BFMAdd(cmd("BF.MADD", "bf", "item1", "item2", "item3"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
	assert.Contains(t, string(resp), "*3\r\n")
}

func TestBFMAddDuplicates(t *testing.T) {
	r := newTestRedis()

	// First add
	r.BFMAdd(cmd("BF.MADD", "bf", "item1", "item2"))

	// Add with some duplicates
	resp := r.BFMAdd(cmd("BF.MADD", "bf", "item1", "item3"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
	// item1 should return 0, item3 should return 1
	assert.Contains(t, string(resp), ":0\r\n")
	assert.Contains(t, string(resp), ":1\r\n")
}

func TestBFMAddWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.BFMAdd(cmd("BF.MADD", "bf"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestBFMAddWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.BFMAdd(cmd("BF.MADD", "k", "item1"))
	assert.Equal(t, protocol.RespWrongTypeOperation, resp)
}

func TestBFMExists(t *testing.T) {
	r := newTestRedis()

	r.BFMAdd(cmd("BF.MADD", "bf", "item1", "item3"))

	resp := r.BFMExists(cmd("BF.MEXISTS", "bf", "item1", "item2", "item3"))
	expected := "*3\r\n:1\r\n:0\r\n:1\r\n"
	assert.Equal(t, expected, string(resp))
}

func TestBFMExistsNonExisting(t *testing.T) {
	r := newTestRedis()

	resp := r.BFMExists(cmd("BF.MEXISTS", "bf", "item1", "item2"))
	expected := "*2\r\n:0\r\n:0\r\n"
	assert.Equal(t, expected, string(resp))
}

func TestBFMExistsWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.BFMExists(cmd("BF.MEXISTS", "bf"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestBFMExistsWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.BFMExists(cmd("BF.MEXISTS", "k", "item1"))
	assert.Equal(t, protocol.RespWrongTypeOperation, resp)
}

func TestBFReserve(t *testing.T) {
	r := newTestRedis()

	resp := r.BFReserve(cmd("BF.RESERVE", "bf", "0.01", "1000"))
	assert.Equal(t, protocol.RespOK, resp)

	// Verify bloom filter was created by adding an item
	resp = r.BFAdd(cmd("BF.ADD", "bf", "item1"))
	assert.Equal(t, []byte(":1\r\n"), resp)
}

func TestBFReserveWithExpansion(t *testing.T) {
	r := newTestRedis()

	resp := r.BFReserve(cmd("BF.RESERVE", "bf", "0.01", "1000", "EXPANSION", "4"))
	assert.Equal(t, protocol.RespOK, resp)
}

func TestBFReserveExpansionCaseInsensitive(t *testing.T) {
	r := newTestRedis()

	resp := r.BFReserve(cmd("BF.RESERVE", "bf", "0.01", "1000", "expansion", "4"))
	assert.Equal(t, protocol.RespOK, resp)
}

func TestBFReserveWrongArgs(t *testing.T) {
	r := newTestRedis()

	// Too few args
	resp := r.BFReserve(cmd("BF.RESERVE", "bf", "0.01"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	// Wrong number of args (4 args - missing expansion value)
	resp = r.BFReserve(cmd("BF.RESERVE", "bf", "0.01", "1000", "EXPANSION"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestBFReserveWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.BFReserve(cmd("BF.RESERVE", "k", "0.01", "1000"))
	assert.Equal(t, protocol.RespItemExists, resp)
}

func TestBFReserveItemExists(t *testing.T) {
	r := newTestRedis()

	// Create bloom filter first
	r.BFReserve(cmd("BF.RESERVE", "bf", "0.01", "1000"))

	// Try to reserve again
	resp := r.BFReserve(cmd("BF.RESERVE", "bf", "0.01", "1000"))
	assert.Equal(t, protocol.RespItemExists, resp)
}

func TestBFReserveBadErrorRate(t *testing.T) {
	r := newTestRedis()

	resp := r.BFReserve(cmd("BF.RESERVE", "bf", "invalid", "1000"))
	assert.Equal(t, protocol.RespBadErrorRate, resp)
}

func TestBFReserveErrorRateOutOfRange(t *testing.T) {
	r := newTestRedis()

	// Error rate > 1
	resp := r.BFReserve(cmd("BF.RESERVE", "bf", "1.5", "1000"))
	assert.Equal(t, protocol.RespErrorRateInvalidRange, resp)

	// Error rate < 0
	resp = r.BFReserve(cmd("BF.RESERVE", "bf2", "-0.1", "1000"))
	assert.Equal(t, protocol.RespErrorRateInvalidRange, resp)
}

func TestBFReserveBadCapacity(t *testing.T) {
	r := newTestRedis()

	resp := r.BFReserve(cmd("BF.RESERVE", "bf", "0.01", "invalid"))
	assert.Equal(t, protocol.RespBadCapacity, resp)
}

func TestBFReserveCapacityOutOfRange(t *testing.T) {
	r := newTestRedis()

	// Capacity < min (0)
	resp := r.BFReserve(cmd("BF.RESERVE", "bf", "0.01", "0"))
	assert.Equal(t, protocol.RespCapacityInvalidRange, resp)

	// Capacity > max (1073741825)
	resp = r.BFReserve(cmd("BF.RESERVE", "bf2", "0.01", "1073741825"))
	assert.Equal(t, protocol.RespCapacityInvalidRange, resp)
}

func TestBFReserveSyntaxError(t *testing.T) {
	r := newTestRedis()

	// Invalid keyword instead of EXPANSION
	resp := r.BFReserve(cmd("BF.RESERVE", "bf", "0.01", "1000", "INVALID", "4"))
	assert.Equal(t, protocol.RespSyntaxError, resp)
}

func TestBFReserveBadExpansion(t *testing.T) {
	r := newTestRedis()

	resp := r.BFReserve(cmd("BF.RESERVE", "bf", "0.01", "1000", "EXPANSION", "invalid"))
	assert.Equal(t, protocol.RespBadExpansion, resp)
}

func TestBFReserveExpansionOutOfRange(t *testing.T) {
	r := newTestRedis()

	// Expansion < min (0)
	resp := r.BFReserve(cmd("BF.RESERVE", "bf", "0.01", "1000", "EXPANSION", "0"))
	assert.Equal(t, protocol.RespExpansionInvalidRange, resp)

	// Expansion > max (32769)
	resp = r.BFReserve(cmd("BF.RESERVE", "bf2", "0.01", "1000", "EXPANSION", "32769"))
	assert.Equal(t, protocol.RespExpansionInvalidRange, resp)
}

func TestBFWorkflow(t *testing.T) {
	r := newTestRedis()

	// Reserve a bloom filter
	resp := r.BFReserve(cmd("BF.RESERVE", "bf", "0.001", "10000"))
	assert.Equal(t, protocol.RespOK, resp)

	// Add multiple items
	resp = r.BFMAdd(cmd("BF.MADD", "bf", "apple", "banana", "cherry"))
	require.NotEmpty(t, resp)

	// Check cardinality
	resp = r.BFCard(cmd("BF.CARD", "bf"))
	assert.Equal(t, []byte(":3\r\n"), resp)

	// Check existence
	resp = r.BFMExists(cmd("BF.MEXISTS", "bf", "apple", "grape", "banana"))
	expected := "*3\r\n:1\r\n:0\r\n:1\r\n"
	assert.Equal(t, expected, string(resp))

	// Get info
	resp = r.BFInfo(cmd("BF.INFO", "bf"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestBFAutoCreate(t *testing.T) {
	r := newTestRedis()

	// BF.ADD should auto-create the bloom filter
	resp := r.BFAdd(cmd("BF.ADD", "bf", "item1"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	// Verify it was created
	resp = r.BFCard(cmd("BF.CARD", "bf"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	resp = r.BFExists(cmd("BF.EXISTS", "bf", "item1"))
	assert.Equal(t, []byte(":1\r\n"), resp)
}
