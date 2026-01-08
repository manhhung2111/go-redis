package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manhhung2111/go-redis/internal/constant"
)

func TestCFAdd(t *testing.T) {
	r := newTestRedis()

	resp := r.CFAdd(cmd("CF.ADD", "cf", "item1"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	// Adding same item again should still return 1 (cuckoo filter allows duplicates)
	resp = r.CFAdd(cmd("CF.ADD", "cf", "item1"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	// Adding different item should return 1
	resp = r.CFAdd(cmd("CF.ADD", "cf", "item2"))
	assert.Equal(t, []byte(":1\r\n"), resp)
}

func TestCFAddWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.CFAdd(cmd("CF.ADD", "cf"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	resp = r.CFAdd(cmd("CF.ADD", "cf", "item1", "item2"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestCFAddWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.CFAdd(cmd("CF.ADD", "k", "item1"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestCFAddNx(t *testing.T) {
	r := newTestRedis()

	// First add should succeed
	resp := r.CFAddNx(cmd("CF.ADDNX", "cf", "item1"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	// Adding same item again should return 0 (item exists)
	resp = r.CFAddNx(cmd("CF.ADDNX", "cf", "item1"))
	assert.Equal(t, []byte(":0\r\n"), resp)

	// Adding different item should return 1
	resp = r.CFAddNx(cmd("CF.ADDNX", "cf", "item2"))
	assert.Equal(t, []byte(":1\r\n"), resp)
}

func TestCFAddNxWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.CFAddNx(cmd("CF.ADDNX", "cf"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	resp = r.CFAddNx(cmd("CF.ADDNX", "cf", "item1", "item2"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestCFAddNxWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.CFAddNx(cmd("CF.ADDNX", "k", "item1"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestCFCount(t *testing.T) {
	r := newTestRedis()

	// Non-existing key should return 0
	resp := r.CFCount(cmd("CF.COUNT", "cf", "item1"))
	assert.Equal(t, []byte(":0\r\n"), resp)

	// Add item and check count
	r.CFAdd(cmd("CF.ADD", "cf", "item1"))
	resp = r.CFCount(cmd("CF.COUNT", "cf", "item1"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	// Add same item again (cuckoo filter counts duplicates)
	r.CFAdd(cmd("CF.ADD", "cf", "item1"))
	resp = r.CFCount(cmd("CF.COUNT", "cf", "item1"))
	assert.Equal(t, []byte(":2\r\n"), resp)

	// Non-existing item should return 0
	resp = r.CFCount(cmd("CF.COUNT", "cf", "item2"))
	assert.Equal(t, []byte(":0\r\n"), resp)
}

func TestCFCountWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.CFCount(cmd("CF.COUNT", "cf"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	resp = r.CFCount(cmd("CF.COUNT", "cf", "item1", "item2"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestCFCountWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.CFCount(cmd("CF.COUNT", "k", "item1"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestCFDel(t *testing.T) {
	r := newTestRedis()

	// Add items
	r.CFAdd(cmd("CF.ADD", "cf", "item1"))
	r.CFAdd(cmd("CF.ADD", "cf", "item1")) // Add duplicate

	// Delete should return 1
	resp := r.CFDel(cmd("CF.DEL", "cf", "item1"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	// Item should still exist (one copy remains)
	resp = r.CFCount(cmd("CF.COUNT", "cf", "item1"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	// Delete again
	resp = r.CFDel(cmd("CF.DEL", "cf", "item1"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	// Item should no longer exist
	resp = r.CFCount(cmd("CF.COUNT", "cf", "item1"))
	assert.Equal(t, []byte(":0\r\n"), resp)

	// Deleting non-existing item should return 0
	resp = r.CFDel(cmd("CF.DEL", "cf", "item1"))
	assert.Equal(t, []byte(":0\r\n"), resp)
}

func TestCFDelNonExistingKey(t *testing.T) {
	r := newTestRedis()

	resp := r.CFDel(cmd("CF.DEL", "cf", "item1"))
	assert.Equal(t, constant.RESP_NOT_FOUND, resp)
}

func TestCFDelWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.CFDel(cmd("CF.DEL", "cf"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	resp = r.CFDel(cmd("CF.DEL", "cf", "item1", "item2"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestCFDelWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.CFDel(cmd("CF.DEL", "k", "item1"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestCFExists(t *testing.T) {
	r := newTestRedis()

	// Non-existing key should return 0
	resp := r.CFExists(cmd("CF.EXISTS", "cf", "item1"))
	assert.Equal(t, []byte(":0\r\n"), resp)

	// Add item and check existence
	r.CFAdd(cmd("CF.ADD", "cf", "item1"))

	resp = r.CFExists(cmd("CF.EXISTS", "cf", "item1"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	resp = r.CFExists(cmd("CF.EXISTS", "cf", "item2"))
	assert.Equal(t, []byte(":0\r\n"), resp)
}

func TestCFExistsWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.CFExists(cmd("CF.EXISTS", "cf"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	resp = r.CFExists(cmd("CF.EXISTS", "cf", "item1", "item2"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestCFExistsWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.CFExists(cmd("CF.EXISTS", "k", "item1"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestCFInfo(t *testing.T) {
	r := newTestRedis()

	// Create a cuckoo filter with CF.ADD
	r.CFAdd(cmd("CF.ADD", "cf", "item1"))

	// Get all info
	resp := r.CFInfo(cmd("CF.INFO", "cf"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestCFInfoNonExistingKey(t *testing.T) {
	r := newTestRedis()

	resp := r.CFInfo(cmd("CF.INFO", "cf"))
	assert.Equal(t, constant.RESP_NOT_FOUND, resp)
}

func TestCFInfoWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.CFInfo(cmd("CF.INFO"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	resp = r.CFInfo(cmd("CF.INFO", "cf", "extra"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestCFInfoWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.CFInfo(cmd("CF.INFO", "k"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestCFMExists(t *testing.T) {
	r := newTestRedis()

	r.CFAdd(cmd("CF.ADD", "cf", "item1"))
	r.CFAdd(cmd("CF.ADD", "cf", "item3"))

	resp := r.CFMExists(cmd("CF.MEXISTS", "cf", "item1", "item2", "item3"))
	expected := "*3\r\n:1\r\n:0\r\n:1\r\n"
	assert.Equal(t, expected, string(resp))
}

func TestCFMExistsNonExisting(t *testing.T) {
	r := newTestRedis()

	resp := r.CFMExists(cmd("CF.MEXISTS", "cf", "item1", "item2"))
	expected := "*2\r\n:0\r\n:0\r\n"
	assert.Equal(t, expected, string(resp))
}

func TestCFMExistsWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.CFMExists(cmd("CF.MEXISTS", "cf"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestCFMExistsWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.CFMExists(cmd("CF.MEXISTS", "k", "item1"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestCFReserve(t *testing.T) {
	r := newTestRedis()

	resp := r.CFReserve(cmd("CF.RESERVE", "cf", "1000"))
	assert.Equal(t, constant.RESP_OK, resp)

	// Verify cuckoo filter was created by adding an item
	resp = r.CFAdd(cmd("CF.ADD", "cf", "item1"))
	assert.Equal(t, []byte(":1\r\n"), resp)
}

func TestCFReserveWithBucketSize(t *testing.T) {
	r := newTestRedis()

	resp := r.CFReserve(cmd("CF.RESERVE", "cf", "1000", "BUCKETSIZE", "2"))
	assert.Equal(t, constant.RESP_OK, resp)
}

func TestCFReserveWithMaxIterations(t *testing.T) {
	r := newTestRedis()

	resp := r.CFReserve(cmd("CF.RESERVE", "cf", "1000", "MAXITERATIONS", "100"))
	assert.Equal(t, constant.RESP_OK, resp)
}

func TestCFReserveWithExpansion(t *testing.T) {
	r := newTestRedis()

	resp := r.CFReserve(cmd("CF.RESERVE", "cf", "1000", "EXPANSION", "2"))
	assert.Equal(t, constant.RESP_OK, resp)
}

func TestCFReserveWithAllOptions(t *testing.T) {
	r := newTestRedis()

	resp := r.CFReserve(cmd("CF.RESERVE", "cf", "1000", "BUCKETSIZE", "2", "MAXITERATIONS", "100", "EXPANSION", "2"))
	assert.Equal(t, constant.RESP_OK, resp)
}

func TestCFReserveCaseInsensitive(t *testing.T) {
	r := newTestRedis()

	resp := r.CFReserve(cmd("CF.RESERVE", "cf", "1000", "bucketsize", "2", "maxiterations", "100", "expansion", "2"))
	assert.Equal(t, constant.RESP_OK, resp)
}

func TestCFReserveWrongArgs(t *testing.T) {
	r := newTestRedis()

	// Too few args
	resp := r.CFReserve(cmd("CF.RESERVE", "cf"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestCFReserveWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.CFReserve(cmd("CF.RESERVE", "k", "1000"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestCFReserveItemExists(t *testing.T) {
	r := newTestRedis()

	// Create cuckoo filter first
	r.CFReserve(cmd("CF.RESERVE", "cf", "1000"))

	// Try to reserve again
	resp := r.CFReserve(cmd("CF.RESERVE", "cf", "1000"))
	assert.Equal(t, constant.RESP_ITEM_EXISTS, resp)
}

func TestCFReserveBadCapacity(t *testing.T) {
	r := newTestRedis()

	resp := r.CFReserve(cmd("CF.RESERVE", "cf", "invalid"))
	assert.Equal(t, constant.RESP_BAD_CAPACITY, resp)
}

func TestCFReserveCapacityOutOfRange(t *testing.T) {
	r := newTestRedis()

	// Capacity < 1
	resp := r.CFReserve(cmd("CF.RESERVE", "cf", "0"))
	assert.Equal(t, constant.RESP_CAPACITY_INVALID_RANGE, resp)

	// Capacity > max (1048577)
	resp = r.CFReserve(cmd("CF.RESERVE", "cf2", "1048577"))
	assert.Equal(t, constant.RESP_CAPACITY_INVALID_RANGE, resp)
}

func TestCFReserveBadBucketSize(t *testing.T) {
	r := newTestRedis()

	resp := r.CFReserve(cmd("CF.RESERVE", "cf", "1000", "BUCKETSIZE", "invalid"))
	assert.Equal(t, constant.RESP_BAD_BUCKET_SIZE, resp)
}

func TestCFReserveBucketSizeOutOfRange(t *testing.T) {
	r := newTestRedis()

	// Bucket size < 1
	resp := r.CFReserve(cmd("CF.RESERVE", "cf", "1000", "BUCKETSIZE", "0"))
	assert.Equal(t, constant.RESP_BUCKET_SIZE_INVALID_RANGE, resp)

	// Bucket size > 255
	resp = r.CFReserve(cmd("CF.RESERVE", "cf2", "1000", "BUCKETSIZE", "256"))
	assert.Equal(t, constant.RESP_BUCKET_SIZE_INVALID_RANGE, resp)
}

func TestCFReserveBadMaxIterations(t *testing.T) {
	r := newTestRedis()

	resp := r.CFReserve(cmd("CF.RESERVE", "cf", "1000", "MAXITERATIONS", "invalid"))
	assert.Equal(t, constant.RESP_BAD_MAX_ITERATIONS, resp)
}

func TestCFReserveMaxIterationsOutOfRange(t *testing.T) {
	r := newTestRedis()

	// Max iterations < 1
	resp := r.CFReserve(cmd("CF.RESERVE", "cf", "1000", "MAXITERATIONS", "0"))
	assert.Equal(t, constant.RESP_MAX_ITERATIONS_INVALID_RANGE, resp)

	// Max iterations > 65535
	resp = r.CFReserve(cmd("CF.RESERVE", "cf2", "1000", "MAXITERATIONS", "65536"))
	assert.Equal(t, constant.RESP_MAX_ITERATIONS_INVALID_RANGE, resp)
}

func TestCFReserveBadExpansion(t *testing.T) {
	r := newTestRedis()

	resp := r.CFReserve(cmd("CF.RESERVE", "cf", "1000", "EXPANSION", "invalid"))
	assert.Equal(t, constant.RESP_BAD_EXPANSION, resp)
}

func TestCFReserveExpansionOutOfRange(t *testing.T) {
	r := newTestRedis()

	// Expansion < 0
	resp := r.CFReserve(cmd("CF.RESERVE", "cf", "1000", "EXPANSION", "-1"))
	assert.Equal(t, constant.RESP_EXPANSION_INVALID_RANGE, resp)

	// Expansion > 32768
	resp = r.CFReserve(cmd("CF.RESERVE", "cf2", "1000", "EXPANSION", "32769"))
	assert.Equal(t, constant.RESP_EXPANSION_INVALID_RANGE, resp)
}

func TestCFReserveSyntaxError(t *testing.T) {
	r := newTestRedis()

	// Invalid keyword
	resp := r.CFReserve(cmd("CF.RESERVE", "cf", "1000", "INVALID", "4"))
	assert.Equal(t, constant.RESP_SYNTAX_ERROR, resp)
}

func TestCFReserveMissingOptionValue(t *testing.T) {
	r := newTestRedis()

	// Missing BUCKETSIZE value
	resp := r.CFReserve(cmd("CF.RESERVE", "cf", "1000", "BUCKETSIZE"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	// Missing MAXITERATIONS value
	resp = r.CFReserve(cmd("CF.RESERVE", "cf2", "1000", "MAXITERATIONS"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	// Missing EXPANSION value
	resp = r.CFReserve(cmd("CF.RESERVE", "cf3", "1000", "EXPANSION"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestCFWorkflow(t *testing.T) {
	r := newTestRedis()

	// Reserve a cuckoo filter
	resp := r.CFReserve(cmd("CF.RESERVE", "cf", "10000"))
	assert.Equal(t, constant.RESP_OK, resp)

	// Add multiple items
	r.CFAdd(cmd("CF.ADD", "cf", "apple"))
	r.CFAdd(cmd("CF.ADD", "cf", "banana"))
	r.CFAdd(cmd("CF.ADD", "cf", "cherry"))

	// Check existence
	resp = r.CFMExists(cmd("CF.MEXISTS", "cf", "apple", "grape", "banana"))
	expected := "*3\r\n:1\r\n:0\r\n:1\r\n"
	assert.Equal(t, expected, string(resp))

	// Get info
	resp = r.CFInfo(cmd("CF.INFO", "cf"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])

	// Delete an item
	resp = r.CFDel(cmd("CF.DEL", "cf", "apple"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	// Verify deletion
	resp = r.CFExists(cmd("CF.EXISTS", "cf", "apple"))
	assert.Equal(t, []byte(":0\r\n"), resp)
}

func TestCFAutoCreate(t *testing.T) {
	r := newTestRedis()

	// CF.ADD should auto-create the cuckoo filter
	resp := r.CFAdd(cmd("CF.ADD", "cf", "item1"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	// Verify it was created
	resp = r.CFExists(cmd("CF.EXISTS", "cf", "item1"))
	assert.Equal(t, []byte(":1\r\n"), resp)
}

func TestCFAddNxWorkflow(t *testing.T) {
	r := newTestRedis()

	// Add item with ADDNX
	resp := r.CFAddNx(cmd("CF.ADDNX", "cf", "item1"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	// Try to add same item again
	resp = r.CFAddNx(cmd("CF.ADDNX", "cf", "item1"))
	assert.Equal(t, []byte(":0\r\n"), resp)

	// Count should still be 1
	resp = r.CFCount(cmd("CF.COUNT", "cf", "item1"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	// Regular add should work
	resp = r.CFAdd(cmd("CF.ADD", "cf", "item1"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	// Count should now be 2
	resp = r.CFCount(cmd("CF.COUNT", "cf", "item1"))
	assert.Equal(t, []byte(":2\r\n"), resp)
}

func TestCFDelAndCount(t *testing.T) {
	r := newTestRedis()

	// Add same item multiple times
	r.CFAdd(cmd("CF.ADD", "cf", "item1"))
	r.CFAdd(cmd("CF.ADD", "cf", "item1"))
	r.CFAdd(cmd("CF.ADD", "cf", "item1"))

	// Count should be 3
	resp := r.CFCount(cmd("CF.COUNT", "cf", "item1"))
	assert.Equal(t, []byte(":3\r\n"), resp)

	// Delete one occurrence
	resp = r.CFDel(cmd("CF.DEL", "cf", "item1"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	// Count should be 2
	resp = r.CFCount(cmd("CF.COUNT", "cf", "item1"))
	assert.Equal(t, []byte(":2\r\n"), resp)

	// Delete remaining occurrences
	r.CFDel(cmd("CF.DEL", "cf", "item1"))
	r.CFDel(cmd("CF.DEL", "cf", "item1"))

	// Count should be 0
	resp = r.CFCount(cmd("CF.COUNT", "cf", "item1"))
	assert.Equal(t, []byte(":0\r\n"), resp)

	// Exists should return 0
	resp = r.CFExists(cmd("CF.EXISTS", "cf", "item1"))
	assert.Equal(t, []byte(":0\r\n"), resp)
}
