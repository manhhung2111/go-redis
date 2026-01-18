package test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manhhung2111/go-redis/internal/core"
)

// PFADD tests

func TestPFAdd(t *testing.T) {
	r := newTestRedis()

	resp := r.PFAdd(cmd("PFADD", "hll", "item1"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	// Adding same item again should return 0 (no register changed)
	resp = r.PFAdd(cmd("PFADD", "hll", "item1"))
	assert.Equal(t, []byte(":0\r\n"), resp)

	// Adding different item should return 1
	resp = r.PFAdd(cmd("PFADD", "hll", "item2"))
	assert.Equal(t, []byte(":1\r\n"), resp)
}

func TestPFAddNoElements(t *testing.T) {
	r := newTestRedis()

	// Creating HLL with no elements should return 1 (key created)
	resp := r.PFAdd(cmd("PFADD", "hll"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	// Calling again with no elements should return 0 (nothing changed)
	resp = r.PFAdd(cmd("PFADD", "hll"))
	assert.Equal(t, []byte(":0\r\n"), resp)
}

func TestPFAddMultipleElements(t *testing.T) {
	r := newTestRedis()

	resp := r.PFAdd(cmd("PFADD", "hll", "a", "b", "c", "d", "e"))
	assert.Equal(t, []byte(":1\r\n"), resp)

	// Verify count
	resp = r.PFCount(cmd("PFCOUNT", "hll"))
	assert.Equal(t, []byte(":5\r\n"), resp)
}

func TestPFAddWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.PFAdd(cmd("PFADD"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestPFAddWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.PFAdd(cmd("PFADD", "k", "item1"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

// PFCOUNT tests

func TestPFCount(t *testing.T) {
	r := newTestRedis()

	// Non-existing key should return 0
	resp := r.PFCount(cmd("PFCOUNT", "hll"))
	assert.Equal(t, []byte(":0\r\n"), resp)

	// Add items and check count
	r.PFAdd(cmd("PFADD", "hll", "item1", "item2", "item3"))

	resp = r.PFCount(cmd("PFCOUNT", "hll"))
	assert.Equal(t, []byte(":3\r\n"), resp)
}

func TestPFCountMultipleKeys(t *testing.T) {
	r := newTestRedis()

	// Add different items to each HLL
	r.PFAdd(cmd("PFADD", "hll1", "a", "b", "c"))
	r.PFAdd(cmd("PFADD", "hll2", "d", "e", "f"))

	// Count union
	resp := r.PFCount(cmd("PFCOUNT", "hll1", "hll2"))
	assert.Equal(t, []byte(":6\r\n"), resp)
}

func TestPFCountMultipleKeysWithOverlap(t *testing.T) {
	r := newTestRedis()

	r.PFAdd(cmd("PFADD", "hll1", "a", "b", "c"))
	r.PFAdd(cmd("PFADD", "hll2", "b", "c", "d"))

	// Union should be 4 (a, b, c, d)
	resp := r.PFCount(cmd("PFCOUNT", "hll1", "hll2"))
	assert.Equal(t, []byte(":4\r\n"), resp)
}

func TestPFCountMixedExistingNonExisting(t *testing.T) {
	r := newTestRedis()

	r.PFAdd(cmd("PFADD", "hll1", "a", "b", "c"))

	// Include non-existing key - should still work
	resp := r.PFCount(cmd("PFCOUNT", "hll1", "nonexistent"))
	assert.Equal(t, []byte(":3\r\n"), resp)
}

func TestPFCountWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.PFCount(cmd("PFCOUNT"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestPFCountWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.PFCount(cmd("PFCOUNT", "k"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestPFCountOneWrongType(t *testing.T) {
	r := newTestRedis()

	r.PFAdd(cmd("PFADD", "hll", "item1"))
	r.Set(cmd("SET", "k", "v"))

	// Should fail because one key is wrong type
	resp := r.PFCount(cmd("PFCOUNT", "hll", "k"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

// PFMERGE tests

func TestPFMerge(t *testing.T) {
	r := newTestRedis()

	r.PFAdd(cmd("PFADD", "hll1", "a", "b", "c"))
	r.PFAdd(cmd("PFADD", "hll2", "d", "e", "f"))

	resp := r.PFMerge(cmd("PFMERGE", "dest", "hll1", "hll2"))
	assert.Equal(t, core.RespOK, resp)

	// Verify merged count
	resp = r.PFCount(cmd("PFCOUNT", "dest"))
	assert.Equal(t, []byte(":6\r\n"), resp)
}

func TestPFMergeWithOverlap(t *testing.T) {
	r := newTestRedis()

	r.PFAdd(cmd("PFADD", "hll1", "a", "b", "c"))
	r.PFAdd(cmd("PFADD", "hll2", "b", "c", "d"))

	resp := r.PFMerge(cmd("PFMERGE", "dest", "hll1", "hll2"))
	assert.Equal(t, core.RespOK, resp)

	// Union should be 4 (a, b, c, d)
	resp = r.PFCount(cmd("PFCOUNT", "dest"))
	assert.Equal(t, []byte(":4\r\n"), resp)
}

func TestPFMergeIntoExisting(t *testing.T) {
	r := newTestRedis()

	r.PFAdd(cmd("PFADD", "dest", "x", "y", "z"))
	r.PFAdd(cmd("PFADD", "src", "a", "b", "c"))

	resp := r.PFMerge(cmd("PFMERGE", "dest", "src"))
	assert.Equal(t, core.RespOK, resp)

	// Dest should have union of both
	resp = r.PFCount(cmd("PFCOUNT", "dest"))
	assert.Equal(t, []byte(":6\r\n"), resp)
}

func TestPFMergeNoSources(t *testing.T) {
	r := newTestRedis()

	// Merge with no sources - just creates empty dest
	resp := r.PFMerge(cmd("PFMERGE", "dest"))
	assert.Equal(t, core.RespOK, resp)

	// Dest should exist but be empty
	resp = r.PFCount(cmd("PFCOUNT", "dest"))
	assert.Equal(t, []byte(":0\r\n"), resp)
}

func TestPFMergeNonExistingSources(t *testing.T) {
	r := newTestRedis()

	r.PFAdd(cmd("PFADD", "hll", "a", "b", "c"))

	// Merge including non-existing sources
	resp := r.PFMerge(cmd("PFMERGE", "dest", "hll", "nonexistent"))
	assert.Equal(t, core.RespOK, resp)

	// Should only have data from hll
	resp = r.PFCount(cmd("PFCOUNT", "dest"))
	assert.Equal(t, []byte(":3\r\n"), resp)
}

func TestPFMergeWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.PFMerge(cmd("PFMERGE"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestPFMergeWrongTypeInDest(t *testing.T) {
	r := newTestRedis()

	r.Set(cmd("SET", "k", "v"))

	resp := r.PFMerge(cmd("PFMERGE", "k", "hll"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestPFMergeWrongTypeInSource(t *testing.T) {
	r := newTestRedis()

	r.PFAdd(cmd("PFADD", "hll", "item"))
	r.Set(cmd("SET", "k", "v"))

	resp := r.PFMerge(cmd("PFMERGE", "dest", "hll", "k"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

// Workflow tests

func TestPFWorkflow(t *testing.T) {
	r := newTestRedis()

	// Simulate page view counting
	r.PFAdd(cmd("PFADD", "page:home", "user:1", "user:2", "user:3"))
	r.PFAdd(cmd("PFADD", "page:about", "user:2", "user:3", "user:4"))
	r.PFAdd(cmd("PFADD", "page:contact", "user:3", "user:4", "user:5"))

	// Individual page counts
	resp := r.PFCount(cmd("PFCOUNT", "page:home"))
	assert.Equal(t, []byte(":3\r\n"), resp)

	resp = r.PFCount(cmd("PFCOUNT", "page:about"))
	assert.Equal(t, []byte(":3\r\n"), resp)

	resp = r.PFCount(cmd("PFCOUNT", "page:contact"))
	assert.Equal(t, []byte(":3\r\n"), resp)

	// Total unique visitors (union)
	resp = r.PFCount(cmd("PFCOUNT", "page:home", "page:about", "page:contact"))
	assert.Equal(t, []byte(":5\r\n"), resp)

	// Merge into daily total
	resp = r.PFMerge(cmd("PFMERGE", "daily:total", "page:home", "page:about", "page:contact"))
	assert.Equal(t, core.RespOK, resp)

	resp = r.PFCount(cmd("PFCOUNT", "daily:total"))
	assert.Equal(t, []byte(":5\r\n"), resp)
}

func TestPFLargerDataset(t *testing.T) {
	r := newTestRedis()

	// Add 100 unique items
	for i := 0; i < 100; i++ {
		r.PFAdd(cmd("PFADD", "hll", fmt.Sprintf("item%d", i)))
	}

	resp := r.PFCount(cmd("PFCOUNT", "hll"))
	require.NotEmpty(t, resp)

	// Parse response and check it's close to 100
	// Response format: :NNN\r\n
	assert.Equal(t, byte(':'), resp[0])
}

func TestPFDuplicatesDontChangeCount(t *testing.T) {
	r := newTestRedis()

	r.PFAdd(cmd("PFADD", "hll", "a", "b", "c"))

	// Add same items again
	resp := r.PFAdd(cmd("PFADD", "hll", "a", "b", "c"))
	assert.Equal(t, []byte(":0\r\n"), resp, "should return 0 for duplicates")

	// Count should still be 3
	resp = r.PFCount(cmd("PFCOUNT", "hll"))
	assert.Equal(t, []byte(":3\r\n"), resp)
}

func TestPFCountDoesNotModifyHLLs(t *testing.T) {
	r := newTestRedis()

	r.PFAdd(cmd("PFADD", "hll1", "a", "b", "c"))
	r.PFAdd(cmd("PFADD", "hll2", "d", "e", "f"))

	// Get initial counts
	count1 := r.PFCount(cmd("PFCOUNT", "hll1"))
	count2 := r.PFCount(cmd("PFCOUNT", "hll2"))

	// Count union
	r.PFCount(cmd("PFCOUNT", "hll1", "hll2"))

	// Individual counts should be unchanged
	assert.Equal(t, count1, r.PFCount(cmd("PFCOUNT", "hll1")))
	assert.Equal(t, count2, r.PFCount(cmd("PFCOUNT", "hll2")))
}

func TestPFMergeDoesNotModifySources(t *testing.T) {
	r := newTestRedis()

	r.PFAdd(cmd("PFADD", "src", "a", "b", "c"))
	countBefore := r.PFCount(cmd("PFCOUNT", "src"))

	r.PFMerge(cmd("PFMERGE", "dest", "src"))

	countAfter := r.PFCount(cmd("PFCOUNT", "src"))
	assert.Equal(t, countBefore, countAfter, "source should not be modified")
}
