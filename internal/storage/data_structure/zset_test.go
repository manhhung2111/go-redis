package data_structure

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZSet_ZAdd_BasicInsert(t *testing.T) {
	z := NewZSet()

	res := z.ZAdd(map[float64]string{
		1: "one",
		2: "two",
	}, ZAddOptions{})

	require.NotNil(t, res)
	assert.Equal(t, uint32(2), *res)
	assert.Equal(t, uint32(2), z.ZCard())
}

func TestZSet_ZAdd_NX(t *testing.T) {
	z := NewZSet()

	z.ZAdd(map[float64]string{1: "a"}, ZAddOptions{})

	res := z.ZAdd(map[float64]string{
		2: "a", // should be ignored
		3: "b",
	}, ZAddOptions{NX: true})

	require.NotNil(t, res)
	assert.Equal(t, uint32(1), *res)
	assert.Equal(t, uint32(2), z.ZCard())
	assert.Equal(t, float64(1), z.data["a"])
}

func TestZSet_ZAdd_XX(t *testing.T) {
	z := NewZSet()

	z.ZAdd(map[float64]string{1: "a"}, ZAddOptions{})

	// update without CH → result must be 0
	res := z.ZAdd(map[float64]string{
		2: "a",
	}, ZAddOptions{XX: true})

	require.NotNil(t, res)
	assert.Equal(t, uint32(0), *res)
	assert.Equal(t, uint32(1), z.ZCard())
	assert.Equal(t, float64(2), z.data["a"])

	// update with CH → result increments
	res = z.ZAdd(map[float64]string{
		3: "a",
	}, ZAddOptions{XX: true, CH: true})

	require.NotNil(t, res)
	assert.Equal(t, uint32(1), *res)
	assert.Equal(t, float64(3), z.data["a"])
}

func TestZSet_ZAdd_GT_LT(t *testing.T) {
	z := NewZSet()
	z.ZAdd(map[float64]string{5: "a"}, ZAddOptions{})

	// GT reject
	res := z.ZAdd(map[float64]string{3: "a"}, ZAddOptions{GT: true})
	require.NotNil(t, res)
	assert.Equal(t, uint32(0), *res)
	assert.Equal(t, float64(5), z.data["a"])

	// GT accept, no CH → 0
	res = z.ZAdd(map[float64]string{7: "a"}, ZAddOptions{GT: true})
	require.NotNil(t, res)
	assert.Equal(t, uint32(0), *res)
	assert.Equal(t, float64(7), z.data["a"])

	// GT accept with CH → 1
	res = z.ZAdd(map[float64]string{9: "a"}, ZAddOptions{GT: true, CH: true})
	require.NotNil(t, res)
	assert.Equal(t, uint32(1), *res)
	assert.Equal(t, float64(9), z.data["a"])

	// LT reject
	res = z.ZAdd(map[float64]string{10: "a"}, ZAddOptions{LT: true})
	require.NotNil(t, res)
	assert.Equal(t, uint32(0), *res)

	// LT accept with CH
	res = z.ZAdd(map[float64]string{8: "a"}, ZAddOptions{LT: true, CH: true})
	require.NotNil(t, res)
	assert.Equal(t, uint32(1), *res)
	assert.Equal(t, float64(8), z.data["a"])
}

func TestZSet_ZAdd_CH(t *testing.T) {
	z := NewZSet()

	res := z.ZAdd(map[float64]string{1: "a"}, ZAddOptions{CH: true})
	require.NotNil(t, res)
	assert.Equal(t, uint32(1), *res)

	// same score → no change
	res = z.ZAdd(map[float64]string{1: "a"}, ZAddOptions{CH: true})
	require.NotNil(t, res)
	assert.Equal(t, uint32(0), *res)

	// score change
	res = z.ZAdd(map[float64]string{2: "a"}, ZAddOptions{CH: true})
	require.NotNil(t, res)
	assert.Equal(t, uint32(1), *res)
}

func TestZSet_ZAdd_InvalidOptions(t *testing.T) {
	z := NewZSet()

	assert.Nil(t, z.ZAdd(
		map[float64]string{1: "a"},
		ZAddOptions{NX: true, XX: true},
	))

	assert.Nil(t, z.ZAdd(
		map[float64]string{1: "a"},
		ZAddOptions{GT: true, LT: true},
	))

	assert.Nil(t, z.ZAdd(
		map[float64]string{1: "a"},
		ZAddOptions{NX: true, GT: true},
	))
}

func TestZSet_ZCard_AfterUpdates(t *testing.T) {
	z := NewZSet()

	z.ZAdd(map[float64]string{
		1: "a",
		2: "b",
		3: "c",
	}, ZAddOptions{})

	assert.Equal(t, uint32(3), z.ZCard())

	z.ZAdd(map[float64]string{5: "b"}, ZAddOptions{})
	assert.Equal(t, uint32(3), z.ZCard())

	z.ZAdd(map[float64]string{4: "d"}, ZAddOptions{})
	assert.Equal(t, uint32(4), z.ZCard())
}
