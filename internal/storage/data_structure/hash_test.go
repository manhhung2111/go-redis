package data_structure

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewHash(t *testing.T) {
	h := NewHash()
	require.NotNil(t, h)
	assert.Equal(t, int64(0), h.Size())
}

func TestHashSetSingle(t *testing.T) {
	h := NewHash()

	added := h.Set(map[string]string{"key1": "value1"})
	assert.Equal(t, int64(1), added)
	assert.Equal(t, int64(1), h.Size())
}

func TestHashSetMultiple(t *testing.T) {
	h := NewHash()

	added := h.Set(map[string]string{
		"name":  "Alice",
		"age":   "30",
		"city":  "NYC",
		"email": "alice@example.com",
	})
	assert.Equal(t, int64(4), added)
	assert.Equal(t, int64(4), h.Size())
}

func TestHashSetUpdate(t *testing.T) {
	h := NewHash()

	// Initial set
	added := h.Set(map[string]string{"key1": "value1", "key2": "value2"})
	assert.Equal(t, int64(2), added)

	// Update existing and add new
	added = h.Set(map[string]string{"key1": "newvalue1", "key3": "value3"})
	assert.Equal(t, int64(1), added, "only key3 should be counted as new")
	assert.Equal(t, int64(3), h.Size())

	// Verify update worked
	val, ok := h.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "newvalue1", val)
}

func TestHashSetEmpty(t *testing.T) {
	h := NewHash()

	added := h.Set(map[string]string{})
	assert.Equal(t, int64(0), added)
	assert.Equal(t, int64(0), h.Size())
}

func TestHashGet(t *testing.T) {
	h := NewHash()
	h.Set(map[string]string{"key1": "value1", "key2": "value2"})

	// Get existing key
	val, ok := h.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", val)

	// Get non-existing key
	val, ok = h.Get("nonexistent")
	assert.False(t, ok)
	assert.Empty(t, val)
}

func TestHashGetEmpty(t *testing.T) {
	h := NewHash()

	val, ok := h.Get("anykey")
	assert.False(t, ok)
	assert.Empty(t, val)
}

func TestHashMGet(t *testing.T) {
	h := NewHash()
	h.Set(map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"})

	results := h.MGet("key1", "nonexistent", "key2", "missing", "key3")
	require.Len(t, results, 5)

	assert.NotNil(t, results[0])
	assert.Equal(t, "value1", *results[0])

	assert.Nil(t, results[1])

	assert.NotNil(t, results[2])
	assert.Equal(t, "value2", *results[2])

	assert.Nil(t, results[3])

	assert.NotNil(t, results[4])
	assert.Equal(t, "value3", *results[4])
}

func TestHashMGetEmpty(t *testing.T) {
	h := NewHash()

	results := h.MGet("key1", "key2")
	require.Len(t, results, 2)
	assert.Nil(t, results[0])
	assert.Nil(t, results[1])
}

func TestHashMGetNoKeys(t *testing.T) {
	h := NewHash()
	h.Set(map[string]string{"key1": "value1"})

	results := h.MGet()
	assert.Empty(t, results)
}

func TestHashMGetAllExist(t *testing.T) {
	h := NewHash()
	h.Set(map[string]string{"a": "1", "b": "2", "c": "3"})

	results := h.MGet("a", "b", "c")
	require.Len(t, results, 3)
	assert.Equal(t, "1", *results[0])
	assert.Equal(t, "2", *results[1])
	assert.Equal(t, "3", *results[2])
}

func TestHashMGetAllMissing(t *testing.T) {
	h := NewHash()

	results := h.MGet("x", "y", "z")
	require.Len(t, results, 3)
	assert.Nil(t, results[0])
	assert.Nil(t, results[1])
	assert.Nil(t, results[2])
}

func TestHashGetAll(t *testing.T) {
	h := NewHash()
	h.Set(map[string]string{"key1": "value1", "key2": "value2"})

	result := h.GetAll()
	assert.Len(t, result, 4)

	// Convert to map for easier testing (order doesn't matter)
	m := make(map[string]string)
	for i := 0; i < len(result); i += 2 {
		m[result[i]] = result[i+1]
	}

	assert.Equal(t, "value1", m["key1"])
	assert.Equal(t, "value2", m["key2"])
}

func TestHashGetAllEmpty(t *testing.T) {
	h := NewHash()

	result := h.GetAll()
	assert.Empty(t, result)
}

func TestHashGetKeys(t *testing.T) {
	h := NewHash()
	h.Set(map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"})

	keys := h.GetKeys()
	assert.Len(t, keys, 3)
	assert.ElementsMatch(t, []string{"key1", "key2", "key3"}, keys)
}

func TestHashGetKeysEmpty(t *testing.T) {
	h := NewHash()

	keys := h.GetKeys()
	assert.Empty(t, keys)
}

func TestHashGetValues(t *testing.T) {
	h := NewHash()
	h.Set(map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"})

	values := h.GetValues()
	assert.Len(t, values, 3)
	assert.ElementsMatch(t, []string{"value1", "value2", "value3"}, values)
}

func TestHashGetValuesEmpty(t *testing.T) {
	h := NewHash()

	values := h.GetValues()
	assert.Empty(t, values)
}

func TestHashExists(t *testing.T) {
	h := NewHash()
	h.Set(map[string]string{"key1": "value1"})

	assert.True(t, h.Exists("key1"))
	assert.False(t, h.Exists("nonexistent"))
}

func TestHashExistsEmpty(t *testing.T) {
	h := NewHash()

	assert.False(t, h.Exists("anykey"))
}

func TestHashSetNX(t *testing.T) {
	h := NewHash()

	// Set new field
	set := h.SetNX("key1", "value1")
	assert.True(t, set)
	assert.Equal(t, int64(1), h.Size())

	val, ok := h.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", val)

	// Try to set existing field
	set = h.SetNX("key1", "value2")
	assert.False(t, set)

	// Verify value didn't change
	val, ok = h.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, "value1", val)
}

func TestHashSetNXMultiple(t *testing.T) {
	h := NewHash()

	assert.True(t, h.SetNX("key1", "value1"))
	assert.True(t, h.SetNX("key2", "value2"))
	assert.False(t, h.SetNX("key1", "newvalue"))
	assert.Equal(t, int64(2), h.Size())
}

func TestHashDelete(t *testing.T) {
	h := NewHash()
	h.Set(map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"})

	// Delete existing key
	deleted := h.Delete("key1")
	assert.Equal(t, int64(1), deleted)
	assert.Equal(t, int64(2), h.Size())
	assert.False(t, h.Exists("key1"))

	// Delete non-existing key
	deleted = h.Delete("nonexistent")
	assert.Equal(t, int64(0), deleted)
	assert.Equal(t, int64(2), h.Size())
}

func TestHashDeleteMultiple(t *testing.T) {
	h := NewHash()
	h.Set(map[string]string{"key1": "value1", "key2": "value2", "key3": "value3", "key4": "value4"})

	deleted := h.Delete("key1", "key3")
	assert.Equal(t, int64(2), deleted)
	assert.Equal(t, int64(2), h.Size())
	assert.False(t, h.Exists("key1"))
	assert.False(t, h.Exists("key3"))
	assert.True(t, h.Exists("key2"))
	assert.True(t, h.Exists("key4"))
}

func TestHashDeleteMixed(t *testing.T) {
	h := NewHash()
	h.Set(map[string]string{"key1": "value1", "key2": "value2"})

	// Mix of existing and non-existing
	deleted := h.Delete("key1", "nonexistent", "key2", "missing")
	assert.Equal(t, int64(2), deleted)
	assert.Equal(t, int64(0), h.Size())
}

func TestHashDeleteEmpty(t *testing.T) {
	h := NewHash()
	h.Set(map[string]string{"key1": "value1"})

	deleted := h.Delete()
	assert.Equal(t, int64(0), deleted)
	assert.Equal(t, int64(1), h.Size())
}

func TestHashDeleteFromEmpty(t *testing.T) {
	h := NewHash()

	deleted := h.Delete("key1", "key2")
	assert.Equal(t, int64(0), deleted)
}

func TestHashIncBy(t *testing.T) {
	h := NewHash()

	// Increment non-existing field (should initialize to increment)
	val, err := h.IncBy("counter", 5)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), val)

	// Increment existing field
	val, err = h.IncBy("counter", 3)
	assert.NoError(t, err)
	assert.Equal(t, int64(8), val)

	// Decrement
	val, err = h.IncBy("counter", -10)
	assert.NoError(t, err)
	assert.Equal(t, int64(-2), val)
}

func TestHashIncByNegativeNumbers(t *testing.T) {
	h := NewHash()

	// Start with negative
	val, err := h.IncBy("counter", -10)
	assert.NoError(t, err)
	assert.Equal(t, int64(-10), val)

	// Increment by negative
	val, err = h.IncBy("counter", -5)
	assert.NoError(t, err)
	assert.Equal(t, int64(-15), val)

	// Increment by positive
	val, err = h.IncBy("counter", 20)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), val)
}

func TestHashIncByZero(t *testing.T) {
	h := NewHash()
	h.Set(map[string]string{"counter": "10"})

	val, err := h.IncBy("counter", 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), val)
}

func TestHashIncByNonInteger(t *testing.T) {
	h := NewHash()
	h.Set(map[string]string{"name": "Alice"})

	val, err := h.IncBy("name", 5)
	assert.Error(t, err)
	assert.Equal(t, int64(0), val)
	assert.Contains(t, err.Error(), "not an integer")
}

func TestHashIncByOverflow(t *testing.T) {
	h := NewHash()
	h.Set(map[string]string{"counter": "9223372036854775807"}) // math.MaxInt64

	// Should overflow
	val, err := h.IncBy("counter", 1)
	assert.Error(t, err)
	assert.Equal(t, int64(0), val)
	assert.Contains(t, err.Error(), "out of range")
}

func TestHashIncByUnderflow(t *testing.T) {
	h := NewHash()
	h.Set(map[string]string{"counter": "-9223372036854775808"}) // math.MinInt64

	// Should underflow
	val, err := h.IncBy("counter", -1)
	assert.Error(t, err)
	assert.Equal(t, int64(0), val)
	assert.Contains(t, err.Error(), "out of range")
}

func TestHashIncByLargeNumbers(t *testing.T) {
	h := NewHash()

	// Test with large positive number
	val, err := h.IncBy("counter", math.MaxInt64/2)
	assert.NoError(t, err)
	assert.Equal(t, int64(math.MaxInt64/2), val)

	// Should still work
	val, err = h.IncBy("counter", 100)
	assert.NoError(t, err)
	assert.Equal(t, int64(math.MaxInt64/2+100), val)
}

func TestHashSize(t *testing.T) {
	h := NewHash()

	assert.Equal(t, int64(0), h.Size())

	h.Set(map[string]string{"key1": "value1"})
	assert.Equal(t, int64(1), h.Size())

	h.Set(map[string]string{"key2": "value2", "key3": "value3"})
	assert.Equal(t, int64(3), h.Size())

	h.Delete("key1")
	assert.Equal(t, int64(2), h.Size())

	h.Delete("key2", "key3")
	assert.Equal(t, int64(0), h.Size())
}

func TestHashComplexWorkflow(t *testing.T) {
	h := NewHash()

	// Setup initial data
	added := h.Set(map[string]string{
		"user:1:name":  "Alice",
		"user:1:age":   "30",
		"user:1:email": "alice@example.com",
	})
	assert.Equal(t, int64(3), added)

	// Check existence
	assert.True(t, h.Exists("user:1:name"))
	assert.False(t, h.Exists("user:1:phone"))

	// Get multiple values
	results := h.MGet("user:1:name", "user:1:age", "user:1:phone")
	assert.NotNil(t, results[0])
	assert.Equal(t, "Alice", *results[0])
	assert.NotNil(t, results[1])
	assert.Equal(t, "30", *results[1])
	assert.Nil(t, results[2])

	// Increment age
	newAge, err := h.IncBy("user:1:age", 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(31), newAge)

	// Try to set existing with SetNX (should fail)
	set := h.SetNX("user:1:name", "Bob")
	assert.False(t, set)

	// Set new field with SetNX (should succeed)
	set = h.SetNX("user:1:phone", "555-1234")
	assert.True(t, set)

	// Verify size
	assert.Equal(t, int64(4), h.Size())

	// Get all keys
	keys := h.GetKeys()
	assert.ElementsMatch(t, []string{"user:1:name", "user:1:age", "user:1:email", "user:1:phone"}, keys)

	// Update existing fields
	added = h.Set(map[string]string{
		"user:1:name":  "Alice Smith",
		"user:1:city":  "NYC",
	})
	assert.Equal(t, int64(1), added, "only city is new")

	// Delete some fields
	deleted := h.Delete("user:1:email", "user:1:phone")
	assert.Equal(t, int64(2), deleted)
	assert.Equal(t, int64(3), h.Size())

	// Get all remaining
	all := h.GetAll()
	assert.Len(t, all, 6) // 3 fields * 2 (key + value)
}

func TestHashEmptyStringValues(t *testing.T) {
	h := NewHash()

	// Set empty string value
	h.Set(map[string]string{"empty": ""})
	
	val, ok := h.Get("empty")
	assert.True(t, ok)
	assert.Empty(t, val)

	// Should still exist
	assert.True(t, h.Exists("empty"))
	assert.Equal(t, int64(1), h.Size())
}

func TestHashOverwriteWithEmptyString(t *testing.T) {
	h := NewHash()

	h.Set(map[string]string{"key": "value"})
	h.Set(map[string]string{"key": ""})

	val, ok := h.Get("key")
	assert.True(t, ok)
	assert.Empty(t, val)
}

func TestHashIncByStringZero(t *testing.T) {
	h := NewHash()
	h.Set(map[string]string{"counter": "0"})

	val, err := h.IncBy("counter", 5)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), val)
}

func TestHashIncByFloatString(t *testing.T) {
	h := NewHash()
	h.Set(map[string]string{"counter": "3.14"})

	val, err := h.IncBy("counter", 1)
	assert.Error(t, err)
	assert.Equal(t, int64(0), val)
}

func TestHashMultipleOperations(t *testing.T) {
	h := NewHash()

	// Add 100 fields
	fields := make(map[string]string)
	for i := 0; i < 100; i++ {
		fields[fmt.Sprintf("key%d", i)] = fmt.Sprintf("value%d", i)
	}
	added := h.Set(fields)
	assert.Equal(t, int64(100), added)
	assert.Equal(t, int64(100), h.Size())

	// Delete half
	keysToDelete := make([]string, 50)
	for i := 0; i < 50; i++ {
		keysToDelete[i] = fmt.Sprintf("key%d", i)
	}
	deleted := h.Delete(keysToDelete...)
	assert.Equal(t, int64(50), deleted)
	assert.Equal(t, int64(50), h.Size())

	// Verify correct keys remain
	for i := 50; i < 100; i++ {
		assert.True(t, h.Exists(fmt.Sprintf("key%d", i)))
	}
	for i := 0; i < 50; i++ {
		assert.False(t, h.Exists(fmt.Sprintf("key%d", i)))
	}
}