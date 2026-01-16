package storage

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStringObjectInteger(t *testing.T) {
	obj := newStringObject("123")

	assert.Equal(t, ObjString, obj.objType)
	assert.Equal(t, EncInt, obj.encoding)

	val, ok := obj.value.(int64)
	require.True(t, ok)
	assert.Equal(t, int64(123), val)
}

func TestNewStringObjectNegativeInteger(t *testing.T) {
	obj := newStringObject("-456")

	assert.Equal(t, EncInt, obj.encoding)
	assert.Equal(t, int64(-456), obj.value.(int64))
}

func TestNewStringObjectZero(t *testing.T) {
	obj := newStringObject("0")

	assert.Equal(t, EncInt, obj.encoding)
	assert.Equal(t, int64(0), obj.value.(int64))
}

func TestNewStringObjectMaxInt64(t *testing.T) {
	obj := newStringObject("9223372036854775807")

	assert.Equal(t, EncInt, obj.encoding)
	assert.Equal(t, int64(math.MaxInt64), obj.value.(int64))
}

func TestNewStringObjectMinInt64(t *testing.T) {
	obj := newStringObject("-9223372036854775808")

	assert.Equal(t, EncInt, obj.encoding)
	assert.Equal(t, int64(math.MinInt64), obj.value.(int64))
}

func TestNewStringObjectRawString(t *testing.T) {
	obj := newStringObject("hello")

	assert.Equal(t, ObjString, obj.objType)
	assert.Equal(t, EncRaw, obj.encoding)
	assert.Equal(t, "hello", obj.value.(string))
}

func TestNewStringObjectEmptyString(t *testing.T) {
	obj := newStringObject("")

	assert.Equal(t, EncRaw, obj.encoding)
	assert.Equal(t, "", obj.value.(string))
}

func TestNewStringObjectFloat(t *testing.T) {
	obj := newStringObject("3.14")

	assert.Equal(t, EncRaw, obj.encoding)
	assert.Equal(t, "3.14", obj.value.(string))
}

func TestNewStringObjectIntegerOverflow(t *testing.T) {
	obj := newStringObject("9223372036854775808")

	assert.Equal(t, EncRaw, obj.encoding)
}

func TestNewStringObjectMixedContent(t *testing.T) {
	tests := []struct {
		input    string
		encoding ObjectEncoding
	}{
		{"123abc", EncRaw},
		{"abc123", EncRaw},
		{"12.34.56", EncRaw},
		{"+123", EncInt},
		{"-0", EncInt},
		{"  123", EncRaw},
		{"123  ", EncRaw},
	}

	for _, tt := range tests {
		obj := newStringObject(tt.input)
		assert.Equal(t, tt.encoding, obj.encoding, "input=%q", tt.input)
	}
}

func TestSetBasic(t *testing.T) {
	s := NewStore()

	s.Set("key1", "value1")

	val, err := s.Get("key1")
	require.Nil(t, err)
	assert.Equal(t, "value1", *val)
}

func TestSetOverwrite(t *testing.T) {
	s := NewStore()

	s.Set("key1", "value1")
	s.Set("key1", "value2")

	val, err := s.Get("key1")
	require.Nil(t, err)
	assert.Equal(t, "value2", *val)
}

func TestSetClearsExpiration(t *testing.T) {
	s := NewStore().(*store)

	s.SetEx("key1", "value1", 10)
	_, exists := s.expires.Get("key1")
	require.True(t, exists)

	s.Set("key1", "value2")
	_, exists = s.expires.Get("key1")
	assert.False(t, exists)
}

func TestSetEmptyString(t *testing.T) {
	s := NewStore()

	s.Set("key1", "")

	val, err := s.Get("key1")
	require.Nil(t, err)
	assert.Empty(t, val)
}

func TestSetExBasic(t *testing.T) {
	s := NewStore().(*store)

	s.SetEx("key1", "value1", 10)

	val, err := s.Get("key1")
	require.Nil(t, err)
	assert.Equal(t, "value1", *val)

	_, exists := s.expires.Get("key1")
	assert.True(t, exists)
}

func TestSetExExpires(t *testing.T) {
	s := NewStore()

	s.SetEx("key1", "value1", 0)
	time.Sleep(10 * time.Millisecond)

	str, err := s.Get("key1")
	assert.Nil(t, err)
	assert.Nil(t, str)
}

func TestSetExOverwriteExpiration(t *testing.T) {
	s := NewStore().(*store)

	s.SetEx("key1", "value1", 5)
	exp1, _ := s.expires.Get("key1")

	time.Sleep(10 * time.Millisecond)

	s.SetEx("key1", "value2", 10)
	exp2, _ := s.expires.Get("key1")

	assert.Greater(t, exp2, exp1)
}

func TestGetExisting(t *testing.T) {
	s := NewStore()
	s.Set("key1", "value1")

	val, err := s.Get("key1")
	require.Nil(t, err)
	assert.Equal(t, "value1", *val)
}

func TestGetNonExisting(t *testing.T) {
	s := NewStore()

	val, err := s.Get("key1")
	require.Nil(t, err)
	assert.Nil(t, val)
}

func TestGetExpiredKey(t *testing.T) {
	s := NewStore()

	s.SetEx("key1", "value1", 0)
	time.Sleep(10 * time.Millisecond)

	str, err := s.Get("key1")
	assert.Nil(t, err)
	assert.Nil(t, str)
}

func TestGetDeletesExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.SetEx("key1", "value1", 0)
	time.Sleep(10 * time.Millisecond)
	s.Get("key1")

	_, exists := s.data.Get("key1")
	assert.False(t, exists)
	_, exists = s.expires.Get("key1")
	assert.False(t, exists)
}

func TestDelExisting(t *testing.T) {
	s := NewStore()
	s.Set("key1", "value1")

	assert.True(t, s.Del("key1"))
}

func TestDelNonExisting(t *testing.T) {
	s := NewStore()

	assert.False(t, s.Del("nonexistent"))
}

func TestIncrByIntEncoding(t *testing.T) {
	s := NewStore()

	result, err := s.IncrBy("counter", 5)
	require.Nil(t, err)

	assert.Equal(t, int64(5), *result)
}

func TestIncrByRawEncodingInvalid(t *testing.T) {
	s := NewStore()
	s.Set("key", "not a number")

	_, err := s.IncrBy("key", 5)
	assert.NotNil(t, err)
}

func TestSetGetDelFlow(t *testing.T) {
	s := NewStore()

	s.Set("key1", "value1")

	val, err := s.Get("key1")
	require.Nil(t, err)
	assert.Equal(t, "value1", *val)

	assert.True(t, s.Del("key1"))

	val, err = s.Get("key1")
	assert.Nil(t, err)
	assert.Nil(t, val)
}

func TestEncodingConversionThroughIncrBy(t *testing.T) {
	s := NewStore()

	s.Set("counter", "10")
	s.IncrBy("counter", 5)

	val, err := s.Get("counter")
	assert.Nil(t, err)
	assert.Equal(t, "15", *val)
}
