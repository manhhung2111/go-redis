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

	assert.Equal(t, ObjString, obj.Type)
	assert.Equal(t, EncInt, obj.Encoding)

	val, ok := obj.Value.(int64)
	require.True(t, ok)
	assert.Equal(t, int64(123), val)
}

func TestNewStringObjectNegativeInteger(t *testing.T) {
	obj := newStringObject("-456")

	assert.Equal(t, EncInt, obj.Encoding)
	assert.Equal(t, int64(-456), obj.Value.(int64))
}

func TestNewStringObjectZero(t *testing.T) {
	obj := newStringObject("0")

	assert.Equal(t, EncInt, obj.Encoding)
	assert.Equal(t, int64(0), obj.Value.(int64))
}

func TestNewStringObjectMaxInt64(t *testing.T) {
	obj := newStringObject("9223372036854775807")

	assert.Equal(t, EncInt, obj.Encoding)
	assert.Equal(t, int64(math.MaxInt64), obj.Value.(int64))
}

func TestNewStringObjectMinInt64(t *testing.T) {
	obj := newStringObject("-9223372036854775808")

	assert.Equal(t, EncInt, obj.Encoding)
	assert.Equal(t, int64(math.MinInt64), obj.Value.(int64))
}

func TestNewStringObjectRawString(t *testing.T) {
	obj := newStringObject("hello")

	assert.Equal(t, ObjString, obj.Type)
	assert.Equal(t, EncRaw, obj.Encoding)
	assert.Equal(t, "hello", obj.Value.(string))
}

func TestNewStringObjectEmptyString(t *testing.T) {
	obj := newStringObject("")

	assert.Equal(t, EncRaw, obj.Encoding)
	assert.Equal(t, "", obj.Value.(string))
}

func TestNewStringObjectFloat(t *testing.T) {
	obj := newStringObject("3.14")

	assert.Equal(t, EncRaw, obj.Encoding)
	assert.Equal(t, "3.14", obj.Value.(string))
}

func TestNewStringObjectIntegerOverflow(t *testing.T) {
	obj := newStringObject("9223372036854775808")

	assert.Equal(t, EncRaw, obj.Encoding)
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
		assert.Equal(t, tt.encoding, obj.Encoding, "input=%q", tt.input)
	}
}

func TestStringValueFromIntEncoding(t *testing.T) {
	obj := &RObj{Type: ObjString, Encoding: EncInt, Value: int64(42)}

	val, ok := obj.StringValue()
	require.True(t, ok)
	assert.Equal(t, "42", val)
}

func TestStringValueFromRawEncoding(t *testing.T) {
	obj := &RObj{Type: ObjString, Encoding: EncRaw, Value: "hello"}

	val, ok := obj.StringValue()
	require.True(t, ok)
	assert.Equal(t, "hello", val)
}

func TestStringValueNilObject(t *testing.T) {
	var obj *RObj

	val, ok := obj.StringValue()
	assert.False(t, ok)
	assert.Empty(t, val)
}

func TestStringValueWrongType(t *testing.T) {
	obj := &RObj{Type: ObjSet, Encoding: EncRaw, Value: "hello"}

	val, ok := obj.StringValue()
	assert.False(t, ok)
	assert.Empty(t, val)
}

func TestStringValueInvalidEncoding(t *testing.T) {
	obj := &RObj{Type: ObjString, Encoding: EncHashTable, Value: "hello"}

	val, ok := obj.StringValue()
	assert.False(t, ok)
	assert.Empty(t, val)
}

func TestStringValueNegativeInteger(t *testing.T) {
	obj := &RObj{Type: ObjString, Encoding: EncInt, Value: int64(-999)}

	val, ok := obj.StringValue()
	require.True(t, ok)
	assert.Equal(t, "-999", val)
}

func TestStringValueZero(t *testing.T) {
	obj := &RObj{Type: ObjString, Encoding: EncInt, Value: int64(0)}

	val, ok := obj.StringValue()
	require.True(t, ok)
	assert.Equal(t, "0", val)
}

func TestSetBasic(t *testing.T) {
	s := NewStore()

	s.Set("key1", "value1")

	obj, ok := s.Get("key1")
	require.True(t, ok)

	val, ok := obj.StringValue()
	require.True(t, ok)
	assert.Equal(t, "value1", val)
}

func TestSetInteger(t *testing.T) {
	s := NewStore()

	s.Set("key1", "123")

	obj, ok := s.Get("key1")
	require.True(t, ok)
	assert.Equal(t, EncInt, obj.Encoding)
}

func TestSetOverwrite(t *testing.T) {
	s := NewStore()

	s.Set("key1", "value1")
	s.Set("key1", "value2")

	obj, ok := s.Get("key1")
	require.True(t, ok)

	val, _ := obj.StringValue()
	assert.Equal(t, "value2", val)
}

func TestSetClearsExpiration(t *testing.T) {
	s := NewStore().(*store)

	s.SetEx("key1", "value1", 10)
	require.Contains(t, s.expires, "key1")

	s.Set("key1", "value2")
	assert.NotContains(t, s.expires, "key1")
}

func TestSetEmptyString(t *testing.T) {
	s := NewStore()

	s.Set("key1", "")

	obj, ok := s.Get("key1")
	require.True(t, ok)

	val, _ := obj.StringValue()
	assert.Empty(t, val)
}

func TestSetExBasic(t *testing.T) {
	s := NewStore().(*store)

	s.SetEx("key1", "value1", 10)

	obj, ok := s.Get("key1")
	require.True(t, ok)

	val, _ := obj.StringValue()
	assert.Equal(t, "value1", val)

	assert.Contains(t, s.expires, "key1")
}

func TestSetExExpires(t *testing.T) {
	s := NewStore()

	s.SetEx("key1", "value1", 0)
	time.Sleep(10 * time.Millisecond)

	_, ok := s.Get("key1")
	assert.False(t, ok)
}

func TestSetExOverwriteExpiration(t *testing.T) {
	s := NewStore().(*store)

	s.SetEx("key1", "value1", 5)
	exp1 := s.expires["key1"]

	time.Sleep(10 * time.Millisecond)

	s.SetEx("key1", "value2", 10)
	exp2 := s.expires["key1"]

	assert.Greater(t, exp2, exp1)
}

func TestGetExisting(t *testing.T) {
	s := NewStore()
	s.Set("key1", "value1")

	obj, ok := s.Get("key1")
	require.True(t, ok)

	val, _ := obj.StringValue()
	assert.Equal(t, "value1", val)
}

func TestGetNonExisting(t *testing.T) {
	s := NewStore()

	obj, ok := s.Get("nonexistent")
	assert.False(t, ok)
	assert.Nil(t, obj)
}

func TestGetExpiredKey(t *testing.T) {
	s := NewStore()

	s.SetEx("key1", "value1", 0)
	time.Sleep(10 * time.Millisecond)

	_, ok := s.Get("key1")
	assert.False(t, ok)
}

func TestGetDeletesExpiredKey(t *testing.T) {
	s := NewStore().(*store)

	s.SetEx("key1", "value1", 0)
	time.Sleep(10 * time.Millisecond)
	s.Get("key1")

	assert.NotContains(t, s.data, "key1")
	assert.NotContains(t, s.expires, "key1")
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
	obj := &RObj{Type: ObjString, Encoding: EncInt, Value: int64(10)}

	result, ok := obj.IncrBy(5)
	require.True(t, ok)

	assert.Equal(t, int64(15), result)
	assert.Equal(t, int64(15), obj.Value.(int64))
}

func TestIncrByRawEncodingValid(t *testing.T) {
	obj := &RObj{Type: ObjString, Encoding: EncRaw, Value: "42"}

	result, ok := obj.IncrBy(8)
	require.True(t, ok)

	assert.Equal(t, int64(50), result)
	assert.Equal(t, EncInt, obj.Encoding)
}

func TestIncrByRawEncodingInvalid(t *testing.T) {
	obj := &RObj{Type: ObjString, Encoding: EncRaw, Value: "not a number"}

	_, ok := obj.IncrBy(5)
	assert.False(t, ok)

	assert.Equal(t, EncRaw, obj.Encoding)
	assert.Equal(t, "not a number", obj.Value)
}

func TestSetGetDelFlow(t *testing.T) {
	s := NewStore()

	s.Set("key1", "value1")

	obj, ok := s.Get("key1")
	require.True(t, ok)

	val, _ := obj.StringValue()
	assert.Equal(t, "value1", val)

	assert.True(t, s.Del("key1"))

	_, ok = s.Get("key1")
	assert.False(t, ok)
}

func TestEncodingConversionThroughIncrBy(t *testing.T) {
	s := NewStore()

	s.Set("counter", "10")
	obj, _ := s.Get("counter")

	obj.IncrBy(5)

	assert.Equal(t, EncInt, obj.Encoding)

	val, _ := obj.StringValue()
	assert.Equal(t, "15", val)
}
