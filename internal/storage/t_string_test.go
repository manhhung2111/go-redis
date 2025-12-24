package storage

import (
	"math"
	"testing"
	"time"
)

func TestNewStringObjectInteger(t *testing.T) {
	obj := newStringObject("123")
	
	if obj.Type != ObjString {
		t.Errorf("Expected type ObjString, got %v", obj.Type)
	}
	
	if obj.Encoding != EncInt {
		t.Errorf("Expected encoding EncInt, got %v", obj.Encoding)
	}
	
	val, ok := obj.Value.(int64)
	if !ok {
		t.Fatal("Expected value to be int64")
	}
	
	if val != 123 {
		t.Errorf("Expected value 123, got %d", val)
	}
}

func TestNewStringObjectNegativeInteger(t *testing.T) {
	obj := newStringObject("-456")
	
	if obj.Encoding != EncInt {
		t.Errorf("Expected encoding EncInt, got %v", obj.Encoding)
	}
	
	val := obj.Value.(int64)
	if val != -456 {
		t.Errorf("Expected value -456, got %d", val)
	}
}

func TestNewStringObjectZero(t *testing.T) {
	obj := newStringObject("0")
	
	if obj.Encoding != EncInt {
		t.Errorf("Expected encoding EncInt, got %v", obj.Encoding)
	}
	
	val := obj.Value.(int64)
	if val != 0 {
		t.Errorf("Expected value 0, got %d", val)
	}
}

func TestNewStringObjectMaxInt64(t *testing.T) {
	obj := newStringObject("9223372036854775807")
	
	if obj.Encoding != EncInt {
		t.Errorf("Expected encoding EncInt, got %v", obj.Encoding)
	}
	
	val := obj.Value.(int64)
	if val != math.MaxInt64 {
		t.Errorf("Expected value %d, got %d", math.MaxInt64, val)
	}
}

func TestNewStringObjectMinInt64(t *testing.T) {
	obj := newStringObject("-9223372036854775808")
	
	if obj.Encoding != EncInt {
		t.Errorf("Expected encoding EncInt, got %v", obj.Encoding)
	}
	
	val := obj.Value.(int64)
	if val != math.MinInt64 {
		t.Errorf("Expected value %d, got %d", math.MinInt64, val)
	}
}

func TestNewStringObjectRawString(t *testing.T) {
	obj := newStringObject("hello")
	
	if obj.Type != ObjString {
		t.Errorf("Expected type ObjString, got %v", obj.Type)
	}
	
	if obj.Encoding != EncRaw {
		t.Errorf("Expected encoding EncRaw, got %v", obj.Encoding)
	}
	
	val, ok := obj.Value.(string)
	if !ok {
		t.Fatal("Expected value to be string")
	}
	
	if val != "hello" {
		t.Errorf("Expected value 'hello', got %s", val)
	}
}

func TestNewStringObjectEmptyString(t *testing.T) {
	obj := newStringObject("")
	
	if obj.Encoding != EncRaw {
		t.Errorf("Expected encoding EncRaw for empty string, got %v", obj.Encoding)
	}
	
	val := obj.Value.(string)
	if val != "" {
		t.Errorf("Expected empty string, got %s", val)
	}
}

func TestNewStringObjectFloat(t *testing.T) {
	obj := newStringObject("3.14")
	
	if obj.Encoding != EncRaw {
		t.Errorf("Expected encoding EncRaw for float, got %v", obj.Encoding)
	}
	
	val := obj.Value.(string)
	if val != "3.14" {
		t.Errorf("Expected value '3.14', got %s", val)
	}
}

func TestNewStringObjectIntegerOverflow(t *testing.T) {
	// Larger than MaxInt64
	obj := newStringObject("9223372036854775808")
	
	if obj.Encoding != EncRaw {
		t.Errorf("Expected encoding EncRaw for overflow, got %v", obj.Encoding)
	}
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
		{"  123", EncRaw}, // Leading space
		{"123  ", EncRaw}, // Trailing space
	}
	
	for _, tt := range tests {
		obj := newStringObject(tt.input)
		if obj.Encoding != tt.encoding {
			t.Errorf("Input %q: expected encoding %v, got %v", 
				tt.input, tt.encoding, obj.Encoding)
		}
	}
}

func TestStringValueFromIntEncoding(t *testing.T) {
	obj := &RObj{
		Type:     ObjString,
		Encoding: EncInt,
		Value:    int64(42),
	}
	
	val, ok := obj.StringValue()
	if !ok {
		t.Fatal("Expected StringValue to succeed")
	}
	
	if val != "42" {
		t.Errorf("Expected '42', got %s", val)
	}
}

func TestStringValueFromRawEncoding(t *testing.T) {
	obj := &RObj{
		Type:     ObjString,
		Encoding: EncRaw,
		Value:    "hello",
	}
	
	val, ok := obj.StringValue()
	if !ok {
		t.Fatal("Expected StringValue to succeed")
	}
	
	if val != "hello" {
		t.Errorf("Expected 'hello', got %s", val)
	}
}

func TestStringValueNilObject(t *testing.T) {
	var obj *RObj
	
	val, ok := obj.StringValue()
	if ok {
		t.Error("Expected StringValue to fail for nil object")
	}
	
	if val != "" {
		t.Errorf("Expected empty string, got %s", val)
	}
}

func TestStringValueWrongType(t *testing.T) {
	obj := &RObj{
		Type:     ObjSet,
		Encoding: EncRaw,
		Value:    "hello",
	}
	
	val, ok := obj.StringValue()
	if ok {
		t.Error("Expected StringValue to fail for non-string type")
	}
	
	if val != "" {
		t.Errorf("Expected empty string, got %s", val)
	}
}

func TestStringValueInvalidEncoding(t *testing.T) {
	obj := &RObj{
		Type:     ObjString,
		Encoding: EncHashTable, // Invalid for string
		Value:    "hello",
	}
	
	val, ok := obj.StringValue()
	if ok {
		t.Error("Expected StringValue to fail for invalid encoding")
	}
	
	if val != "" {
		t.Errorf("Expected empty string, got %s", val)
	}
}

func TestStringValueNegativeInteger(t *testing.T) {
	obj := &RObj{
		Type:     ObjString,
		Encoding: EncInt,
		Value:    int64(-999),
	}
	
	val, ok := obj.StringValue()
	if !ok {
		t.Fatal("Expected StringValue to succeed")
	}
	
	if val != "-999" {
		t.Errorf("Expected '-999', got %s", val)
	}
}

func TestStringValueZero(t *testing.T) {
	obj := &RObj{
		Type:     ObjString,
		Encoding: EncInt,
		Value:    int64(0),
	}
	
	val, ok := obj.StringValue()
	if !ok {
		t.Fatal("Expected StringValue to succeed")
	}
	
	if val != "0" {
		t.Errorf("Expected '0', got %s", val)
	}
}

func TestSetBasic(t *testing.T) {
	s := NewStore()
	
	s.Set("key1", "value1")
	
	obj, ok := s.Get("key1")
	if !ok {
		t.Fatal("Expected key to exist")
	}
	
	val, ok := obj.StringValue()
	if !ok {
		t.Fatal("Expected to get string value")
	}
	
	if val != "value1" {
		t.Errorf("Expected 'value1', got %s", val)
	}
}

func TestSetInteger(t *testing.T) {
	s := NewStore()
	
	s.Set("key1", "123")
	
	obj, ok := s.Get("key1")
	if !ok {
		t.Fatal("Expected key to exist")
	}
	
	if obj.Encoding != EncInt {
		t.Errorf("Expected EncInt encoding, got %v", obj.Encoding)
	}
}

func TestSetOverwrite(t *testing.T) {
	s := NewStore()
	
	s.Set("key1", "value1")
	s.Set("key1", "value2")
	
	obj, ok := s.Get("key1")
	if !ok {
		t.Fatal("Expected key to exist")
	}
	
	val, _ := obj.StringValue()
	if val != "value2" {
		t.Errorf("Expected 'value2', got %s", val)
	}
}

func TestSetClearsExpiration(t *testing.T) {
	s := NewStore().(*store)
	
	// Set with expiration
	s.SetEx("key1", "value1", 10)
	
	// Verify expiration is set
	if _, ok := s.expires["key1"]; !ok {
		t.Fatal("Expected expiration to be set")
	}
	
	// Overwrite with regular Set
	s.Set("key1", "value2")
	
	// Verify expiration is cleared
	if _, ok := s.expires["key1"]; ok {
		t.Error("Expected expiration to be cleared")
	}
}

func TestSetEmptyString(t *testing.T) {
	s := NewStore()
	
	s.Set("key1", "")
	
	obj, ok := s.Get("key1")
	if !ok {
		t.Fatal("Expected key to exist")
	}
	
	val, _ := obj.StringValue()
	if val != "" {
		t.Errorf("Expected empty string, got %s", val)
	}
}

func TestSetExBasic(t *testing.T) {
	s := NewStore().(*store)
	
	s.SetEx("key1", "value1", 10)
	
	obj, ok := s.Get("key1")
	if !ok {
		t.Fatal("Expected key to exist")
	}
	
	val, _ := obj.StringValue()
	if val != "value1" {
		t.Errorf("Expected 'value1', got %s", val)
	}
	
	// Check expiration is set
	if _, ok := s.expires["key1"]; !ok {
		t.Error("Expected expiration to be set")
	}
}

func TestSetExExpires(t *testing.T) {
	s := NewStore()
	
	// Set with 1 millisecond expiration
	s.SetEx("key1", "value1", 0) // 0 seconds = expires immediately
	
	// Wait a bit
	time.Sleep(10 * time.Millisecond)
	
	// Key should be expired
	obj, ok := s.Get("key1")
	if ok {
		t.Errorf("Expected key to be expired, got %v", obj)
	}
}

func TestSetExOverwriteExpiration(t *testing.T) {
	s := NewStore().(*store)
	
	s.SetEx("key1", "value1", 5)
	exp1 := s.expires["key1"]
	
	time.Sleep(10 * time.Millisecond)
	
	s.SetEx("key1", "value2", 10)
	exp2 := s.expires["key1"]
	
	if exp2 <= exp1 {
		t.Error("Expected new expiration to be later than old one")
	}
}

func TestGetExisting(t *testing.T) {
	s := NewStore()
	
	s.Set("key1", "value1")
	
	obj, ok := s.Get("key1")
	if !ok {
		t.Fatal("Expected key to exist")
	}
	
	val, _ := obj.StringValue()
	if val != "value1" {
		t.Errorf("Expected 'value1', got %s", val)
	}
}

func TestGetNonExisting(t *testing.T) {
	s := NewStore()
	
	obj, ok := s.Get("nonexistent")
	if ok {
		t.Errorf("Expected key to not exist, got %v", obj)
	}
	
	if obj != nil {
		t.Error("Expected nil object for non-existing key")
	}
}

func TestGetExpiredKey(t *testing.T) {
	s := NewStore()
	
	s.SetEx("key1", "value1", 0)
	time.Sleep(10 * time.Millisecond)
	
	obj, ok := s.Get("key1")
	if ok {
		t.Errorf("Expected expired key to not exist, got %v", obj)
	}
}

func TestGetDeletesExpiredKey(t *testing.T) {
	s := NewStore().(*store)
	
	s.SetEx("key1", "value1", 0)
	time.Sleep(10 * time.Millisecond)
	
	s.Get("key1")
	
	// Verify key is deleted from both maps
	if _, ok := s.data["key1"]; ok {
		t.Error("Expected expired key to be deleted from data")
	}
	
	if _, ok := s.expires["key1"]; ok {
		t.Error("Expected expired key to be deleted from expires")
	}
}

func TestGetNonExpiredKey(t *testing.T) {
	s := NewStore()
	
	s.SetEx("key1", "value1", 3600) // 1 hour
	
	obj, ok := s.Get("key1")
	if !ok {
		t.Fatal("Expected non-expired key to exist")
	}
	
	val, _ := obj.StringValue()
	if val != "value1" {
		t.Errorf("Expected 'value1', got %s", val)
	}
}

func TestDelExisting(t *testing.T) {
	s := NewStore()
	
	s.Set("key1", "value1")
	
	deleted := s.Del("key1")
	if !deleted {
		t.Error("Expected Del to return true")
	}
	
	// Verify key is gone
	obj, ok := s.Get("key1")
	if ok {
		t.Errorf("Expected key to be deleted, got %v", obj)
	}
}

func TestDelNonExisting(t *testing.T) {
	s := NewStore()
	
	deleted := s.Del("nonexistent")
	if deleted {
		t.Error("Expected Del to return false for non-existing key")
	}
}

func TestDelClearsExpiration(t *testing.T) {
	s := NewStore().(*store)
	
	s.SetEx("key1", "value1", 10)
	s.Del("key1")
	
	// Verify expiration is cleared
	if _, ok := s.expires["key1"]; ok {
		t.Error("Expected expiration to be cleared")
	}
}

func TestDelMultipleTimes(t *testing.T) {
	s := NewStore()
	
	s.Set("key1", "value1")
	
	deleted := s.Del("key1")
	if !deleted {
		t.Error("Expected first Del to return true")
	}
	
	deleted = s.Del("key1")
	if deleted {
		t.Error("Expected second Del to return false")
	}
}

func TestIncrByIntEncoding(t *testing.T) {
	obj := &RObj{
		Type:     ObjString,
		Encoding: EncInt,
		Value:    int64(10),
	}
	
	result, ok := obj.IncrBy(5)
	if !ok {
		t.Fatal("Expected IncrBy to succeed")
	}
	
	if result != 15 {
		t.Errorf("Expected 15, got %d", result)
	}
	
	// Verify encoding and value are updated
	if obj.Encoding != EncInt {
		t.Error("Expected encoding to remain EncInt")
	}
	
	if obj.Value.(int64) != 15 {
		t.Errorf("Expected value 15, got %d", obj.Value.(int64))
	}
}

func TestIncrByRawEncodingValid(t *testing.T) {
	obj := &RObj{
		Type:     ObjString,
		Encoding: EncRaw,
		Value:    "42",
	}
	
	result, ok := obj.IncrBy(8)
	if !ok {
		t.Fatal("Expected IncrBy to succeed")
	}
	
	if result != 50 {
		t.Errorf("Expected 50, got %d", result)
	}
	
	// Verify encoding is changed to EncInt
	if obj.Encoding != EncInt {
		t.Errorf("Expected encoding to change to EncInt, got %v", obj.Encoding)
	}
	
	if obj.Value.(int64) != 50 {
		t.Errorf("Expected value 50, got %d", obj.Value.(int64))
	}
}

func TestIncrByRawEncodingInvalid(t *testing.T) {
	obj := &RObj{
		Type:     ObjString,
		Encoding: EncRaw,
		Value:    "not a number",
	}
	
	result, ok := obj.IncrBy(5)
	if ok {
		t.Errorf("Expected IncrBy to fail, got result %d", result)
	}
	
	// Verify object is unchanged
	if obj.Encoding != EncRaw {
		t.Error("Expected encoding to remain EncRaw on failure")
	}
	
	if obj.Value.(string) != "not a number" {
		t.Error("Expected value to remain unchanged on failure")
	}
}

func TestIncrByNegativeIncrement(t *testing.T) {
	obj := &RObj{
		Type:     ObjString,
		Encoding: EncInt,
		Value:    int64(100),
	}
	
	result, ok := obj.IncrBy(-30)
	if !ok {
		t.Fatal("Expected IncrBy to succeed")
	}
	
	if result != 70 {
		t.Errorf("Expected 70, got %d", result)
	}
}

func TestIncrByZero(t *testing.T) {
	obj := &RObj{
		Type:     ObjString,
		Encoding: EncInt,
		Value:    int64(42),
	}
	
	result, ok := obj.IncrBy(0)
	if !ok {
		t.Fatal("Expected IncrBy to succeed")
	}
	
	if result != 42 {
		t.Errorf("Expected 42, got %d", result)
	}
}

func TestIncrByOverflowPositive(t *testing.T) {
	obj := &RObj{
		Type:     ObjString,
		Encoding: EncInt,
		Value:    int64(math.MaxInt64),
	}
	
	result, ok := obj.IncrBy(1)
	if ok {
		t.Errorf("Expected IncrBy to fail on overflow, got result %d", result)
	}
	
	// Verify object is unchanged
	if obj.Value.(int64) != math.MaxInt64 {
		t.Error("Expected value to remain unchanged on overflow")
	}
}

func TestIncrByOverflowNegative(t *testing.T) {
	obj := &RObj{
		Type:     ObjString,
		Encoding: EncInt,
		Value:    int64(math.MinInt64),
	}
	
	result, ok := obj.IncrBy(-1)
	if ok {
		t.Errorf("Expected IncrBy to fail on underflow, got result %d", result)
	}
	
	// Verify object is unchanged
	if obj.Value.(int64) != math.MinInt64 {
		t.Error("Expected value to remain unchanged on underflow")
	}
}

func TestIncrByNearOverflow(t *testing.T) {
	obj := &RObj{
		Type:     ObjString,
		Encoding: EncInt,
		Value:    int64(math.MaxInt64 - 10),
	}
	
	result, ok := obj.IncrBy(10)
	if !ok {
		t.Fatal("Expected IncrBy to succeed")
	}
	
	if result != math.MaxInt64 {
		t.Errorf("Expected %d, got %d", math.MaxInt64, result)
	}
	
	// Try to increment again (should fail)
	result, ok = obj.IncrBy(1)
	if ok {
		t.Errorf("Expected IncrBy to fail on overflow, got result %d", result)
	}
}

func TestIncrByWrongType(t *testing.T) {
	obj := &RObj{
		Type:     ObjSet,
		Encoding: EncInt,
		Value:    int64(10),
	}
	
	result, ok := obj.IncrBy(5)
	if ok {
		t.Errorf("Expected IncrBy to fail on wrong type, got result %d", result)
	}
}

func TestIncrByInvalidEncoding(t *testing.T) {
	obj := &RObj{
		Type:     ObjString,
		Encoding: EncHashTable,
		Value:    int64(10),
	}
	
	result, ok := obj.IncrBy(5)
	if ok {
		t.Errorf("Expected IncrBy to fail on invalid encoding, got result %d", result)
	}
}

func TestSetGetDelFlow(t *testing.T) {
	s := NewStore()
	
	// Set
	s.Set("key1", "value1")
	
	// Get
	obj, ok := s.Get("key1")
	if !ok {
		t.Fatal("Expected key to exist after Set")
	}
	
	val, _ := obj.StringValue()
	if val != "value1" {
		t.Errorf("Expected 'value1', got %s", val)
	}
	
	// Del
	deleted := s.Del("key1")
	if !deleted {
		t.Error("Expected Del to succeed")
	}
	
	// Get again (should fail)
	obj, ok = s.Get("key1")
	if ok {
		t.Errorf("Expected key to not exist after Del, got %v", obj)
	}
}

func TestMultipleKeys(t *testing.T) {
	s := NewStore()
	
	s.Set("key1", "value1")
	s.Set("key2", "123")
	s.Set("key3", "value3")
	
	// Verify all exist
	obj1, ok1 := s.Get("key1")
	obj2, ok2 := s.Get("key2")
	obj3, ok3 := s.Get("key3")
	
	if !ok1 || !ok2 || !ok3 {
		t.Fatal("Expected all keys to exist")
	}
	
	// Check encodings
	if obj1.Encoding != EncRaw {
		t.Error("Expected key1 to use EncRaw")
	}
	if obj2.Encoding != EncInt {
		t.Error("Expected key2 to use EncInt")
	}
	if obj3.Encoding != EncRaw {
		t.Error("Expected key3 to use EncRaw")
	}
}

func TestEncodingConversionThroughIncrBy(t *testing.T) {
	s := NewStore()
	
	// Set as string
	s.Set("counter", "10")
	
	obj, _ := s.Get("counter")
	
	// Increment (should convert to EncInt)
	obj.IncrBy(5)
	
	// Verify encoding changed
	if obj.Encoding != EncInt {
		t.Errorf("Expected encoding EncInt after IncrBy, got %v", obj.Encoding)
	}
	
	val, _ := obj.StringValue()
	if val != "15" {
		t.Errorf("Expected '15', got %s", val)
	}
}