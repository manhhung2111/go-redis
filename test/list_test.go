package test

import (
	"bytes"
	"testing"

	"github.com/manhhung2111/go-redis/internal/constant"
)

func TestLPush(t *testing.T) {
	r := newTestRedis()

	resp := r.LPush(cmd("LPUSH", "mylist", "a", "b", "c"))
	if !bytes.Equal(resp, []byte(":3\r\n")) {
		t.Fatalf("unexpected resp: %q", resp)
	}

	// Push more elements
	resp = r.LPush(cmd("LPUSH", "mylist", "d"))
	if !bytes.Equal(resp, []byte(":4\r\n")) {
		t.Fatalf("expected total size 4, got: %q", resp)
	}
}

func TestLPushWrongArgs(t *testing.T) {
	r := newTestRedis()
	
	// Missing element argument
	resp := r.LPush(cmd("LPUSH", "mylist"))
	if !bytes.HasPrefix(resp, []byte("-ERR")) {
		t.Fatal("expected ERR for missing arguments")
	}
	
	// Missing key
	resp = r.LPush(cmd("LPUSH"))
	if !bytes.HasPrefix(resp, []byte("-ERR")) {
		t.Fatal("expected ERR for missing key")
	}
}

func TestLPushWrongType(t *testing.T) {
	r := newTestRedis()
	
	// Create a string key
	r.Set(cmd("SET", "mykey", "value"))
	
	// Try to LPUSH on string key
	resp := r.LPush(cmd("LPUSH", "mykey", "a"))
	if !bytes.Equal(resp, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY) {
		t.Fatalf("expected WRONGTYPE, got: %q", resp)
	}
}

func TestLPopSingle(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "mylist", "a", "b", "c"))
	
	// Pop single element (no count)
	resp := r.LPop(cmd("LPOP", "mylist"))
	if !bytes.HasPrefix(resp, []byte("$")) {
		t.Fatalf("expected bulk string, got %q", resp)
	}
}

func TestLPopWithCount(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "mylist", "a", "b", "c", "d", "e"))
	
	// Pop 3 elements
	resp := r.LPop(cmd("LPOP", "mylist", "3"))
	if !bytes.HasPrefix(resp, []byte("*3\r\n")) {
		t.Fatalf("expected array of 3, got %q", resp)
	}
}

func TestLPopNil(t *testing.T) {
	r := newTestRedis()
	
	// Pop from non-existent list
	resp := r.LPop(cmd("LPOP", "missing"))
	if !bytes.Equal(resp, constant.RESP_NIL_BULK_STRING) {
		t.Fatalf("expected NIL, got: %q", resp)
	}
}

func TestLPopWrongArgs(t *testing.T) {
	r := newTestRedis()
	
	// Missing key
	resp := r.LPop(cmd("LPOP"))
	if !bytes.HasPrefix(resp, []byte("-ERR")) {
		t.Fatal("expected ERR for missing key")
	}
}

func TestLPopInvalidCount(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "mylist", "a", "b"))
	
	// Invalid count (not a number)
	resp := r.LPop(cmd("LPOP", "mylist", "abc"))
	if !bytes.Equal(resp, constant.RESP_VALUE_IS_OUT_OF_RANGE_MUST_BE_POSITIVE) {
		t.Fatalf("expected out of range error, got: %q", resp)
	}
	
	// Negative count
	resp = r.LPop(cmd("LPOP", "mylist", "-1"))
	if !bytes.Equal(resp, constant.RESP_VALUE_IS_OUT_OF_RANGE_MUST_BE_POSITIVE) {
		t.Fatalf("expected out of range error, got: %q", resp)
	}
}

func TestLPopWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "mykey", "value"))
	
	resp := r.LPop(cmd("LPOP", "mykey"))
	if !bytes.Equal(resp, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY) {
		t.Fatalf("expected WRONGTYPE, got: %q", resp)
	}
}

func TestRPush(t *testing.T) {
	r := newTestRedis()
	
	resp := r.RPush(cmd("RPUSH", "mylist", "a", "b", "c"))
	if !bytes.Equal(resp, []byte(":3\r\n")) {
		t.Fatalf("unexpected resp: %q", resp)
	}
	
	// Push more elements
	resp = r.RPush(cmd("RPUSH", "mylist", "d", "e"))
	if !bytes.Equal(resp, []byte(":5\r\n")) {
		t.Fatalf("expected total size 5, got: %q", resp)
	}
}

func TestRPushWrongArgs(t *testing.T) {
	r := newTestRedis()
	
	resp := r.RPush(cmd("RPUSH", "mylist"))
	if !bytes.HasPrefix(resp, []byte("-ERR")) {
		t.Fatal("expected ERR for missing arguments")
	}
}

func TestRPushWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "mykey", "value"))
	
	resp := r.RPush(cmd("RPUSH", "mykey", "a"))
	if !bytes.Equal(resp, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY) {
		t.Fatalf("expected WRONGTYPE, got: %q", resp)
	}
}

func TestRPopSingle(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "mylist", "a", "b", "c"))
	
	resp := r.RPop(cmd("RPOP", "mylist"))
	if !bytes.HasPrefix(resp, []byte("$")) {
		t.Fatalf("expected bulk string, got %q", resp)
	}
}

func TestRPopWithCount(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "mylist", "a", "b", "c", "d", "e"))
	
	resp := r.RPop(cmd("RPOP", "mylist", "2"))
	if !bytes.HasPrefix(resp, []byte("*2\r\n")) {
		t.Fatalf("expected array of 2, got %q", resp)
	}
}

func TestRPopNil(t *testing.T) {
	r := newTestRedis()
	
	resp := r.RPop(cmd("RPOP", "missing"))
	if !bytes.Equal(resp, constant.RESP_NIL_BULK_STRING) {
		t.Fatalf("expected NIL, got: %q", resp)
	}
}

func TestRPopWrongArgs(t *testing.T) {
	r := newTestRedis()
	
	resp := r.RPop(cmd("RPOP"))
	if !bytes.HasPrefix(resp, []byte("-ERR")) {
		t.Fatal("expected ERR for missing key")
	}
}

func TestRPopInvalidCount(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "mylist", "a", "b"))
	
	resp := r.RPop(cmd("RPOP", "mylist", "invalid"))
	if !bytes.Equal(resp, constant.RESP_VALUE_IS_OUT_OF_RANGE_MUST_BE_POSITIVE) {
		t.Fatalf("expected out of range error, got: %q", resp)
	}
	
	resp = r.RPop(cmd("RPOP", "mylist", "-5"))
	if !bytes.Equal(resp, constant.RESP_VALUE_IS_OUT_OF_RANGE_MUST_BE_POSITIVE) {
		t.Fatalf("expected out of range error, got: %q", resp)
	}
}

func TestRPopWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "mykey", "value"))
	
	resp := r.RPop(cmd("RPOP", "mykey"))
	if !bytes.Equal(resp, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY) {
		t.Fatalf("expected WRONGTYPE, got: %q", resp)
	}
}

func TestLRange(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "mylist", "a", "b", "c", "d", "e"))
	
	// Get all elements
	resp := r.LRange(cmd("LRANGE", "mylist", "0", "-1"))
	if !bytes.HasPrefix(resp, []byte("*5\r\n")) {
		t.Fatalf("expected array of 5, got %q", resp)
	}
}

func TestLRangePartial(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "mylist", "a", "b", "c", "d", "e"))
	
	// Get middle elements
	resp := r.LRange(cmd("LRANGE", "mylist", "1", "3"))
	if !bytes.HasPrefix(resp, []byte("*3\r\n")) {
		t.Fatalf("expected array of 3, got %q", resp)
	}
}

func TestLRangeNegativeIndices(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "mylist", "a", "b", "c", "d", "e"))
	
	// Get last 3 elements
	resp := r.LRange(cmd("LRANGE", "mylist", "-3", "-1"))
	if !bytes.HasPrefix(resp, []byte("*3\r\n")) {
		t.Fatalf("expected array of 3, got %q", resp)
	}
}

func TestLRangeEmpty(t *testing.T) {
	r := newTestRedis()
	
	// Non-existent key
	resp := r.LRange(cmd("LRANGE", "missing", "0", "-1"))
	if !bytes.Equal(resp, []byte("*0\r\n")) {
		t.Fatalf("expected empty array, got %q", resp)
	}
}

func TestLRangeOutOfBounds(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "mylist", "a", "b", "c"))
	
	// Start > end
	resp := r.LRange(cmd("LRANGE", "mylist", "5", "1"))
	if !bytes.Equal(resp, []byte("*0\r\n")) {
		t.Fatalf("expected empty array, got %q", resp)
	}
	
	// Start beyond size
	resp = r.LRange(cmd("LRANGE", "mylist", "10", "20"))
	if !bytes.Equal(resp, []byte("*0\r\n")) {
		t.Fatalf("expected empty array, got %q", resp)
	}
}

func TestLRangeWrongArgs(t *testing.T) {
	r := newTestRedis()
	
	// Missing arguments
	resp := r.LRange(cmd("LRANGE", "mylist", "0"))
	if !bytes.HasPrefix(resp, []byte("-ERR")) {
		t.Fatal("expected ERR for missing stop argument")
	}
	
	resp = r.LRange(cmd("LRANGE", "mylist"))
	if !bytes.HasPrefix(resp, []byte("-ERR")) {
		t.Fatal("expected ERR for missing arguments")
	}
	
	// Too many arguments
	resp = r.LRange(cmd("LRANGE", "mylist", "0", "1", "extra"))
	if !bytes.HasPrefix(resp, []byte("-ERR")) {
		t.Fatal("expected ERR for too many arguments")
	}
}

func TestLRangeInvalidIndices(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "mylist", "a", "b", "c"))
	
	// Invalid start index
	resp := r.LRange(cmd("LRANGE", "mylist", "abc", "2"))
	if !bytes.Equal(resp, constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE) {
		t.Fatalf("expected integer error, got: %q", resp)
	}
	
	// Invalid stop index
	resp = r.LRange(cmd("LRANGE", "mylist", "0", "xyz"))
	if !bytes.Equal(resp, constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE) {
		t.Fatalf("expected integer error, got: %q", resp)
	}
}

func TestLRangeWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "mykey", "value"))
	
	resp := r.LRange(cmd("LRANGE", "mykey", "0", "-1"))
	if !bytes.Equal(resp, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY) {
		t.Fatalf("expected WRONGTYPE, got: %q", resp)
	}
}

func TestMixedListOperations(t *testing.T) {
	r := newTestRedis()
	
	// Build a list with mixed push operations
	r.LPush(cmd("LPUSH", "mylist", "2", "1"))
	r.RPush(cmd("RPUSH", "mylist", "3", "4"))
	r.LPush(cmd("LPUSH", "mylist", "0"))
	r.RPush(cmd("RPUSH", "mylist", "5"))
	
	// Verify with LRANGE
	resp := r.LRange(cmd("LRANGE", "mylist", "0", "-1"))
	// Should contain 6 elements
	if !bytes.HasPrefix(resp, []byte("*6\r\n")) {
		t.Fatalf("expected array of 6, got %q", resp)
	}
}

func TestListPopUntilEmpty(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "mylist", "a", "b", "c"))
	
	// Pop all elements
	r.LPop(cmd("LPOP", "mylist", "3"))
	
	// List should be empty now
	resp := r.LRange(cmd("LRANGE", "mylist", "0", "-1"))
	if !bytes.Equal(resp, []byte("*0\r\n")) {
		t.Fatalf("expected empty list, got: %q", resp)
	}
	
	// Pop from empty list should return nil
	resp = r.LPop(cmd("LPOP", "mylist"))
	if !bytes.Equal(resp, constant.RESP_NIL_BULK_STRING) {
		t.Fatalf("expected NIL, got: %q", resp)
	}
}

func TestListAlternatingPushPop(t *testing.T) {
	r := newTestRedis()
	
	r.LPush(cmd("LPUSH", "mylist", "1"))
	r.RPush(cmd("RPUSH", "mylist", "2"))
	r.LPop(cmd("LPOP", "mylist"))
	r.RPush(cmd("RPUSH", "mylist", "3"))
	r.LPush(cmd("LPUSH", "mylist", "4"))
	r.RPop(cmd("RPOP", "mylist"))
	
	// Should have 2 elements remaining
	resp := r.LRange(cmd("LRANGE", "mylist", "0", "-1"))
	if !bytes.HasPrefix(resp, []byte("*2\r\n")) {
		t.Fatalf("expected 2 elements, got: %q", resp)
	}
}


func TestLargeListOperations(t *testing.T) {
	r := newTestRedis()
	
	// Push 100 elements
	elements := make([]string, 102) // "RPUSH" + "mylist" + 100 elements
	elements[0] = "RPUSH"
	elements[1] = "mylist"
	for i := 0; i < 100; i++ {
		elements[i+2] = string(rune('0' + (i % 10)))
	}
	
	resp := r.RPush(cmd(elements[0], elements[1:]...))
	if !bytes.Equal(resp, []byte(":100\r\n")) {
		t.Fatalf("expected size 100, got: %q", resp)
	}
	
	// Pop 50 from left
	resp = r.LPop(cmd("LPOP", "mylist", "50"))
	if !bytes.HasPrefix(resp, []byte("*50\r\n")) {
		t.Fatalf("expected array of 50, got %q", resp)
	}
	
	// Verify remaining
	resp = r.LRange(cmd("LRANGE", "mylist", "0", "-1"))
	if !bytes.HasPrefix(resp, []byte("*50\r\n")) {
		t.Fatalf("expected 50 remaining, got %q", resp)
	}
}

func TestListSingleElement(t *testing.T) {
	r := newTestRedis()
	
	r.LPush(cmd("LPUSH", "mylist", "only"))
	
	// Range single element
	resp := r.LRange(cmd("LRANGE", "mylist", "0", "0"))
	if !bytes.HasPrefix(resp, []byte("*1\r\n")) {
		t.Fatalf("expected 1 element, got: %q", resp)
	}
	
	// Pop the element
	resp = r.LPop(cmd("LPOP", "mylist"))
	if !bytes.HasPrefix(resp, []byte("$4\r\nonly")) {
		t.Fatalf("expected 'only', got: %q", resp)
	}
	
	// List should be empty
	resp = r.LRange(cmd("LRANGE", "mylist", "0", "-1"))
	if !bytes.Equal(resp, []byte("*0\r\n")) {
		t.Fatalf("expected empty list, got: %q", resp)
	}
}

func TestListPopMoreThanExists(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "mylist", "a", "b", "c"))
	
	// Try to pop more than exists
	resp := r.LPop(cmd("LPOP", "mylist", "10"))
	// Should return only the 3 elements that exist
	if !bytes.HasPrefix(resp, []byte("*3\r\n")) {
		t.Fatalf("expected 3 elements, got: %q", resp)
	}
}

// LIndex Tests
func TestLIndex(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "k", "c", "b", "a"))

	resp := r.LIndex(cmd("LINDEX", "k", "0"))
	if !bytes.Equal(resp, []byte("$1\r\na\r\n")) {
		t.Fatalf("expected 'a', got %q", resp)
	}

	resp = r.LIndex(cmd("LINDEX", "k", "2"))
	if !bytes.Equal(resp, []byte("$1\r\nc\r\n")) {
		t.Fatalf("expected 'c', got %q", resp)
	}
}

func TestLIndexNegative(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "k", "c", "b", "a"))

	resp := r.LIndex(cmd("LINDEX", "k", "-1"))
	if !bytes.Equal(resp, []byte("$1\r\nc\r\n")) {
		t.Fatalf("expected 'c', got %q", resp)
	}
}

func TestLIndexOutOfRange(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "k", "a"))

	resp := r.LIndex(cmd("LINDEX", "k", "10"))
	if !bytes.Equal(resp, constant.RESP_NIL_BULK_STRING) {
		t.Fatalf("expected nil bulk string, got %q", resp)
	}
}

func TestLIndexNonExistentKey(t *testing.T) {
	r := newTestRedis()

	resp := r.LIndex(cmd("LINDEX", "missing", "0"))
	if !bytes.Equal(resp, constant.RESP_NIL_BULK_STRING) {
		t.Fatalf("expected nil bulk string")
	}
}

func TestLIndexWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.LIndex(cmd("LINDEX", "k", "0"))
	if !bytes.Equal(resp, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY) {
		t.Fatalf("expected WRONGTYPE")
	}
}

func TestLIndexInvalidIndex(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "k", "a"))

	resp := r.LIndex(cmd("LINDEX", "k", "notanumber"))
	if !bytes.Equal(resp, constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE) {
		t.Fatalf("expected integer error")
	}
}

func TestLIndexWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.LIndex(cmd("LINDEX", "k"))
	if !bytes.HasPrefix(resp, []byte("-ERR")) {
		t.Fatal("expected ERR")
	}
}

// LLen Tests
func TestLLen(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "k", "a", "b", "c"))

	resp := r.LLen(cmd("LLEN", "k"))
	if !bytes.Equal(resp, []byte(":3\r\n")) {
		t.Fatalf("expected 3, got %q", resp)
	}
}

func TestLLenNonExistentKey(t *testing.T) {
	r := newTestRedis()

	resp := r.LLen(cmd("LLEN", "missing"))
	if !bytes.Equal(resp, []byte(":0\r\n")) {
		t.Fatalf("expected 0, got %q", resp)
	}
}

func TestLLenWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.LLen(cmd("LLEN", "k"))
	if !bytes.Equal(resp, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY) {
		t.Fatalf("expected WRONGTYPE")
	}
}

func TestLLenWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.LLen(cmd("LLEN"))
	if !bytes.HasPrefix(resp, []byte("-ERR")) {
		t.Fatal("expected ERR")
	}
}

// LRem Tests
func TestLRem(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "k", "a", "b", "a", "c", "a"))

	resp := r.LRem(cmd("LREM", "k", "2", "a"))
	if !bytes.Equal(resp, []byte(":2\r\n")) {
		t.Fatalf("expected 2 removed, got %q", resp)
	}
}

func TestLRemNegativeCount(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "k", "a", "b", "a", "c", "a"))

	resp := r.LRem(cmd("LREM", "k", "-2", "a"))
	if !bytes.Equal(resp, []byte(":2\r\n")) {
		t.Fatalf("expected 2 removed from tail, got %q", resp)
	}
}

func TestLRemZeroCount(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "k", "a", "b", "a", "c", "a"))

	resp := r.LRem(cmd("LREM", "k", "0", "a"))
	if !bytes.Equal(resp, []byte(":3\r\n")) {
		t.Fatalf("expected all 'a' removed, got %q", resp)
	}
}

func TestLRemNonExistentKey(t *testing.T) {
	r := newTestRedis()

	resp := r.LRem(cmd("LREM", "missing", "1", "a"))
	if !bytes.Equal(resp, []byte(":0\r\n")) {
		t.Fatalf("expected 0, got %q", resp)
	}
}

func TestLRemWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.LRem(cmd("LREM", "k", "1", "a"))
	if !bytes.Equal(resp, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY) {
		t.Fatalf("expected WRONGTYPE")
	}
}

func TestLRemInvalidCount(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "k", "a"))

	resp := r.LRem(cmd("LREM", "k", "notanumber", "a"))
	if !bytes.Equal(resp, constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE) {
		t.Fatalf("expected integer error")
	}
}

func TestLRemWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.LRem(cmd("LREM", "k", "1"))
	if !bytes.HasPrefix(resp, []byte("-ERR")) {
		t.Fatal("expected ERR")
	}
}

// LSet Tests
func TestLSet(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "k", "c", "b", "a"))

	resp := r.LSet(cmd("LSET", "k", "1", "x"))
	if !bytes.Equal(resp, constant.RESP_OK) {
		t.Fatalf("expected OK, got %q", resp)
	}

	// Verify the change
	val := r.LIndex(cmd("LINDEX", "k", "1"))
	if !bytes.Equal(val, []byte("$1\r\nx\r\n")) {
		t.Fatalf("expected 'x', got %q", val)
	}
}

func TestLSetNegativeIndex(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "k", "c", "b", "a"))

	resp := r.LSet(cmd("LSET", "k", "-1", "z"))
	if !bytes.Equal(resp, constant.RESP_OK) {
		t.Fatalf("expected OK, got %q", resp)
	}
}

func TestLSetOutOfRange(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "k", "a"))

	resp := r.LSet(cmd("LSET", "k", "10", "x"))
	if !bytes.HasPrefix(resp, []byte("-")) {
		t.Fatalf("expected error for out of range, but got %q", resp)
	}
}

func TestLSetNonExistentKey(t *testing.T) {
	r := newTestRedis()

	resp := r.LSet(cmd("LSET", "missing", "0", "x"))
	if !bytes.HasPrefix(resp, []byte("-")) {
		t.Fatalf("expected error for non-existent key, but got %q", resp)
	}
}

func TestLSetWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.LSet(cmd("LSET", "k", "0", "x"))
	if !bytes.Equal(resp, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY) {
		t.Fatalf("expected WRONGTYPE")
	}
}

func TestLSetInvalidIndex(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "k", "a"))

	resp := r.LSet(cmd("LSET", "k", "notanumber", "x"))
	if !bytes.Equal(resp, constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE) {
		t.Fatalf("expected integer error")
	}
}

func TestLSetWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.LSet(cmd("LSET", "k", "0"))
	if !bytes.HasPrefix(resp, []byte("-ERR")) {
		t.Fatal("expected ERR")
	}
}

// LTrim Tests
func TestLTrim(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "k", "a", "b", "c", "d", "e"))

	resp := r.LTrim(cmd("LTRIM", "k", "1", "3"))
	if !bytes.Equal(resp, constant.RESP_OK) {
		t.Fatalf("expected OK, got %q", resp)
	}

	// Verify the list was trimmed
	length := r.LLen(cmd("LLEN", "k"))
	if !bytes.Equal(length, []byte(":3\r\n")) {
		t.Fatalf("expected length 3, got %q", length)
	}
}

func TestLTrimNegativeIndices(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "k", "a", "b", "c", "d", "e"))

	resp := r.LTrim(cmd("LTRIM", "k", "-3", "-1"))
	if !bytes.Equal(resp, constant.RESP_OK) {
		t.Fatalf("expected OK")
	}
}

func TestLTrimNonExistentKey(t *testing.T) {
	r := newTestRedis()

	resp := r.LTrim(cmd("LTRIM", "missing", "0", "1"))
	if !bytes.Equal(resp, constant.RESP_OK) {
		t.Fatalf("expected OK for non-existent key")
	}
}

func TestLTrimWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.LTrim(cmd("LTRIM", "k", "0", "1"))
	if !bytes.Equal(resp, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY) {
		t.Fatalf("expected WRONGTYPE")
	}
}

func TestLTrimInvalidIndices(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "k", "a"))

	resp := r.LTrim(cmd("LTRIM", "k", "notanumber", "1"))
	if !bytes.Equal(resp, constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE) {
		t.Fatalf("expected integer error")
	}
}

func TestLTrimWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.LTrim(cmd("LTRIM", "k", "0"))
	if !bytes.HasPrefix(resp, []byte("-ERR")) {
		t.Fatal("expected ERR")
	}
}

// LPushX Tests
func TestLPushX(t *testing.T) {
	r := newTestRedis()
	r.LPush(cmd("LPUSH", "k", "a"))

	resp := r.LPushX(cmd("LPUSHX", "k", "b", "c"))
	if !bytes.Equal(resp, []byte(":3\r\n")) {
		t.Fatalf("expected 3, got %q", resp)
	}
}

func TestLPushXNonExistentKey(t *testing.T) {
	r := newTestRedis()

	resp := r.LPushX(cmd("LPUSHX", "missing", "a"))
	if !bytes.Equal(resp, []byte(":0\r\n")) {
		t.Fatalf("expected 0, got %q", resp)
	}

	// Verify key was not created
	length := r.LLen(cmd("LLEN", "missing"))
	if !bytes.Equal(length, []byte(":0\r\n")) {
		t.Fatalf("key should not exist")
	}
}

func TestLPushXWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.LPushX(cmd("LPUSHX", "k", "a"))
	if !bytes.Equal(resp, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY) {
		t.Fatalf("expected WRONGTYPE")
	}
}

func TestLPushXWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.LPushX(cmd("LPUSHX", "k"))
	if !bytes.HasPrefix(resp, []byte("-ERR")) {
		t.Fatal("expected ERR")
	}
}

// RPushX Tests
func TestRPushX(t *testing.T) {
	r := newTestRedis()
	r.RPush(cmd("RPUSH", "k", "a"))

	resp := r.RPushX(cmd("RPUSHX", "k", "b", "c"))
	if !bytes.Equal(resp, []byte(":3\r\n")) {
		t.Fatalf("expected 3, got %q", resp)
	}
}

func TestRPushXNonExistentKey(t *testing.T) {
	r := newTestRedis()

	resp := r.RPushX(cmd("RPUSHX", "missing", "a"))
	if !bytes.Equal(resp, []byte(":0\r\n")) {
		t.Fatalf("expected 0, got %q", resp)
	}

	// Verify key was not created
	length := r.LLen(cmd("LLEN", "missing"))
	if !bytes.Equal(length, []byte(":0\r\n")) {
		t.Fatalf("key should not exist")
	}
}

func TestRPushXWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.RPushX(cmd("RPUSHX", "k", "a"))
	if !bytes.Equal(resp, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY) {
		t.Fatalf("expected WRONGTYPE")
	}
}

func TestRPushXWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.RPushX(cmd("RPUSHX", "k"))
	if !bytes.HasPrefix(resp, []byte("-ERR")) {
		t.Fatal("expected ERR")
	}
}