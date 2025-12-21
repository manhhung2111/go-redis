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