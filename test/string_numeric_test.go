package test

import (
	"math"
	"strconv"
	"testing"

	"github.com/manhhung2111/go-redis/internal/constant"
)

func TestIncr(t *testing.T) {
	r := newTestRedis()

	// INCR non-existing key -> 1
	resp := r.Incr(cmd("INCR", "a"))
	if string(resp) != ":1\r\n" {
		t.Fatalf("expected :1, got %q", resp)
	}

	// INCR existing numeric
	resp = r.Incr(cmd("INCR", "a"))
	if string(resp) != ":2\r\n" {
		t.Fatalf("expected :2, got %q", resp)
	}

	// INCR non-numeric
	r.Set(cmd("SET", "b", "foo"))
	resp = r.Incr(cmd("INCR", "b"))
	if string(resp) != string(constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE) {
		t.Fatalf("expected integer error, got %q", resp)
	}

	// INCR overflow
	r.Set(cmd("SET", "c", "9223372036854775807"))
	resp = r.Incr(cmd("INCR", "c"))
	if string(resp) != string(constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE) {
		t.Fatalf("expected overflow error, got %q", resp)
	}
}

func TestIncrBy(t *testing.T) {
	r := newTestRedis()

	// INCRBY non-existing key
	resp := r.IncrBy(cmd("INCRBY", "a", "5"))
	if string(resp) != ":5\r\n" {
		t.Fatalf("expected :5, got %q", resp)
	}

	// INCRBY existing
	resp = r.IncrBy(cmd("INCRBY", "a", "3"))
	if string(resp) != ":8\r\n" {
		t.Fatalf("expected :8, got %q", resp)
	}

	// INCRBY negative increment
	resp = r.IncrBy(cmd("INCRBY", "a", "-2"))
	if string(resp) != ":6\r\n" {
		t.Fatalf("expected :6, got %q", resp)
	}

	// INCRBY non-numeric value
	r.Set(cmd("SET", "b", "foo"))
	resp = r.IncrBy(cmd("INCRBY", "b", "1"))
	if string(resp) != string(constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE) {
		t.Fatalf("expected integer error, got %q", resp)
	}

	// INCRBY overflow
	r.Set(cmd("SET", "c", "9223372036854775807"))
	resp = r.IncrBy(cmd("INCRBY", "c", "1"))
	if string(resp) != string(constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE) {
		t.Fatalf("expected overflow error, got %q", resp)
	}
}

func TestDecr(t *testing.T) {
	r := newTestRedis()

	// DECR non-existing key -> -1
	resp := r.Decr(cmd("DECR", "a"))
	if string(resp) != ":-1\r\n" {
		t.Fatalf("expected :-1, got %q", resp)
	}

	// DECR existing
	resp = r.Decr(cmd("DECR", "a"))
	if string(resp) != ":-2\r\n" {
		t.Fatalf("expected :-2, got %q", resp)
	}

	// DECR non-numeric
	r.Set(cmd("SET", "b", "foo"))
	resp = r.Decr(cmd("DECR", "b"))
	if string(resp) != string(constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE) {
		t.Fatalf("expected integer error, got %q", resp)
	}

	// DECR underflow
	r.Set(cmd("SET", "c", "-9223372036854775808"))
	resp = r.Decr(cmd("DECR", "c"))
	if string(resp) != string(constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE) {
		t.Fatalf("expected underflow error, got %q", resp)
	}
}

func TestDecrBy(t *testing.T) {
	r := newTestRedis()

	// DECRBY non-existing
	resp := r.DecrBy(cmd("DECRBY", "a", "5"))
	if string(resp) != ":-5\r\n" {
		t.Fatalf("expected :-5, got %q", resp)
	}

	// DECRBY existing
	resp = r.DecrBy(cmd("DECRBY", "a", "3"))
	if string(resp) != ":-8\r\n" {
		t.Fatalf("expected :-8, got %q", resp)
	}

	// DECRBY negative decrement (acts like INCR)
	resp = r.DecrBy(cmd("DECRBY", "a", "-2"))
	if string(resp) != ":-6\r\n" {
		t.Fatalf("expected :-6, got %q", resp)
	}

	// DECRBY non-numeric value
	r.Set(cmd("SET", "b", "foo"))
	resp = r.DecrBy(cmd("DECRBY", "b", "1"))
	if string(resp) != string(constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE) {
		t.Fatalf("expected integer error, got %q", resp)
	}

	// DECRBY underflow
	r.Set(cmd("SET", "c", strconv.FormatInt(math.MinInt64, 10)))
	resp = r.DecrBy(cmd("DECRBY", "c", "1"))
	if string(resp) != string(constant.RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE) {
		t.Fatalf("expected underflow error, got %q", resp)
	}
}