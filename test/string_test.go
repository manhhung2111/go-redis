package test

import (
	"testing"

	"github.com/manhhung2111/go-redis/internal/constant"
	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/util"
)


func TestGet(t *testing.T) {
	r := newTestRedis()

	// missing key
	resp := r.Get(cmd("GET", "a"))
	if string(resp) != string(constant.RESP_NIL_BULK_STRING) {
		t.Fatalf("expected nil bulk string")
	}

	// set + get
	r.Set(cmd("SET", "a", "hello"))
	resp = r.Get(cmd("GET", "a"))
	expected := core.EncodeResp("hello", false)

	if string(resp) != string(expected) {
		t.Fatalf("expected %q, got %q", expected, resp)
	}

	// non-existing key
	resp = r.Get(cmd("GET", "b"))
	if string(resp) != string(constant.RESP_NIL_BULK_STRING) {
		t.Fatalf("expected nil bulk string, got %q", resp)
	}
}

func TestSet(t *testing.T) {
	r := newTestRedis()

	resp := r.Set(cmd("SET", "a", "1"))
	if string(resp) != string(constant.RESP_OK) {
		t.Fatalf("SET failed")
	}

	// Test SET NX existing
	r.Set(cmd("SET", "foo", "bar"))
	resp = r.Set(cmd("SET", "foo", "baz", "NX"))

	if string(resp) != string(constant.RESP_NIL_BULK_STRING) {
		t.Fatalf("expected nil, got %q", resp)
	}

	// Test SET XX existing
	resp = r.Set(cmd("SET", "foo", "bar", "XX"))
	if string(resp) != string(constant.RESP_OK) {
		t.Fatalf("expected OK, got %q", resp)
	}
}

func TestDel(t *testing.T) {
	r := newTestRedis()

	r.Set(cmd("SET", "a", "1"))
	r.Set(cmd("SET", "b", "2"))

	resp := r.Del(cmd("DEL", "a", "b", "c"))
	expected := core.EncodeResp(int64(2), false)

	if string(resp) != string(expected) {
		t.Fatalf("expected %q, got %q", expected, resp)
	}
}

func TestTTL_NoKey(t *testing.T) {
	r := newTestRedis()

	resp := r.TTL(cmd("TTL", "missing"))
	if string(resp) != string(constant.RESP_TTL_KEY_NOT_EXIST) {
		t.Fatalf("unexpected response %q", resp)
	}
}

func TestTTL_NoExpire(t *testing.T) {
	r := newTestRedis()

	r.Set(cmd("SET", "foo", "bar"))
	resp := r.TTL(cmd("TTL", "foo"))

	if string(resp) != string(constant.RESP_TTL_KEY_EXIST_NO_EXPIRE) {
		t.Fatalf("unexpected response %q", resp)
	}
}

func TestTTL_WithExpire(t *testing.T) {
	r := newTestRedis()

	r.Set(cmd("SET", "foo", "bar", "EX", "2"))
	resp := r.TTL(cmd("TTL", "foo"))

	val, _, err := core.DecodeResp(resp)
	if err != nil {
		t.Fatal(err)
	}

	if ttl := val.(int64); ttl <= 0 || ttl > 2 {
		t.Fatalf("unexpected ttl %d", ttl)
	}
}

func TestExpire_InvalidArity(t *testing.T) {
	r := newTestRedis()

	resp := r.Expire(cmd("EXPIRE", "key"))
	if string(resp) != string(core.EncodeResp(util.InvalidNumberOfArgs("EXPIRE"), false)) {
		t.Fatalf("expected wrong arity error")
	}
}

func TestExpire_InvalidTTL(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	tests := [][]string{
		{"k", "abc"},
		{"k", "0"},
		{"k", "-10"},
	}

	for _, args := range tests {
		resp := r.Expire(cmd("EXPIRE", args...))
		if string(resp) != string(core.EncodeResp(util.InvalidExpireTime("EXPIRE"), false)) {
			t.Fatalf("expected invalid expire time for args=%v", args)
		}
	}
}

func TestExpire_InvalidOption(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.Expire(cmd("EXPIRE", "k", "10", "BAD"))
	if string(resp) != string(core.EncodeResp(util.InvalidCommandOption("BAD", "EXPIRE"), false)) {
		t.Fatalf("expected invalid option error")
	}
}

func TestExpire_IncompatibleOptions(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	cases := [][]string{
		{"k", "10", "NX", "XX"},
		{"k", "10", "GT", "LT"},
		{"k", "10", "NX", "GT"},
	}

	for _, args := range cases {
		resp := r.Expire(cmd("EXPIRE", args...))
		if string(resp) != string(constant.RESP_EXPIRE_OPTIONS_NOT_COMPATIBLE) {
			t.Fatalf("expected incompatible options for %v, got %q", args, resp)
		}
	}
}

func TestExpire_KeyNotExist(t *testing.T) {
	r := newTestRedis()

	resp := r.Expire(cmd("EXPIRE", "missing", "10"))
	if string(resp) != string(constant.RESP_EXPIRE_TIMEOUT_NOT_SET) {
		t.Fatalf("expected expire not set for missing key")
	}
}

func TestExpire_NX(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	// first time → OK
	resp := r.Expire(cmd("EXPIRE", "k", "10", "NX"))
	if string(resp) != string(constant.RESP_EXPIRE_TIMEOUT_SET) {
		t.Fatalf("expected expire set")
	}

	// already has TTL → reject
	resp = r.Expire(cmd("EXPIRE", "k", "20", "NX"))
	if string(resp) != string(constant.RESP_EXPIRE_TIMEOUT_NOT_SET) {
		t.Fatalf("expected NX reject")
	}
}

func TestExpire_XX(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	// no TTL yet → reject
	resp := r.Expire(cmd("EXPIRE", "k", "10", "XX"))
	if string(resp) != string(constant.RESP_EXPIRE_TIMEOUT_NOT_SET) {
		t.Fatalf("expected XX reject")
	}

	// set TTL
	r.Expire(cmd("EXPIRE", "k", "5"))

	// now OK
	resp = r.Expire(cmd("EXPIRE", "k", "10", "XX"))
	if string(resp) != string(constant.RESP_EXPIRE_TIMEOUT_SET) {
		t.Fatalf("expected XX success")
	}
}

func TestExpire_GT(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))
	r.Expire(cmd("EXPIRE", "k", "10"))

	resp := r.Expire(cmd("EXPIRE", "k", "5", "GT"))
	if string(resp) != string(constant.RESP_EXPIRE_TIMEOUT_NOT_SET) {
		t.Fatalf("expected GT reject")
	}

	resp = r.Expire(cmd("EXPIRE", "k", "20", "GT"))
	if string(resp) != string(constant.RESP_EXPIRE_TIMEOUT_SET) {
		t.Fatalf("expected GT accept")
	}
}

func TestExpire_LT(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))
	r.Expire(cmd("EXPIRE", "k", "10"))

	resp := r.Expire(cmd("EXPIRE", "k", "20", "LT"))
	if string(resp) != string(constant.RESP_EXPIRE_TIMEOUT_NOT_SET) {
		t.Fatalf("expected LT reject")
	}

	resp = r.Expire(cmd("EXPIRE", "k", "5", "LT"))
	if string(resp) != string(constant.RESP_EXPIRE_TIMEOUT_SET) {
		t.Fatalf("expected LT accept")
	}
}
