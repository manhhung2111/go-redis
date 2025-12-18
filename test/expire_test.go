package test

import (
	"testing"

	"github.com/manhhung2111/go-redis/internal/constant"
	"github.com/manhhung2111/go-redis/internal/core"
	"github.com/manhhung2111/go-redis/internal/util"
)

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
