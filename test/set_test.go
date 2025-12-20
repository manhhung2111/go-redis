package test

import (
	"bytes"
	"testing"

	"github.com/manhhung2111/go-redis/internal/constant"
)

func TestSAdd(t *testing.T) {
	r := newTestRedis()

	resp := r.SAdd(cmd("SADD", "k", "a", "b"))
	if !bytes.Equal(resp, []byte(":2\r\n")) {
		t.Fatalf("unexpected resp: %q", resp)
	}

	resp = r.SAdd(cmd("SADD", "k", "a"))
	if !bytes.Equal(resp, []byte(":0\r\n")) {
		t.Fatalf("expected 0 new members")
	}
}

func TestSAddWrongArgs(t *testing.T) {
	r := newTestRedis()
	resp := r.SAdd(cmd("SADD", "k"))
	if !bytes.HasPrefix(resp, []byte("-ERR")) {
		t.Fatal("expected ERR")
	}
}

func TestSCard(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "b"))

	resp := r.SCard(cmd("SCARD", "k"))
	if !bytes.Equal(resp, []byte(":2\r\n")) {
		t.Fatalf("unexpected resp: %q", resp)
	}
}

func TestSIsMember(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a"))

	resp := r.SIsMember(cmd("SISMEMBER", "k", "a"))
	if !bytes.Equal(resp, []byte(":1\r\n")) {
		t.Fatalf("expected 1, got %q", resp)
	}

	resp = r.SIsMember(cmd("SISMEMBER", "k", "b"))
	if !bytes.Equal(resp, []byte(":0\r\n")) {
		t.Fatalf("expected 0, got %q", resp)
	}
}

func TestSMembers(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "b"))

	resp := r.SMembers(cmd("SMEMBERS", "k"))
	if !bytes.HasPrefix(resp, []byte("*2\r\n")) {
		t.Fatalf("expected array resp, got %q", resp)
	}
}

func TestSMIsMember(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "c"))

	resp := r.SMIsMember(cmd("SMISMEMBER", "k", "a", "b", "c"))
	expected := "*3\r\n:1\r\n:0\r\n:1\r\n"

	if string(resp) != expected {
		t.Fatalf("expected %q, got %q", expected, resp)
	}
}

func TestSRem(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "b"))

	resp := r.SRem(cmd("SREM", "k", "a", "x"))
	if !bytes.Equal(resp, []byte(":1\r\n")) {
		t.Fatalf("expected 1 removed")
	}
}

func TestSPopSingle(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "b"))

	resp := r.SPop(cmd("SPOP", "k"))
	if !bytes.HasPrefix(resp, []byte("$")) {
		t.Fatalf("expected bulk string, got %q", resp)
	}
}

func TestSPopCount(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "b", "c"))

	resp := r.SPop(cmd("SPOP", "k", "2"))
	if !bytes.HasPrefix(resp, []byte("*2\r\n")) {
		t.Fatalf("expected array of 2, got %q", resp)
	}
}

func TestSPopNil(t *testing.T) {
	r := newTestRedis()
	resp := r.SPop(cmd("SPOP", "missing"))
	if !bytes.Equal(resp, constant.RESP_NIL_BULK_STRING) {
		t.Fatalf("expected nil bulk")
	}
}


func TestSRandMemberSingle(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "b"))

	resp := r.SRandMember(cmd("SRANDMEMBER", "k"))
	if !bytes.HasPrefix(resp, []byte("$")) {
		t.Fatalf("expected bulk string")
	}
}

func TestSRandMemberCountPositive(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "b", "c"))

	resp := r.SRandMember(cmd("SRANDMEMBER", "k", "2"))
	if !bytes.HasPrefix(resp, []byte("*2\r\n")) {
		t.Fatalf("expected array")
	}
}

func TestSRandMemberCountNegative(t *testing.T) {
	r := newTestRedis()
	r.SAdd(cmd("SADD", "k", "a", "b", "c"))

	resp := r.SRandMember(cmd("SRANDMEMBER", "k", "-10"))
	if !bytes.HasPrefix(resp, []byte("*10\r\n")) {
		t.Fatalf("expected array")
	}
}

func TestSetWrongType(t *testing.T) {
	r := newTestRedis()

	// simulate non-set key
	r.Set(cmd("SET", "k", "v"))

	resp := r.SAdd(cmd("SADD", "k", "a"))
	if !bytes.Equal(resp, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY) {
		t.Fatalf("expected WRONGTYPE")
	}
}