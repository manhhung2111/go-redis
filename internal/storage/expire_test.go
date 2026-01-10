package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTTL_NonExistentKey(t *testing.T) {
	s := NewStore()

	ttl := s.TTL("nonexistent")

	assert.Equal(t, int64(-2), ttl)
}

func TestTTL_KeyWithoutExpire(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "value")

	ttl := s.TTL("mykey")

	assert.Equal(t, int64(-1), ttl)
}

func TestTTL_RemainingSeconds(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "value")
	s.Expire("mykey", 10, ExpireOptions{})

	ttl := s.TTL("mykey")

	assert.InDelta(t, 10, ttl, 1)
}

func TestTTL_DeletesExpiredKey(t *testing.T) {
	s := NewStore().(*store)
	s.Set("mykey", "value")
	s.expires["mykey"] = uint64(time.Now().UnixMilli() - 1000)

	ttl := s.TTL("mykey")

	assert.Equal(t, int64(-2), ttl)

	str, err := s.Get("mykey")
	assert.NoError(t, err)
	assert.Nil(t, str)
}

func TestTTL_AfterNaturalExpiration(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "value")
	s.Expire("mykey", 1, ExpireOptions{})

	time.Sleep(1100 * time.Millisecond)

	assert.Equal(t, int64(-2), s.TTL("mykey"))
}

func TestExpire_NonExistentKey(t *testing.T) {
	s := NewStore()

	ok := s.Expire("nonexistent", 10, ExpireOptions{})

	assert.False(t, ok)
}

func TestExpire_SetsExpiration(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "value")

	ok := s.Expire("mykey", 10, ExpireOptions{})
	require.True(t, ok)

	assert.InDelta(t, 10, s.TTL("mykey"), 1)
}

func TestExpire_UpdateExistingExpiration(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "value")
	s.Expire("mykey", 10, ExpireOptions{})

	ok := s.Expire("mykey", 20, ExpireOptions{})
	require.True(t, ok)

	assert.InDelta(t, 20, s.TTL("mykey"), 1)
}

func TestExpireNX_KeyWithoutExpire(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "value")

	ok := s.Expire("mykey", 10, ExpireOptions{NX: true})
	require.True(t, ok)

	assert.InDelta(t, 10, s.TTL("mykey"), 1)
}

func TestExpireNX_KeyWithExpire(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "value")
	s.Expire("mykey", 20, ExpireOptions{})

	ok := s.Expire("mykey", 10, ExpireOptions{NX: true})

	assert.False(t, ok)
	assert.InDelta(t, 20, s.TTL("mykey"), 1)
}

func TestExpireXX_KeyWithExpire(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "value")
	s.Expire("mykey", 20, ExpireOptions{})

	ok := s.Expire("mykey", 10, ExpireOptions{XX: true})
	require.True(t, ok)

	assert.InDelta(t, 10, s.TTL("mykey"), 1)
}

func TestExpireXX_KeyWithoutExpire(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "value")

	ok := s.Expire("mykey", 10, ExpireOptions{XX: true})

	assert.False(t, ok)
	assert.Equal(t, int64(-1), s.TTL("mykey"))
}

func TestExpireGT_NewTTLGreater(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "value")
	s.Expire("mykey", 10, ExpireOptions{})

	ok := s.Expire("mykey", 20, ExpireOptions{GT: true})
	require.True(t, ok)

	assert.InDelta(t, 20, s.TTL("mykey"), 1)
}

func TestExpireGT_NewTTLLessOrEqual(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "value")
	s.Expire("mykey", 20, ExpireOptions{})

	ok := s.Expire("mykey", 10, ExpireOptions{GT: true})

	assert.False(t, ok)
	assert.InDelta(t, 20, s.TTL("mykey"), 1)
}

func TestExpireLT_NewTTLLess(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "value")
	s.Expire("mykey", 20, ExpireOptions{})

	ok := s.Expire("mykey", 10, ExpireOptions{LT: true})
	require.True(t, ok)

	assert.InDelta(t, 10, s.TTL("mykey"), 1)
}

func TestExpire_AlreadyExpiredKey(t *testing.T) {
	s := NewStore().(*store)
	s.Set("mykey", "value")
	s.expires["mykey"] = uint64(time.Now().UnixMilli() - 1000)

	ok := s.Expire("mykey", 10, ExpireOptions{})

	assert.False(t, ok)

	str, err := s.Get("mykey")
	assert.NoError(t, err)
	assert.Nil(t, str)
}

func TestExpire_ZeroTTL(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "value")

	ok := s.Expire("mykey", 0, ExpireOptions{})
	require.True(t, ok)

	assert.LessOrEqual(t, s.TTL("mykey"), int64(1))
}

func TestExpire_NegativeTTL(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "value")

	ok := s.Expire("mykey", -5, ExpireOptions{})
	require.True(t, ok)

	assert.Equal(t, int64(-2), s.TTL("mykey"))
}

func TestExpireIntegration_ComplexScenario(t *testing.T) {
	s := NewStore()

	s.Set("mykey", "value")
	assert.Equal(t, int64(-1), s.TTL("mykey"))

	s.Expire("mykey", 30, ExpireOptions{})
	assert.InDelta(t, 30, s.TTL("mykey"), 1)

	assert.False(t, s.Expire("mykey", 10, ExpireOptions{GT: true}))
	assert.True(t, s.Expire("mykey", 60, ExpireOptions{GT: true}))
	assert.InDelta(t, 60, s.TTL("mykey"), 1)

	assert.True(t, s.Expire("mykey", 20, ExpireOptions{LT: true}))
	assert.InDelta(t, 20, s.TTL("mykey"), 1)

	assert.False(t, s.Expire("mykey", 100, ExpireOptions{NX: true}))

	str, err := s.Get("mykey")
	assert.NoError(t, err)
	assert.Equal(t, "value", *str)
}

func TestExpireIntegration_DifferentDataTypes(t *testing.T) {
	s := NewStore()

	s.Set("string_key", "value")
	s.Expire("string_key", 10, ExpireOptions{})
	assert.GreaterOrEqual(t, s.TTL("string_key"), int64(9))

	s.LPush("list_key", "a", "b", "c")
	s.Expire("list_key", 10, ExpireOptions{})
	assert.GreaterOrEqual(t, s.TTL("list_key"), int64(9))

	result, _ := s.LRange("list_key", 0, -1)
	assert.Len(t, result, 3)
}
