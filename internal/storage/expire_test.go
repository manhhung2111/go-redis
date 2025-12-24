package storage

import (
	"testing"
	"time"
)

func TestTTL(t *testing.T) {
	t.Run("TTL on non-existent key returns -2", func(t *testing.T) {
		s := NewStore()
		ttl := s.TTL("nonexistent")
		if ttl != -2 {
			t.Errorf("expected -2 for non-existent key, got %d", ttl)
		}
	})

	t.Run("TTL on key without expire returns -1", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		
		ttl := s.TTL("mykey")
		if ttl != -1 {
			t.Errorf("expected -1 for key without expire, got %d", ttl)
		}
	})

	t.Run("TTL returns remaining seconds", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		s.Expire("mykey", 10, ExpireOptions{})
		
		ttl := s.TTL("mykey")
		// TTL should be around 9-10 seconds
		if ttl < 9 || ttl > 10 {
			t.Errorf("expected TTL around 9-10, got %d", ttl)
		}
	})

	t.Run("TTL deletes expired key and returns -2", func(t *testing.T) {
		s := NewStore().(*store)
		s.Set("mykey", "value")
		// Set expire in the past
		s.expires["mykey"] = uint64(time.Now().UnixMilli() - 1000)
		
		ttl := s.TTL("mykey")
		if ttl != -2 {
			t.Errorf("expected -2 for expired key, got %d", ttl)
		}

		// Verify key was deleted
		_, exists := s.Get("mykey")
		if exists {
			t.Error("expected expired key to be deleted")
		}
	})

	t.Run("TTL after key expires naturally", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		s.Expire("mykey", 1, ExpireOptions{})
		
		// Wait for expiration
		time.Sleep(1100 * time.Millisecond)
		
		ttl := s.TTL("mykey")
		if ttl != -2 {
			t.Errorf("expected -2 after expiration, got %d", ttl)
		}
	})
}

func TestExpire(t *testing.T) {
	t.Run("expire on non-existent key returns false", func(t *testing.T) {
		s := NewStore()
		ok := s.Expire("nonexistent", 10, ExpireOptions{})
		if ok {
			t.Error("expected false when setting expire on non-existent key")
		}
	})

	t.Run("expire sets expiration successfully", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		
		ok := s.Expire("mykey", 10, ExpireOptions{})
		if !ok {
			t.Error("expected true when setting expire")
		}

		ttl := s.TTL("mykey")
		if ttl < 9 || ttl > 10 {
			t.Errorf("expected TTL around 9-10, got %d", ttl)
		}
	})

	t.Run("expire can update existing expiration", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		s.Expire("mykey", 10, ExpireOptions{})
		
		ok := s.Expire("mykey", 20, ExpireOptions{})
		if !ok {
			t.Error("expected true when updating expire")
		}

		ttl := s.TTL("mykey")
		if ttl < 19 || ttl > 20 {
			t.Errorf("expected TTL around 19-20, got %d", ttl)
		}
	})
}

func TestExpireNX(t *testing.T) {
	t.Run("NX sets expire only if key has no expire", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		
		ok := s.Expire("mykey", 10, ExpireOptions{NX: true})
		if !ok {
			t.Error("expected true when setting expire with NX on key without expire")
		}

		ttl := s.TTL("mykey")
		if ttl < 9 || ttl > 10 {
			t.Errorf("expected TTL around 9-10, got %d", ttl)
		}
	})

	t.Run("NX returns false if key already has expire", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		s.Expire("mykey", 20, ExpireOptions{})
		
		ok := s.Expire("mykey", 10, ExpireOptions{NX: true})
		if ok {
			t.Error("expected false when setting expire with NX on key with existing expire")
		}

		// Original expire should remain
		ttl := s.TTL("mykey")
		if ttl < 19 || ttl > 20 {
			t.Errorf("expected TTL to remain around 19-20, got %d", ttl)
		}
	})
}

func TestExpireXX(t *testing.T) {
	t.Run("XX sets expire only if key has expire", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		s.Expire("mykey", 20, ExpireOptions{})
		
		ok := s.Expire("mykey", 10, ExpireOptions{XX: true})
		if !ok {
			t.Error("expected true when setting expire with XX on key with expire")
		}

		ttl := s.TTL("mykey")
		if ttl < 9 || ttl > 10 {
			t.Errorf("expected TTL around 9-10, got %d", ttl)
		}
	})

	t.Run("XX returns false if key has no expire", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		
		ok := s.Expire("mykey", 10, ExpireOptions{XX: true})
		if ok {
			t.Error("expected false when setting expire with XX on key without expire")
		}

		// Key should still have no expire
		ttl := s.TTL("mykey")
		if ttl != -1 {
			t.Errorf("expected -1 (no expire), got %d", ttl)
		}
	})
}

func TestExpireGT(t *testing.T) {
	t.Run("GT sets expire only if new TTL is greater", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		s.Expire("mykey", 10, ExpireOptions{})
		
		ok := s.Expire("mykey", 20, ExpireOptions{GT: true})
		if !ok {
			t.Error("expected true when new TTL is greater")
		}

		ttl := s.TTL("mykey")
		if ttl < 19 || ttl > 20 {
			t.Errorf("expected TTL around 19-20, got %d", ttl)
		}
	})

	t.Run("GT returns false if new TTL is less than or equal", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		s.Expire("mykey", 20, ExpireOptions{})
		
		// Try to set lower TTL
		ok := s.Expire("mykey", 10, ExpireOptions{GT: true})
		if ok {
			t.Error("expected false when new TTL is less")
		}

		// Original expire should remain
		ttl := s.TTL("mykey")
		if ttl < 19 || ttl > 20 {
			t.Errorf("expected TTL to remain around 19-20, got %d", ttl)
		}
	})

	t.Run("GT returns false if new TTL equals old TTL", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		s.Expire("mykey", 10, ExpireOptions{})
		
		// Try to set same TTL
		ok := s.Expire("mykey", 10, ExpireOptions{GT: true})
		if ok {
			t.Error("expected false when new TTL equals old TTL")
		}
	})

	t.Run("GT ignores condition if key has no expire", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		
		ok := s.Expire("mykey", 10, ExpireOptions{GT: true})
		if !ok {
			t.Error("expected true when GT is set but key has no prior expire")
		}

		ttl := s.TTL("mykey")
		if ttl < 9 || ttl > 10 {
			t.Errorf("expected TTL around 9-10, got %d", ttl)
		}
	})
}

func TestExpireLT(t *testing.T) {
	t.Run("LT sets expire only if new TTL is less", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		s.Expire("mykey", 20, ExpireOptions{})
		
		ok := s.Expire("mykey", 10, ExpireOptions{LT: true})
		if !ok {
			t.Error("expected true when new TTL is less")
		}

		ttl := s.TTL("mykey")
		if ttl < 9 || ttl > 10 {
			t.Errorf("expected TTL around 9-10, got %d", ttl)
		}
	})

	t.Run("LT returns false if new TTL is greater than or equal", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		s.Expire("mykey", 10, ExpireOptions{})
		
		// Try to set higher TTL
		ok := s.Expire("mykey", 20, ExpireOptions{LT: true})
		if ok {
			t.Error("expected false when new TTL is greater")
		}

		// Original expire should remain
		ttl := s.TTL("mykey")
		if ttl < 9 || ttl > 10 {
			t.Errorf("expected TTL to remain around 9-10, got %d", ttl)
		}
	})

	t.Run("LT returns false if new TTL equals old TTL", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		s.Expire("mykey", 10, ExpireOptions{})
		
		// Try to set same TTL
		ok := s.Expire("mykey", 10, ExpireOptions{LT: true})
		if ok {
			t.Error("expected false when new TTL equals old TTL")
		}
	})

	t.Run("LT ignores condition if key has no expire", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		
		ok := s.Expire("mykey", 10, ExpireOptions{LT: true})
		if !ok {
			t.Error("expected true when LT is set but key has no prior expire")
		}

		ttl := s.TTL("mykey")
		if ttl < 9 || ttl > 10 {
			t.Errorf("expected TTL around 9-10, got %d", ttl)
		}
	})
}

func TestExpireAlreadyExpired(t *testing.T) {
	t.Run("expire returns false if key already expired", func(t *testing.T) {
		s := NewStore().(*store)
		s.Set("mykey", "value")
		// Set expire in the past
		s.expires["mykey"] = uint64(time.Now().UnixMilli() - 1000)
		
		ok := s.Expire("mykey", 10, ExpireOptions{})
		if ok {
			t.Error("expected false when trying to set expire on already expired key")
		}

		// Verify key was deleted
		_, exists := s.Get("mykey")
		if exists {
			t.Error("expected expired key to be deleted")
		}
	})

	t.Run("expire with GT on already expired key returns false", func(t *testing.T) {
		s := NewStore().(*store)
		s.Set("mykey", "value")
		s.expires["mykey"] = uint64(time.Now().UnixMilli() - 1000)
		
		ok := s.Expire("mykey", 10, ExpireOptions{GT: true})
		if ok {
			t.Error("expected false for expired key with GT")
		}
	})

	t.Run("expire with LT on already expired key returns false", func(t *testing.T) {
		s := NewStore().(*store)
		s.Set("mykey", "value")
		s.expires["mykey"] = uint64(time.Now().UnixMilli() - 1000)
		
		ok := s.Expire("mykey", 10, ExpireOptions{LT: true})
		if ok {
			t.Error("expected false for expired key with LT")
		}
	})
}

func TestExpireCombinedOptions(t *testing.T) {
	t.Run("multiple options do not conflict - NX and GT", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		
		// NX should take precedence - sets only if no expire
		ok := s.Expire("mykey", 10, ExpireOptions{NX: true, GT: true})
		if !ok {
			t.Error("expected true with NX on key without expire")
		}
	})

	t.Run("multiple options - XX and LT", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		s.Expire("mykey", 20, ExpireOptions{})
		
		// XX requires existing expire, LT requires new TTL < old TTL
		ok := s.Expire("mykey", 10, ExpireOptions{XX: true, LT: true})
		if !ok {
			t.Error("expected true with XX and LT when conditions met")
		}

		ttl := s.TTL("mykey")
		if ttl < 9 || ttl > 10 {
			t.Errorf("expected TTL around 9-10, got %d", ttl)
		}
	})
}

func TestExpireEdgeCases(t *testing.T) {
	t.Run("expire with zero TTL", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		
		ok := s.Expire("mykey", 0, ExpireOptions{})
		if !ok {
			t.Error("expected true when setting expire with 0 TTL")
		}

		// Key should expire immediately or very soon
		ttl := s.TTL("mykey")
		if ttl > 1 {
			t.Errorf("expected TTL <= 1, got %d", ttl)
		}
	})

	t.Run("expire with negative TTL", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		
		ok := s.Expire("mykey", -5, ExpireOptions{})
		if !ok {
			t.Error("expected true when setting negative TTL")
		}

		// Key should be considered expired
		ttl := s.TTL("mykey")
		if ttl != -2 {
			t.Errorf("expected -2 for negative TTL key, got %d", ttl)
		}
	})

	t.Run("expire with very large TTL", func(t *testing.T) {
		s := NewStore()
		s.Set("mykey", "value")
		
		largeTTL := int64(86400 * 365) // 1 year in seconds
		ok := s.Expire("mykey", largeTTL, ExpireOptions{})
		if !ok {
			t.Error("expected true when setting large TTL")
		}

		ttl := s.TTL("mykey")
		// Should be close to the original value
		if ttl < largeTTL-2 || ttl > largeTTL {
			t.Errorf("expected TTL around %d, got %d", largeTTL, ttl)
		}
	})
}

// Integration test combining TTL and Expire operations
func TestExpireIntegration(t *testing.T) {
	t.Run("complex expire scenario", func(t *testing.T) {
		s := NewStore()
		
		// Create key
		s.Set("mykey", "value")
		if s.TTL("mykey") != -1 {
			t.Error("expected no expiration initially")
		}

		// Set initial expiration
		s.Expire("mykey", 30, ExpireOptions{})
		ttl1 := s.TTL("mykey")
		if ttl1 < 29 || ttl1 > 30 {
			t.Errorf("expected TTL around 29-30, got %d", ttl1)
		}

		// Try to set lower with GT (should fail)
		ok := s.Expire("mykey", 10, ExpireOptions{GT: true})
		if ok {
			t.Error("GT should reject lower TTL")
		}

		// Set higher with GT (should succeed)
		ok = s.Expire("mykey", 60, ExpireOptions{GT: true})
		if !ok {
			t.Error("GT should accept higher TTL")
		}
		ttl2 := s.TTL("mykey")
		if ttl2 < 59 || ttl2 > 60 {
			t.Errorf("expected TTL around 59-60, got %d", ttl2)
		}

		// Set lower with LT (should succeed)
		ok = s.Expire("mykey", 20, ExpireOptions{LT: true})
		if !ok {
			t.Error("LT should accept lower TTL")
		}
		ttl3 := s.TTL("mykey")
		if ttl3 < 19 || ttl3 > 20 {
			t.Errorf("expected TTL around 19-20, got %d", ttl3)
		}

		// Try to set with NX (should fail as key has expire)
		ok = s.Expire("mykey", 100, ExpireOptions{NX: true})
		if ok {
			t.Error("NX should fail when key has expire")
		}

		// Verify value is still accessible
		rObj, exists := s.Get("mykey")
		if !exists || rObj.Value != "value" {
			t.Error("key value should still be accessible")
		}
	})

	t.Run("expire on different data types", func(t *testing.T) {
		s := NewStore()
		
		// String
		s.Set("string_key", "value")
		s.Expire("string_key", 10, ExpireOptions{})
		if s.TTL("string_key") < 9 {
			t.Error("string key should have expiration")
		}

		// List
		s.LPush("list_key", "a", "b", "c")
		s.Expire("list_key", 10, ExpireOptions{})
		if s.TTL("list_key") < 9 {
			t.Error("list key should have expiration")
		}

		// Verify operations still work before expiration
		result := s.LRange("list_key", 0, -1)
		if len(result) != 3 {
			t.Error("list operations should work before expiration")
		}
	})
}