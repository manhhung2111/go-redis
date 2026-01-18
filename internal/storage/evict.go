package storage

import (
	"math/rand/v2"
	"time"

	"github.com/manhhung2111/go-redis/internal/config"
)

// evictionPoolPopulate samples random keys and inserts them into the eviction pool
// sorted by idle time (ascending). The last entry has the highest idle time and is
// the best candidate for eviction.
func (s *store) evictionPoolPopulate() int {
	if config.EVICTION_POLICY == config.NoEviction {
		return 0
	}

	// Check if we can sample keys based on policy
	switch config.EVICTION_POLICY {
	case config.VolatileLRU, config.VolatileLFU, config.VolatileTTL:
		if s.expires.Empty() {
			return 0
		}
	case config.AllKeysLRU, config.AllKeysLFU:
		if s.data.Empty() {
			return 0
		}
	}

	inserted := 0
	for range config.MAXMEMORY_SAMPLES {
		// Sample a key based on eviction policy
		var key string
		switch config.EVICTION_POLICY {
		case config.AllKeysLRU, config.AllKeysLFU:
			key = s.data.GetRandomKey()
		case config.VolatileLRU, config.VolatileLFU, config.VolatileTTL:
			key = s.expires.GetRandomKey()
		default:
			continue
		}

		rObj, exists := s.data.Get(key)
		if !exists {
			continue
		}

		var idleTime uint32
		switch config.EVICTION_POLICY {
		case config.AllKeysLFU, config.VolatileLFU:
			// For LFU: lower counter = less frequently used = should evict first
			// Invert the counter so lower counter maps to higher idleTime
			counter := rObj.LFUDecay()
			idleTime = uint32(255 - counter)
		case config.VolatileTTL:
			expireAt, _ := s.expires.Get(key)
			nowMs := uint64(time.Now().UnixMilli())
			if expireAt <= nowMs {
				// Already expired - highest eviction priority
				idleTime = 1<<32 - 1
			} else {
				// Convert remaining TTL to seconds, cap at uint32 max
				remainingMs := expireAt - nowMs
				remainingSec := remainingMs / 1000
				if remainingSec > uint64(1<<32-2) {
					remainingSec = 1<<32 - 2
				}
				// Invert: smaller remaining TTL = higher idleTime = evict first
				idleTime = (1<<32 - 1) - uint32(remainingSec)
			}
		default:
			// LRU: use idle time directly
			idleTime = getIdleTime(rObj.lru)
		}

		// Skip if this key is already in the pool with same or higher idle time
		// or if it's not better than the worst candidate when pool is full
		if len(s.evictionPool) > 0 && len(s.evictionPool) >= config.EVICTION_POOL_SIZE {
			// Pool is full - only insert if this key has higher idle time than the worst (first) entry
			if idleTime <= s.evictionPool[0].idle {
				continue
			}
		}

		// Find insertion position using binary search (ascending order by idle time)
		low, high := 0, len(s.evictionPool)
		for low < high {
			mid := low + (high-low)/2
			if s.evictionPool[mid].idle < idleTime {
				low = mid + 1
			} else {
				high = mid
			}
		}

		// If pool is full, remove the first element (lowest idle time = worst candidate)
		if len(s.evictionPool) >= config.EVICTION_POOL_SIZE {
			s.evictionPool = s.evictionPool[1:]
			low-- // Adjust position after removing first element
			if low < 0 {
				low = 0
			}
		}

		// Insert at the found position
		s.evictionPool = append(s.evictionPool, nil) // Extend slice
		copy(s.evictionPool[low+1:], s.evictionPool[low:len(s.evictionPool)-1])
		s.evictionPool[low] = &evictionPoolEntry{
			key:  key,
			idle: idleTime,
		}
		inserted++
	}

	return inserted
}

// performEvictions evicts keys until memory usage is below MAXMEMORY_LIMIT.
func (s *store) performEvictions() {
	for s.usedMemory > config.MAXMEMORY_LIMIT {
		// Try to find a key to evict
		key := s.selectKeyToEvict()
		if key == nil {
			// No more keys can be evicted
			break
		}
		s.delete(*key)
	}
}

// selectKeyToEvict selects the best key to evict from the eviction pool.
// Returns nil if no suitable key is found.
func (s *store) selectKeyToEvict() *string {
	switch config.EVICTION_POLICY {
	case config.AllKeysRandom:
		if s.data.Empty() {
			return nil
		}
		key := s.data.GetRandomKey()
		return &key
	case config.VolatileRandom:
		if s.expires.Empty() {
			return nil
		}
		key := s.expires.GetRandomKey()
		return &key
	}

	// Keep trying until we find a valid key or exhaust options
	for {
		// If pool is empty or low, populate it
		if len(s.evictionPool) == 0 {
			populated := s.evictionPoolPopulate()
			if populated == 0 {
				return nil
			}
		}

		// Find the best candidate (last element has highest idle time)
		for len(s.evictionPool) > 0 {
			idx := len(s.evictionPool) - 1
			entry := s.evictionPool[idx]
			s.evictionPool = s.evictionPool[:idx] // Remove from pool

			// Verify the key still exists
			if _, exists := s.data.Get(entry.key); exists {
				return &entry.key
			}
			// Key was already deleted, try next
		}

		// Pool exhausted, try to populate again
		populated := s.evictionPoolPopulate()
		if populated == 0 {
			return nil
		}
	}
}

// getIdleTime calculates how long a key has been idle based on its last access time.
func getIdleTime(lruTime uint32) uint32 {
	now := uint32(time.Now().Unix())
	if now < lruTime {
		// Handle wrap-around (shouldn't happen in practice for decades)
		return 0
	}
	return now - lruTime
}

func LFULogIncr(counter uint8) uint8 {
	if counter == 255 {
		return 255
	}

	r := rand.Float64()
	baseval := max(0, int(counter)-int(config.LFU_INIT_VAL))
	p := 1.0 / (float64(baseval)*float64(config.LFU_LOG_FACTOR) + 1)
	if r < p {
		counter++
	}
	return counter
}

func (rObj *RObj) LFUDecay() uint8 {
	counter := uint8(rObj.lru & 0xFF)
	lastAccessTime := rObj.lru >> 8
	nowMinutes := uint32(time.Now().Unix()/60) & ((1 << 24) - 1)

	// Calculate elapsed time with wrap-around handling (24-bit minutes)
	var elapsedMinutes uint32
	if nowMinutes >= lastAccessTime {
		elapsedMinutes = nowMinutes - lastAccessTime
	} else {
		// Wrap-around case
		elapsedMinutes = (1 << 24) - lastAccessTime + nowMinutes
	}

	if elapsedMinutes > 0 && config.LFU_DECAY_TIME > 0 {
		decayAmount := elapsedMinutes / config.LFU_DECAY_TIME
		newCounter := max(0, int(counter)-int(decayAmount))
		counter = uint8(newCounter)
		rObj.lru = (nowMinutes << 8) | uint32(counter)
	}

	return counter
}
