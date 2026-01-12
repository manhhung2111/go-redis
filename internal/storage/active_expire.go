package storage

import (
	"math/rand/v2"
	"time"

	"github.com/manhhung2111/go-redis/internal/config"
)

// ActiveExpireCycle runs one bounded expiration cycle
// Returns number of expired keys
func (s *store) ActiveExpireCycle() int {
	deadlineUs := time.Now().UnixMicro() + int64(config.ACTIVE_EXPIRE_CYCLE_TIME_LIMIT_USAGE)
	totalExpired := 0

	for {
		if time.Now().UnixMicro() >= deadlineUs {
			break
		}

		sampled, expired := s.sampleAndExpire(config.ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP)
		totalExpired += expired

		if sampled == 0 || expired*100/sampled < config.ACTIVE_EXPIRE_CYCLE_THRESHOLD_PERCENT {
			break
		}
	}

	return totalExpired
}

// sampleAndExpire samples up to N keys and deletes expired ones
// Returns (sampled count, expired count)
func (s *store) sampleAndExpire(sampleSize int) (int, int) {
	nowMs := uint64(time.Now().UnixMilli())
	expired := 0
	sampled := 0

	for sampled < sampleSize && len(s.expireKeys) > 0 {
		randomIdx := rand.IntN(len(s.expireKeys))
		key := s.expireKeys[randomIdx]

		expireAt := s.expires[key]
		if expireAt <= nowMs {
			expired++
			s.delete(key)
		}

		sampled++
	}

	return sampled, expired
}
