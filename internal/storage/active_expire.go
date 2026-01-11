package storage

import (
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

		if sampled == 0 {
			break
		}

		if expired*100/sampled < config.ACTIVE_EXPIRE_CYCLE_THRESHOLD_PERCENT {
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
	for key, expireAt := range s.expires {
		if sampled >= sampleSize {
			break
		}
		sampled++

		if expireAt <= nowMs {
			expired++
			s.delete(key)
		}
	}

	return sampled, expired
}
