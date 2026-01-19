package storage

import (
	"time"
)

// ActiveExpireCycle runs one bounded expiration cycle
// Returns number of expired keys
func (s *store) ActiveExpireCycle() int {
	deadlineUs := time.Now().UnixMicro() + int64(s.config.ActiveExpireCycleTimeLimitUsage)
	totalExpired := 0

	for {
		if time.Now().UnixMicro() >= deadlineUs {
			break
		}

		sampled, expired := s.sampleAndExpire(s.config.ActiveExpireCycleKeysPerLoop)
		totalExpired += expired

		if sampled == 0 || expired*100/sampled < s.config.ActiveExpireCycleThresholdPercent {
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

	for sampled < sampleSize && s.expires.Len() > 0 {
		key := s.expires.GetRandomKey()

		expireAt, _ := s.expires.Get(key)
		if expireAt <= nowMs {
			expired++
			s.delete(key)
		}

		sampled++
	}

	return sampled, expired
}
