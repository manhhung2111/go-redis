package storage

import (
	"github.com/manhhung2111/go-redis/internal/storage/types"
)

func (s *store) PFAdd(key string, items []string) (int, error) {
	result := s.access(key, ObjHyperLogLog, true)
	if result.err != nil {
		return 0, result.err
	}

	if result.exists {
		hll := result.object.value.(types.HyperLogLog)

		if len(items) == 0 {
			return 0, nil
		}

		res, delta := hll.PFAdd(items)
		s.usedMemory += delta
		return res, nil
	}

	// Key doesn't exist - create new HyperLogLog
	hll := types.NewHyperLogLog()
	if len(items) > 0 {
		hll.PFAdd(items)
	}

	delta := s.data.Set(key, &RObj{
		objType:  ObjHyperLogLog,
		encoding: EncHyperLogLog,
		value:    hll,
	})
	s.usedMemory += delta

	return 1, nil
}

func (s *store) PFCount(keys []string) (int, error) {
	hlls := make([]types.HyperLogLog, 0, len(keys))

	for i := range keys {
		hll, err := s.getHyperLogLog(keys[i], false)
		if err != nil {
			return 0, err
		}
		if hll != nil {
			hlls = append(hlls, hll)
		}
	}

	if len(hlls) == 0 {
		return 0, nil
	}

	return hlls[0].PFCount(hlls[1:]), nil
}

func (s *store) PFMerge(destKey string, sourceKeys []string) error {
	destHll, err := s.getHyperLogLog(destKey, true)
	if err != nil {
		return err
	}

	if destHll == nil {
		destHll = types.NewHyperLogLog()
		delta := s.data.Set(destKey, &RObj{
			objType:  ObjHyperLogLog,
			encoding: EncHyperLogLog,
			value:    destHll,
		})
		s.usedMemory += delta
	}

	if len(sourceKeys) == 0 {
		return nil
	}

	sourceHlls := make([]types.HyperLogLog, 0, len(sourceKeys))
	for i := range sourceKeys {
		sourceHll, err := s.getHyperLogLog(sourceKeys[i], false)
		if err != nil {
			return err
		}
		if sourceHll != nil {
			sourceHlls = append(sourceHlls, sourceHll)
		}
	}

	if len(sourceHlls) > 0 {
		delta := destHll.PFMerge(sourceHlls)
		s.usedMemory += delta
	}

	return nil
}

func (s *store) getHyperLogLog(key string, isWrite bool) (types.HyperLogLog, error) {
	result := s.access(key, ObjHyperLogLog, isWrite)
	if result.err != nil {
		return nil, result.err
	}

	if result.expired || !result.exists {
		return nil, nil
	}

	hll := result.object.value.(types.HyperLogLog)
	return hll, nil
}
