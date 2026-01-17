package storage

import (
	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
)

func (s *store) PFAdd(key string, items []string) (int, error) {
	result := s.access(key, ObjHyperLogLog)
	if result.typeErr != nil {
		return 0, result.typeErr
	}

	if result.exists {
		hll := result.object.value.(data_structure.HyperLogLog)

		if len(items) == 0 {
			return 0, nil
		}

		res, _ := hll.PFAdd(items)
		return res, nil
	}

	// Key doesn't exist - create new HyperLogLog
	hll := data_structure.NewHyperLogLog()
	if len(items) > 0 {
		hll.PFAdd(items)
	}

	s.data.Set(key, &RObj{
		objType:     ObjHyperLogLog,
		encoding: EncHyperLogLog,
		value:    hll,
	})

	return 1, nil
}

func (s *store) PFCount(keys []string) (int, error) {
	hlls := make([]data_structure.HyperLogLog, 0, len(keys))

	for i := range keys {
		hll, err := s.getHyperLogLog(keys[i])
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
	destHll, err := s.getHyperLogLog(destKey)
	if err != nil {
		return err
	}

	if destHll == nil {
		destHll = data_structure.NewHyperLogLog()
		s.data.Set(destKey, &RObj{
			objType:     ObjHyperLogLog,
			encoding: EncHyperLogLog,
			value:    destHll,
		})
	}

	if len(sourceKeys) == 0 {
		return nil
	}

	sourceHlls := make([]data_structure.HyperLogLog, 0, len(sourceKeys))
	for i := range sourceKeys {
		sourceHll, err := s.getHyperLogLog(sourceKeys[i])
		if err != nil {
			return err
		}
		if sourceHll != nil {
			sourceHlls = append(sourceHlls, sourceHll)
		}
	}

	if len(sourceHlls) > 0 {
		destHll.PFMerge(sourceHlls)
	}

	return nil
}

func (s *store) getHyperLogLog(key string) (data_structure.HyperLogLog, error) {
	result := s.access(key, ObjHyperLogLog)
	if result.typeErr != nil {
		return nil, result.typeErr
	}

	if result.expired || !result.exists {
		return nil, nil
	}

	hll := result.object.value.(data_structure.HyperLogLog)
	return hll, nil
}
