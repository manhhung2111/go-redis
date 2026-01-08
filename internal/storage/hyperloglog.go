package storage

import (
	"errors"

	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
)

func (s *store) PFAdd(key string, items []string) int {
	s.expireIfNeeded(key)

	rObj, exists := s.data[key]
	if exists {
		hll, ok := rObj.Value.(data_structure.HyperLogLog)
		if !ok {
			panic("PFAdd operation called on object not type HyperLogLog")
		}

		if len(items) == 0 {
			return 0
		}

		return hll.PFAdd(items)
	}

	// Key doesn't exist - create new HyperLogLog
	hll := data_structure.NewHyperLogLog()
	if len(items) > 0 {
		hll.PFAdd(items)
	}

	s.data[key] = &RObj{
		Type:     ObjHyperLogLog,
		Encoding: EncHyperLogLog,
		Value:    hll,
	}

	return 1
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
	s.expireIfNeeded(destKey)

	destHll, err := s.getHyperLogLog(destKey)
	if err != nil {
		return err
	}

	if destHll == nil {
		destHll = data_structure.NewHyperLogLog()
		s.data[destKey] = &RObj{
			Type:     ObjHyperLogLog,
			Encoding: EncHyperLogLog,
			Value:    destHll,
		}
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
	if expired := s.expireIfNeeded(key); expired {
		return nil, nil
	}

	rObj, exists := s.data[key]
	if !exists {
		return nil, nil
	}

	hll, ok := rObj.Value.(data_structure.HyperLogLog)
	if !ok {
		return nil, errors.New("WRONGTYPE Key is not a valid HyperLogLog string value")
	}

	return hll, nil
}
