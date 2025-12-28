package data_structure

type ZSet struct {
	skipList *skipList
	data     map[string]float64
}

type ZAddOptions struct {
	XX bool
	NX bool
	LT bool
	GT bool
	CH bool
}

func NewZSet() *ZSet {
	return &ZSet{
		skipList: newSkipList(),
		data:     make(map[string]float64),
	}
}

func (zset *ZSet) ZAdd(scoreMember map[float64]string, options ZAddOptions) *uint32 {
	if options.NX && options.XX || options.GT && options.LT {
		return nil
	}

	count := 0
	if options.NX {
		count++
	}
	if options.GT {
		count++
	}
	if options.LT {
		count++
	}

	if count > 1 {
		return nil
	}

	result := uint32(0)
	for newScore, member := range scoreMember {
		oldScore, exists := zset.data[member];

		if options.NX && exists {
			continue
		}
		if options.XX && !exists {
			continue
		}

		if exists {
			if options.GT && newScore <= oldScore {
				continue
			}
			if options.LT && newScore >= oldScore {
				continue
			}

			if oldScore != newScore {
				zset.skipList.update(member, oldScore, newScore)
				zset.data[member] = newScore
				if options.CH {
					result++
				}
			}
			continue
		}

		zset.skipList.insert(member, newScore)
		zset.data[member] = newScore
		result++
	}

	return &result
}

func (zset *ZSet) ZCard() uint32 {
	return uint32(len(zset.data))
}

