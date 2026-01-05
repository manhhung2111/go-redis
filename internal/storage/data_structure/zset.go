package data_structure

import (
	"math"
	"math/rand"
	"strconv"

	"github.com/manhhung2111/go-redis/internal/util"
)

type ZSet interface {
	ZAdd(scoreMember map[string]float64, options ZAddOptions) *uint32
	ZCard() uint32
	ZCount(minScore, maxScore float64) uint32
	ZIncrBy(member string, increment float64) (float64, bool)
	ZLexCount(minValue, maxValue string) uint32
	ZMScore(members []string) []*float64
	ZPopMax(count int) []string
	ZPopMin(count int) []string
	ZRandMember(count int, withScores bool) []string
	ZRangeByRank(start, stop int, withScores bool) []string
	ZRangeByLex(start, stop string, withScores bool) []string
	ZRangeByScore(start, stop float64, withScores bool) []string
	ZRevRangeByRank(start, stop int, withScores bool) []string
	ZRevRangeByLex(start, stop string, withScores bool) []string
	ZRevRangeByScore(start, stop float64, withScores bool) []string
	ZRank(member string, withScore bool) []any
	ZRem(members []string) int
	ZRevRank(member string, withScore bool) []any
	ZScore(member string) *float64

	// Geo commands (stored as ZSet with geohash as score)
	GeoAdd(items []GeoPoint, options ZAddOptions) *uint32
	GeoDist(member1, member2 string, unit string) *float64
	GeoHash(members []string) []*string
	GeoPos(members []string) []*GeoPoint
	GeoSearch(options GeoSearchOptions) []GeoResult
}

type zSet struct {
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

func NewZSet() ZSet {
	return &zSet{
		skipList: newSkipList(),
		data:     make(map[string]float64),
	}
}

func (zset *zSet) ZAdd(scoreMember map[string]float64, options ZAddOptions) *uint32 {
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
	for member, newScore := range scoreMember {
		oldScore, exists := zset.data[member]

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

func (zset *zSet) ZCard() uint32 {
	return uint32(len(zset.data))
}

func (zset *zSet) ZCount(minScore, maxScore float64) uint32 {
	return uint32(zset.skipList.countByScore(minScore, maxScore))
}

func (zset *zSet) ZIncrBy(member string, increment float64) (float64, bool) {
	score, exists := zset.data[member]
	if !exists {
		zset.skipList.insert(member, increment)
		zset.data[member] = increment
		return increment, true
	}

	newScore := score + increment
	if math.IsInf(newScore, 0) || math.IsNaN(newScore) {
		return 0, false
	}

	zset.skipList.update(member, score, newScore)
	zset.data[member] = newScore

	return newScore, true
}

func (zset *zSet) ZLexCount(minValue, maxValue string) uint32 {
	return uint32(zset.skipList.countByLex(minValue, maxValue))
}

func (zset *zSet) ZMScore(members []string) []*float64 {
	result := make([]*float64, len(members))
	for i := 0; i < len(members); i++ {
		if score, exists := zset.data[members[i]]; exists {
			result[i] = &score
		} else {
			result[i] = nil
		}
	}

	return result
}

func (zset *zSet) ZPopMax(count int) []string {
	poppedNodes := zset.skipList.popMax(count)
	if poppedNodes == nil {
		return []string{}
	}

	result := make([]string, 0, len(poppedNodes)*2)
	for i := range poppedNodes {
		delete(zset.data, poppedNodes[i].value)
		result = append(result, poppedNodes[i].value)
		result = append(result, formatFloat(poppedNodes[i].score))
	}

	return result
}

func (zset *zSet) ZPopMin(count int) []string {
	poppedNodes := zset.skipList.popMin(count)
	if poppedNodes == nil {
		return []string{}
	}

	result := make([]string, 0, len(poppedNodes)*2)
	for i := range poppedNodes {
		delete(zset.data, poppedNodes[i].value)
		result = append(result, poppedNodes[i].value)
		result = append(result, formatFloat(poppedNodes[i].score))
	}

	return result
}

func (zset *zSet) ZRandMember(count int, withScores bool) []string {
	if count == 0 {
		return []string{}
	}

	if count > 0 {
		arrLen := min(count, len(zset.data))
		if withScores {
			arrLen *= 2
		}

		result := make([]string, 0, arrLen)
		if count >= len(zset.data) {
			for member, score := range zset.data {
				result = append(result, member)
				if withScores {
					result = append(result, formatFloat(score))
				}
			}
		} else {
			indices := util.FloydSamplingIndices(len(zset.data), count)
			i := 0
			for member, score := range zset.data {
				if _, selected := indices[i]; selected {
					result = append(result, member)
					if withScores {
						result = append(result, formatFloat(score))
					}
				}
				i++
			}
		}

		return result
	}

	members := make([]string, 0, len(zset.data))
	for m := range zset.data {
		members = append(members, m)
	}

	arrLen := -count
	if withScores {
		arrLen *= 2
	}

	result := make([]string, 0, arrLen)
	for i := 0; i < -count; i++ {
		m := members[rand.Intn(len(members))]
		result = append(result, m)

		if withScores {
			result = append(result, formatFloat(zset.data[m]))
		}
	}

	return result
}

func (zset *zSet) ZRangeByRank(start, stop int, withScores bool) []string {
	nodes := zset.skipList.getRangeByRank(start, stop)
	return zset.nodesToStringSlice(nodes, withScores)
}

func (zset *zSet) ZRangeByLex(start, stop string, withScores bool) []string {
	nodes := zset.skipList.getRangeByLex(start, stop)
	return zset.nodesToStringSlice(nodes, withScores)
}

func (zset *zSet) ZRangeByScore(start, stop float64, withScores bool) []string {
	nodes := zset.skipList.getRangeByScore(start, stop)
	return zset.nodesToStringSlice(nodes, withScores)
}

func (zset *zSet) ZRevRangeByRank(start, stop int, withScores bool) []string {
	nodes := zset.skipList.getRevRangeByRank(start, stop)
	return zset.nodesToStringSlice(nodes, withScores)
}

func (zset *zSet) ZRevRangeByLex(start, stop string, withScores bool) []string {
	nodes := zset.skipList.getRevRangeByLex(start, stop)
	return zset.nodesToStringSlice(nodes, withScores)
}

func (zset *zSet) ZRevRangeByScore(start, stop float64, withScores bool) []string {
	nodes := zset.skipList.getRevRangeByScore(start, stop)
	return zset.nodesToStringSlice(nodes, withScores)
}

func (zset *zSet) ZRank(member string, withScore bool) []any {
	score, exists := zset.data[member]
	if !exists {
		return nil
	}

	rank := zset.skipList.getRank(member, score)
	if rank == -1 {
		return nil
	}

	if withScore {
		return []any{rank, formatFloat(score)}
	}
	return []any{rank}
}

func (zset *zSet) ZRem(members []string) int {
	removed := 0

	for _, member := range members {
		if score, exists := zset.data[member]; exists {
			// Delete from skipList first - if this fails, data is unchanged
			if zset.skipList.delete(member, score) {
				delete(zset.data, member)
				removed++
			}
		}
	}

	return removed
}

func (zset *zSet) ZRevRank(member string, withScore bool) []any {
	score, exists := zset.data[member]
	if !exists {
		return nil
	}

	forwardRank := zset.skipList.getRank(member, score)
	if forwardRank == -1 {
		return nil
	}

	rank := zset.skipList.length - 1 - forwardRank

	if withScore {
		return []any{rank, formatFloat(score)}
	}
	return []any{rank}
}

func (zset *zSet) ZScore(member string) *float64 {
	score, exists := zset.data[member]
	if !exists {
		return nil
	}

	return &score
}

func (zset *zSet) nodesToStringSlice(nodes []*skipListNode, withScores bool) []string {
	nodeCount := len(nodes)
	if nodeCount == 0 {
		return []string{}
	}

	if withScores {
		result := make([]string, nodeCount*2)
		for i, node := range nodes {
			idx := i * 2
			result[idx] = node.value
			result[idx+1] = formatFloat(node.score)
		}
		return result
	}

	result := make([]string, nodeCount)
	for i, node := range nodes {
		result[i] = node.value
	}
	return result
}

func formatFloat(num float64) string {
	return strconv.FormatFloat(num, 'g', -1, 64)
}
