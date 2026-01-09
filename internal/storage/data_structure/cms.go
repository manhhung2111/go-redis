package data_structure

import (
	"math"

	"github.com/spaolacci/murmur3"
)

type CountMinSketch interface {
	IncrBy(itemIncrementMap map[string]uint64) []uint64
	Info() []any
	Query(items []string) []uint64
}

type countMinSketch struct {
	grid       [][]uint64
	totalCount uint64
}

func NewCountMinSketchByDim(width, depth int) CountMinSketch {
	grid := make([][]uint64, depth)
	for i := range grid {
		grid[i] = make([]uint64, width)
	}

	return &countMinSketch{
		grid:       grid,
		totalCount: 0,
	}
}

func NewCountMinSketchByProb(errorRate, probability float64) CountMinSketch {
	width := int(math.Ceil(math.E / errorRate))
	depth := int(math.Ceil(math.Log(1 / probability)))

	grid := make([][]uint64, depth)
	for i := range grid {
		grid[i] = make([]uint64, width)
	}
	
	return &countMinSketch{
		grid:       grid,
		totalCount: 0,
	}
}

func (cms *countMinSketch) IncrBy(itemIncrementMap map[string]uint64) []uint64 {
	result := make([]uint64, 0, len(itemIncrementMap))

	for item, increment := range itemIncrementMap {
		cms.totalCount += increment
		indexes := cms.getIndexes(item)
		minCount := uint64(math.MaxUint64)

		for i := 0; i < len(cms.grid); i++ {
			cms.grid[i][indexes[i]] += increment
			minCount = min(minCount, cms.grid[i][indexes[i]])
		}

		result = append(result, minCount)
	}

	return result
}

func (cms *countMinSketch) Info() []any {
	return []any{
		"width", len(cms.grid[0]),
		"depth", len(cms.grid),
		"count", cms.totalCount,
	}
}

func (cms *countMinSketch) Query(items []string) []uint64 {
	result := make([]uint64, 0, len(items))

	for _, item := range items {
		indexes := cms.getIndexes(item)
		minCount := uint64(math.MaxUint64)

		for i := 0; i < len(cms.grid); i++ {
			minCount = min(minCount, cms.grid[i][indexes[i]])
		}

		result = append(result, minCount)
	}

	return result
}

func (cms *countMinSketch) getIndexes(item string) []uint64 {
	depth := uint64(len(cms.grid))
	width := uint64(len(cms.grid[0]))

	h1, h2 := murmur3.Sum128([]byte(item))
	indexes := make([]uint64, depth)

	for i := range depth {
		indexes[i] = (h1 + i*h2) % width
	}

	return indexes
}
