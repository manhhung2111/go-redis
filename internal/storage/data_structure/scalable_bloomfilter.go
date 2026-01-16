package data_structure

import (
	"math"

	"github.com/DmitriyVTitov/size"
	"github.com/spaolacci/murmur3"
)

const (
	// defaultTighteningRatio is the factor by which error rate decreases for each new filter.
	// With r=0.5, total FPR converges to ≤ 2 * initialErrorRate
	defaultTighteningRatio = 0.5

	// defaultExpansionRate is the factor by which capacity increases for each new filter
	defaultExpansionRate = 2

	BloomFilterInfoAll       = 0
	BloomFilterInfoCapacity  = 1
	BloomFilterInfoSize      = 2
	BloomFilterInfoFilters   = 3
	BloomFilterInfoItems     = 4
	BloomFilterInfoExpansion = 5

	bitsPerWord = 64
)

// ScalableBloomFilter implements a dynamically growing bloom filter that maintains
// a target false positive rate by adding new sub-filters as capacity is reached.
// https://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf
type ScalableBloomFilter interface {
	Add(item string) int
	Card() int
	Exists(item string) int
	Info(option int) []any
	MAdd(items []string) []int
	MExists(items []string) []int
	MemoryUsage() int64
}

// subFilter represents a single bloom filter in the chain
type subFilter struct {
	k             uint64   // number of hash functions
	bits          []uint64 // bit vector
	numBits       uint64   // total bits in this filter
	capacity      uint64   // max items for this filter
	insertedItems uint64   // items inserted into this filter
	errorRate     float64  // target error rate for this filter
}

type scalableBloomFilter struct {
	filters         []*subFilter // chain of bloom filters
	initialCapacity uint64       // capacity of first filter
	initialErrRate  float64      // error rate of first filter
	expansionRate   int          // capacity multiplier for new filters
	tighteningRatio float64      // error rate multiplier for new filters (< 1)
	totalItems      uint64       // total items across all filters
}

// NewScalableBloomFilter creates a new scalable bloom filter.
// - errorRate: target false positive rate (e.g., 0.01 for 1%)
// - capacity: initial capacity before first expansion
// - expansionRate: capacity multiplier for each new sub-filter (typically 2)
func NewScalableBloomFilter(errorRate float64, capacity uint64, expansionRate int) ScalableBloomFilter {
	if expansionRate < 1 {
		expansionRate = defaultExpansionRate
	}

	sbf := &scalableBloomFilter{
		filters:         make([]*subFilter, 0, 4),
		initialCapacity: capacity,
		initialErrRate:  errorRate * defaultTighteningRatio, // Reserve room for growth
		expansionRate:   expansionRate,
		tighteningRatio: defaultTighteningRatio,
		totalItems:      0,
	}

	// Create the first sub-filter
	sbf.addNewFilter()

	return sbf
}

// addNewFilter creates and appends a new sub-filter to the chain
func (sbf *scalableBloomFilter) addNewFilter() {
	filterIndex := len(sbf.filters)

	// Calculate error rate for this filter: initialErr * r^filterIndex
	errRate := sbf.initialErrRate * math.Pow(sbf.tighteningRatio, float64(filterIndex))

	// Calculate capacity for this filter: initialCap * expansionRate^filterIndex
	capacity := sbf.initialCapacity * uint64(math.Pow(float64(sbf.expansionRate), float64(filterIndex)))

	// Calculate optimal number of bits: m = -n * ln(p) / (ln(2))^2
	numBits := uint64(math.Ceil(-float64(capacity) * math.Log(errRate) / (math.Ln2 * math.Ln2)))
	if numBits == 0 {
		numBits = 1
	}

	// Calculate optimal number of hash functions: k = (m/n) * ln(2)
	k := uint64(math.Ceil(float64(numBits) / float64(capacity) * math.Ln2))
	if k == 0 {
		k = 1
	}

	// Allocate bit vector
	numWords := (numBits + bitsPerWord - 1) / bitsPerWord

	filter := &subFilter{
		k:             k,
		bits:          make([]uint64, numWords),
		numBits:       numBits,
		capacity:      capacity,
		insertedItems: 0,
		errorRate:     errRate,
	}

	sbf.filters = append(sbf.filters, filter)
}

func (sbf *scalableBloomFilter) Add(item string) int {
	// First check if item already exists in any filter
	if sbf.Exists(item) == 1 {
		return 0
	}

	// Get the current (last) filter
	currentFilter := sbf.filters[len(sbf.filters)-1]

	// Check if current filter is at capacity
	if currentFilter.insertedItems >= currentFilter.capacity {
		sbf.addNewFilter()
		currentFilter = sbf.filters[len(sbf.filters)-1]
	}

	// Add to the current filter
	indexes := currentFilter.getSubFilterHashIndexes(item)
	for _, idx := range indexes {
		currentFilter.setSubFilterBit(idx)
	}

	currentFilter.insertedItems++
	sbf.totalItems++

	return 1
}

func (sbf *scalableBloomFilter) Card() int {
	return int(sbf.totalItems)
}

func (sbf *scalableBloomFilter) Exists(item string) int {
	for _, filter := range sbf.filters {
		if filter.existsInSubFilter(item) {
			return 1
		}
	}
	return 0
}

func (sbf *scalableBloomFilter) Info(option int) []any {
	totalCapacity := uint64(0)
	totalSize := uint64(0)
	for _, f := range sbf.filters {
		totalCapacity += f.capacity
		totalSize += f.numBits
	}

	switch option {
	case BloomFilterInfoCapacity:
		return []any{totalCapacity}
	case BloomFilterInfoSize:
		return []any{totalSize}
	case BloomFilterInfoFilters:
		return []any{len(sbf.filters)}
	case BloomFilterInfoItems:
		return []any{sbf.totalItems}
	case BloomFilterInfoExpansion:
		return []any{sbf.expansionRate}
	default:
		return []any{
			"Capacity", totalCapacity,
			"Size", totalSize,
			"Number of filters", len(sbf.filters),
			"Number of items inserted", sbf.totalItems,
			"Expansion rate", sbf.expansionRate,
		}
	}
}

func (sbf *scalableBloomFilter) MAdd(items []string) []int {
	result := make([]int, len(items))
	for i, item := range items {
		result[i] = sbf.Add(item)
	}
	return result
}

func (sbf *scalableBloomFilter) MExists(items []string) []int {
	result := make([]int, len(items))
	for i, item := range items {
		result[i] = sbf.Exists(item)
	}
	return result
}

func (sbf *scalableBloomFilter) MemoryUsage() int64 {
	return int64(size.Of(sbf))
}

func (f *subFilter) getSubFilterHashIndexes(item string) []uint64 {
	h1, h2 := murmur3.Sum128([]byte(item))
	hashes := make([]uint64, f.k)

	for i := uint64(0); i < f.k; i++ {
		hashes[i] = (h1 + i*h2) % f.numBits
	}
	return hashes
}

func (f *subFilter) setSubFilterBit(idx uint64) {
	wordIdx := idx / bitsPerWord
	bitIdx := idx % bitsPerWord
	f.bits[wordIdx] |= (1 << bitIdx)
}

func (f *subFilter) getSubFilterBit(idx uint64) bool {
	wordIdx := idx / bitsPerWord
	bitIdx := idx % bitsPerWord
	return (f.bits[wordIdx] & (1 << bitIdx)) != 0
}

func (f *subFilter) existsInSubFilter(item string) bool {
	indexes := f.getSubFilterHashIndexes(item)
	for _, idx := range indexes {
		if !f.getSubFilterBit(idx) {
			return false
		}
	}
	return true
}
