package data_structure

import (
	"math"
	"math/bits"

	"github.com/DmitriyVTitov/size"
	"github.com/spaolacci/murmur3"
)

const (
	hllP                = 14                                   // Number of bits for register index
	hllM                = 1 << hllP                            // Number of registers (16384)
	hllAlpha            = 0.7213 / (1.0 + 1.079/float64(hllM)) // Bias correction constant
	hllMaxRegisterValue = 64 - hllP + 1                        // Maximum rarity value (51)
)

type HyperLogLog interface {
	PFAdd(items []string) int
	PFCount(hyperLogLogs []HyperLogLog) int
	PFMerge(hyperLogLogs []HyperLogLog)
	MemoryUsage() int64
}

type hyperLogLog struct {
	registers   []uint8
	cachedCount uint64 // Cached cardinality (8 bytes) - avoids recalculation
	dirty       bool   // True if registers changed since last count
}

func NewHyperLogLog() HyperLogLog {
	return &hyperLogLog{
		registers:   make([]uint8, hllM),
		cachedCount: 0,
		dirty:       true, // Start dirty to force initial calculation
	}
}

// PFAdd adds items to the HyperLogLog and returns 1 if any register was updated, 0 otherwise.
func (h *hyperLogLog) PFAdd(items []string) int {
	updated := 0
	for i := range items {
		updated += h.insert(items[i])
	}

	if updated > 0 {
		h.dirty = true // Invalidate cache when registers change
		return 1
	}

	return 0
}

// PFCount returns the estimated cardinality of the union of this HLL and the provided HLLs.
func (h *hyperLogLog) PFCount(hyperLogLogs []HyperLogLog) int {
	// Fast path: single HLL with valid cache
	if len(hyperLogLogs) == 0 && !h.dirty {
		return int(h.cachedCount)
	}

	// Need to calculate - either cache is dirty or we're merging multiple HLLs
	var registers []uint8

	if len(hyperLogLogs) == 0 {
		// Single HLL - use registers directly (no allocation needed)
		registers = h.registers
	} else {
		// Multiple HLLs - need to merge into temporary buffer
		registers = make([]uint8, hllM)
		copy(registers, h.registers)

		for _, hll := range hyperLogLogs {
			other := hll.(*hyperLogLog)
			for j := range hllM {
				if other.registers[j] > registers[j] {
					registers[j] = other.registers[j]
				}
			}
		}
	}

	// Calculate cardinality
	count := calculateCardinality(registers)

	// Cache result only for single HLL case
	if len(hyperLogLogs) == 0 {
		h.cachedCount = uint64(count)
		h.dirty = false
	}

	return count
}

// PFMerge merges the provided HyperLogLogs into this one.
func (h *hyperLogLog) PFMerge(hyperLogLogs []HyperLogLog) {
	for i := range hyperLogLogs {
		hll := hyperLogLogs[i].(*hyperLogLog)
		for j := range hllM {
			if hll.registers[j] > h.registers[j] {
				h.registers[j] = hll.registers[j]
			}
		}
	}
	h.dirty = true // Invalidate cache after merge
}

func calculateCardinality(registers []uint8) int {
	sum := float64(0)
	emptyRegisters := 0

	for i := range hllM {
		sum += math.Pow(2, -float64(registers[i]))
		if registers[i] == 0 {
			emptyRegisters++
		}
	}

	estimate := hllAlpha * float64(hllM) * float64(hllM) / sum

	// Small range correction using linear counting
	// When estimate is small and there are empty registers
	if estimate <= 2.5*float64(hllM) && emptyRegisters > 0 {
		estimate = float64(hllM) * math.Log(float64(hllM)/float64(emptyRegisters))
	}

	// Large range correction (for values approaching 2^32)
	const twoTo32 = float64(1 << 32)
	if estimate > twoTo32/30.0 {
		estimate = -twoTo32 * math.Log(1-estimate/twoTo32)
	}

	return int(estimate)
}

func (h *hyperLogLog) insert(item string) int {
	hash := murmur3.Sum64([]byte(item))

	registerIndex := hash >> (64 - hllP)
	remaining := hash << hllP

	var rarity uint8
	if remaining == 0 {
		rarity = hllMaxRegisterValue
	} else {
		rarity = uint8(bits.LeadingZeros64(remaining)) + 1
	}

	// Update register if new rarity is higher
	if rarity > h.registers[registerIndex] {
		h.registers[registerIndex] = rarity
		return 1
	}

	return 0
}

func (h *hyperLogLog) MemoryUsage() int64 {
	return int64(size.Of(h))
}
