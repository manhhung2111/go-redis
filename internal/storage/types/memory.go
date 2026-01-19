package types

// Memory size constants for delta calculation
// These are approximations based on Go's memory model (64-bit systems)
const (
	StringHeaderSize  int64 = 16 // string header: data pointer (8) + length (8)
	SliceHeaderSize   int64 = 24 // slice header: data pointer (8) + length (8) + capacity (8)
	MapOverheadPerKey int64 = 48 // estimated per-key overhead in Go maps (bucket overhead)
	PointerSize       int64 = 8
	Int64Size         int64 = 8
	Uint64Size        int64 = 8
	Float64Size       int64 = 8
	Uint32Size        int64 = 4
	Uint16Size        int64 = 2
	Uint8Size         int64 = 1
	EmptyStructSize   int64 = 0 // struct{}{} takes 0 bytes
)

// StringSize returns the memory used by a string (header + data)
func StringSize(s string) int64 {
	return StringHeaderSize + int64(len(s))
}

// StringMapEntrySize returns memory for a map[string]struct{} entry
func StringMapEntrySize(s string) int64 {
	return StringSize(s) + MapOverheadPerKey + EmptyStructSize
}

// StringStringMapEntrySize returns memory for a map[string]string entry
func StringStringMapEntrySize(key, value string) int64 {
	return StringSize(key) + StringSize(value) + MapOverheadPerKey
}

// StringFloat64MapEntrySize returns memory for a map[string]float64 entry
func StringFloat64MapEntrySize(key string) int64 {
	return StringSize(key) + Float64Size + MapOverheadPerKey
}

// SkipListNodeSize returns the estimated memory for a skip list node
// node struct: value (string header) + score (8) + backward pointer (8) + levels slice header (24)
// each level: forward pointer (8) + span (8) = 16 bytes
func SkipListNodeSize(value string, numLevels int) int64 {
	baseSize := StringSize(value) + Float64Size + PointerSize + SliceHeaderSize
	levelSize := int64(numLevels) * (PointerSize + Int64Size) // forward + span per level
	return baseSize + levelSize
}

// SkipListNodeSizeAvg returns the estimated memory for a skip list node with average levels
// Average level in skip list with p=0.25 is approximately 1.33
func SkipListNodeSizeAvg(value string) int64 {
	return SkipListNodeSize(value, 2) // Use 2 as a conservative estimate
}

// QuickListElementSize returns memory for a string element in a quicklist
func QuickListElementSize(element string) int64 {
	return StringHeaderSize + int64(len(element))
}

// BloomFilterBitsSize returns memory for bloom filter bit allocation
func BloomFilterBitsSize(numBits uint64) int64 {
	numWords := (numBits + 63) / 64 // 64 bits per uint64
	return int64(numWords) * Uint64Size
}

// CuckooFilterBucketSize returns memory for a cuckoo filter bucket
func CuckooFilterBucketSize(bucketSize uint64) int64 {
	return SliceHeaderSize + int64(bucketSize)*Uint16Size
}

// HyperLogLogRegisterDelta returns 0 as HLL registers are pre-allocated
func HyperLogLogRegisterDelta() int64 {
	return 0 // Registers are pre-allocated, no delta on insert
}
