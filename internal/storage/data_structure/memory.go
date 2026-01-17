package data_structure

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
