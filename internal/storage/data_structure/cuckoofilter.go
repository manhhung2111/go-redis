package data_structure

import (
	"github.com/spaolacci/murmur3"
)

const (
	defaultBucketSize          = 4
	defaultFingerprintSize     = 16
	defaultMaxKicks            = 500
	defaultCuckooExpansionRate = 2
)

/*
 * Cuckoo filters are probabilistic data structures that support:
 * - Membership testing (like Bloom filters)
 * - Deletion of items (unlike standard Bloom filters)
 * - Counting occurrences of items
 * https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf
**/
type CuckooFilter interface {
	Add(item string) int
	AddNx(item string) int
	Count(item string) int
	Del(item string) int
	Exists(item string) int
	Info() []any
	MExists(items []string) []int
}

type bucket struct {
	fingerprints []uint16
}

type subCuckooFilter struct {
	buckets         []bucket
	bucketSize      uint64
	numBuckets      uint64
	fingerprintSize uint64
	maxKicks        uint64
	insertedItems   uint64
	deletedItems    uint64
}

type scalableCuckooFilter struct {
	filters           []*subCuckooFilter
	initialCapacity   uint64
	initialBucketSize uint64
	maxIterations     uint64
	expansionRate     int
	totalItems        uint64
	totalDeletes      uint64
}

// NewCuckooFilter creates a new scalable cuckoo filter.
//
// Parameters:
//   - capacity: initial capacity (number of items the first filter can hold)
//   - bucketSize: slots per bucket (0 uses default of 4)
//   - maxIterations: max kicks during insert (0 uses default of 500)
//   - expansionRate: capacity multiplier for new filters (0 uses default of 2)
//
// The filter automatically expands when full by adding new sub-filters.
func NewCuckooFilter(capacity, bucketSize, maxIterations uint64, expansionRate int) CuckooFilter {
	if capacity == 0 {
		capacity = 1024
	}
	if bucketSize == 0 {
		bucketSize = defaultBucketSize
	}
	if maxIterations == 0 {
		maxIterations = defaultMaxKicks
	}
	if expansionRate <= 0 {
		expansionRate = defaultCuckooExpansionRate
	}

	scf := &scalableCuckooFilter{
		filters:           make([]*subCuckooFilter, 0, 4),
		initialCapacity:   capacity,
		initialBucketSize: bucketSize,
		maxIterations:     maxIterations,
		expansionRate:     expansionRate,
		totalItems:        0,
		totalDeletes:      0,
	}

	// Create the first sub-filter
	scf.addNewFilter()

	return scf
}

func (scf *scalableCuckooFilter) addNewFilter() {
	filterIndex := len(scf.filters)

	// Calculate capacity: initialCapacity * expansionRate^filterIndex
	capacity := scf.initialCapacity
	for range filterIndex {
		capacity *= uint64(scf.expansionRate)
	}

	// Calculate number of buckets needed.
	// We want numBuckets * bucketSize * loadFactor >= capacity
	// Using loadFactor of ~0.95, so numBuckets ≈ capacity / (bucketSize * 0.95)
	// Round up and ensure it's at least 1
	numBuckets := (capacity + scf.initialBucketSize - 1) / scf.initialBucketSize
	if numBuckets == 0 {
		numBuckets = 1
	}

	// Create buckets with empty fingerprints
	buckets := make([]bucket, numBuckets)
	for i := range buckets {
		buckets[i] = bucket{
			fingerprints: make([]uint16, scf.initialBucketSize),
		}
	}

	filter := &subCuckooFilter{
		buckets:         buckets,
		bucketSize:      scf.initialBucketSize,
		numBuckets:      numBuckets,
		fingerprintSize: defaultFingerprintSize,
		maxKicks:        scf.maxIterations,
		insertedItems:   0,
		deletedItems:    0,
	}

	scf.filters = append(scf.filters, filter)
}

func getFingerprint(item string) uint16 {
	// Use the upper 64 bits of murmur3-128 for fingerprint
	_, h2 := murmur3.Sum128([]byte(item))

	// Take the lower 16 bits, ensure non-zero
	fp := uint16(h2 & 0xFFFF)
	if fp == 0 {
		fp = 1 // 0 indicates empty slot
	}
	return fp
}

func (f *subCuckooFilter) getBucketIndex(item string) uint64 {
	h1, _ := murmur3.Sum128([]byte(item))
	return h1 % f.numBuckets
}

// altIndex = index XOR hash(fingerprint)
func (f *subCuckooFilter) getAltBucketIndex(index uint64, fp uint16) uint64 {
	// Hash the fingerprint to get a displacement
	fpHash := murmur3.Sum64([]byte{byte(fp), byte(fp >> 8)})
	// XOR with current index to get alternate
	return (index ^ fpHash) % f.numBuckets
}

func (b *bucket) insertToBucket(fp uint16) bool {
	for i := range b.fingerprints {
		if b.fingerprints[i] == 0 { // Empty slot
			b.fingerprints[i] = fp
			return true
		}
	}
	return false
}

func (b *bucket) deleteFromBucket(fp uint16) bool {
	for i := range b.fingerprints {
		if b.fingerprints[i] == fp {
			b.fingerprints[i] = 0 // Mark as empty
			return true
		}
	}
	return false
}

func (b *bucket) containsFingerprint(fp uint16) bool {
	for _, storedFp := range b.fingerprints {
		if storedFp == fp {
			return true
		}
	}
	return false
}

func (b *bucket) countFingerprint(fp uint16) int {
	count := 0
	for _, storedFp := range b.fingerprints {
		if storedFp == fp {
			count++
		}
	}
	return count
}

// kickRecord tracks a single relocation during cuckoo insertion.
// Used to restore the filter state if insertion ultimately fails.
type kickRecord struct {
	bucketIdx uint64
	slotIdx   uint64
	fp        uint16
}

// Cuckoo insertion algorithm:
// 1. Try to insert at primary bucket
// 2. If full, try alternate bucket
// 3. If both full, randomly pick one bucket, kick out an existing fingerprint
// 4. Try to insert the kicked fingerprint at its alternate location
// 5. Repeat until successful or maxKicks reached
// 6. If failed, restore all kicked fingerprints to maintain data integrity
func (f *subCuckooFilter) insert(bucketIdx uint64, fp uint16) bool {
	// Try primary bucket
	if f.buckets[bucketIdx].insertToBucket(fp) {
		return true
	}

	// Try alternate bucket
	altIdx := f.getAltBucketIndex(bucketIdx, fp)
	if f.buckets[altIdx].insertToBucket(fp) {
		return true
	}

	// Both buckets are full - need to kick out existing items
	// Track all kicks so we can restore on failure
	kickHistory := make([]kickRecord, 0, f.maxKicks)
	currentIdx := bucketIdx

	for kick := uint64(0); kick < f.maxKicks; kick++ {
		// Pick a slot to kick out (using simple modulo for determinism)
		slotIdx := kick % f.bucketSize

		// Record the kick before modifying
		kickedFp := f.buckets[currentIdx].fingerprints[slotIdx]
		kickHistory = append(kickHistory, kickRecord{
			bucketIdx: currentIdx,
			slotIdx:   slotIdx,
			fp:        kickedFp,
		})

		// Swap: put our fingerprint in, take the existing one out
		f.buckets[currentIdx].fingerprints[slotIdx] = fp
		fp = kickedFp

		// Find alternate bucket for the kicked fingerprint
		currentIdx = f.getAltBucketIndex(currentIdx, fp)

		// Try to insert kicked fingerprint
		if f.buckets[currentIdx].insertToBucket(fp) {
			return true
		}
	}

	// Failed to insert after maxKicks - restore all kicked fingerprints
	// Restore in reverse order to maintain consistency
	for i := len(kickHistory) - 1; i >= 0; i-- {
		record := kickHistory[i]
		f.buckets[record.bucketIdx].fingerprints[record.slotIdx] = record.fp
	}

	// Couldn't find a place after maxKicks - filter is full
	return false
}

func (f *subCuckooFilter) existsInSubFilter(item string) bool {
	fp := getFingerprint(item)
	bucketIdx := f.getBucketIndex(item)

	// Check primary bucket
	if f.buckets[bucketIdx].containsFingerprint(fp) {
		return true
	}

	// Check alternate bucket
	altIdx := f.getAltBucketIndex(bucketIdx, fp)
	return f.buckets[altIdx].containsFingerprint(fp)
}

func (f *subCuckooFilter) countInSubFilter(item string) int {
	fp := getFingerprint(item)
	bucketIdx := f.getBucketIndex(item)

	count := f.buckets[bucketIdx].countFingerprint(fp)

	// Check alternate bucket (might be the same bucket in some cases)
	altIdx := f.getAltBucketIndex(bucketIdx, fp)
	if altIdx != bucketIdx {
		count += f.buckets[altIdx].countFingerprint(fp)
	}

	return count
}

func (f *subCuckooFilter) deleteFromSubFilter(item string) bool {
	fp := getFingerprint(item)
	bucketIdx := f.getBucketIndex(item)

	// Try primary bucket first
	if f.buckets[bucketIdx].deleteFromBucket(fp) {
		return true
	}

	// Try alternate bucket
	altIdx := f.getAltBucketIndex(bucketIdx, fp)
	return f.buckets[altIdx].deleteFromBucket(fp)
}

// Add inserts an item into the filter.
//
// Logic:
// 1. Compute fingerprint and bucket indices
// 2. Try to insert in the current (newest) sub-filter
// 3. If full, create a new sub-filter and retry
// 4. Return 1 on success, 0 if completely full (shouldn't happen with scaling)
func (scf *scalableCuckooFilter) Add(item string) int {
	fp := getFingerprint(item)

	currentFilter := scf.filters[len(scf.filters)-1]
	bucketIdx := currentFilter.getBucketIndex(item)

	// Try to insert
	if currentFilter.insert(bucketIdx, fp) {
		currentFilter.insertedItems++
		scf.totalItems++
		return 1
	}

	// Current filter is full - add a new one
	scf.addNewFilter()
	currentFilter = scf.filters[len(scf.filters)-1]
	bucketIdx = currentFilter.getBucketIndex(item)

	if currentFilter.insert(bucketIdx, fp) {
		currentFilter.insertedItems++
		scf.totalItems++
		return 1
	}

	// Shouldn't happen with a fresh filter, but just in case
	return 0
}

func (scf *scalableCuckooFilter) AddNx(item string) int {
	if scf.Exists(item) == 1 {
		return 0
	}

	result := scf.Add(item)
	if result == 0 {
		return -1
	}
	return 1
}

func (scf *scalableCuckooFilter) Count(item string) int {
	count := 0
	for _, filter := range scf.filters {
		count += filter.countInSubFilter(item)
	}
	return count
}

func (scf *scalableCuckooFilter) Del(item string) int {
	for i := len(scf.filters) - 1; i >= 0; i-- {
		if scf.filters[i].deleteFromSubFilter(item) {
			scf.filters[i].deletedItems++
			scf.totalItems--
			scf.totalDeletes++
			return 1
		}
	}
	return 0
}

func (scf *scalableCuckooFilter) Exists(item string) int {
	for _, filter := range scf.filters {
		if filter.existsInSubFilter(item) {
			return 1
		}
	}
	return 0
}

func (scf *scalableCuckooFilter) Info() []any {
	totalBuckets := uint64(0)
	totalSize := uint64(0)

	for _, f := range scf.filters {
		totalBuckets += f.numBuckets
		// Size in bytes: numBuckets * bucketSize * 2 bytes per fingerprint
		totalSize += f.numBuckets * f.bucketSize * 2
	}

	return []any{
		"Size", totalSize,
		"Number of buckets", totalBuckets,
		"Number of filters", len(scf.filters),
		"Number of items inserted", scf.totalItems,
		"Number of items deleted", scf.totalDeletes,
		"Bucket size", scf.initialBucketSize,
		"Expansion rate", scf.expansionRate,
		"Max iterations", scf.maxIterations,
	}
}

func (scf *scalableCuckooFilter) MExists(items []string) []int {
	result := make([]int, len(items))
	for i, item := range items {
		result[i] = scf.Exists(item)
	}
	return result
}
