package config

type EvictionPolicy string

const (
	NoEviction     EvictionPolicy = "noeviction"
	AllKeysLRU     EvictionPolicy = "allkeys-lru"
	AllKeysLFU     EvictionPolicy = "allkeys-lfu"
	AllKeysRandom  EvictionPolicy = "allkeys-random"
	VolatileLRU    EvictionPolicy = "volatile-lru"
	VolatileLFU    EvictionPolicy = "volatile-lfu"
	VolatileRandom EvictionPolicy = "volatile-random"
	VolatileTTL    EvictionPolicy = "volatile-ttl"
)

// Bloom filter validation constants
const (
	BFMinCapacity  = 1
	BFMaxCapacity  = 1073741824
	BFMinExpansion = 1
	BFMaxExpansion = 32768
)

// Cuckoo filter validation constants
const (
	CFMaxInitialSize  = 1048576
	CFMinBucketSize   = 1
	CFMaxBucketSize   = 255
	CFMinMaxIterations = 1
	CFMaxMaxIterations = 65535
	CFMinExpansionFactor = 0
	CFMaxExpansionFactor = 32768
)

// Config holds all configuration values for the Redis server.
type Config struct {
	// Server settings
	Host          string
	Port          int
	MaxConnection int

	// List settings
	ListMaxListpackSize string

	// Set settings
	SetMaxIntsetEntries int

	// Bloom filter settings
	BFDefaultErrorRate float64
	BFDefaultCapacity  int
	BFDefaultExpansion int
	BFMinCapacity      int
	BFMaxCapacity      int
	BFMinExpansion     int
	BFMaxExpansion     int

	// Cuckoo filter settings
	CFDefaultBucketSize      int
	CFMaxBucketSize          int
	CFMinBucketSize          int
	CFDefaultInitialSize     int
	CFMaxInitialSize         int
	CFDefaultExpansionFactor int
	CFMinExpansionFactor     int
	CFMaxExpansionFactor     int
	CFDefaultMaxExpansions   int
	CFDefaultMaxIterations   int
	CFMinMaxIterations       int
	CFMaxMaxIterations       int

	// Active expire cycle settings
	ActiveExpireCycleMs               int
	ActiveExpireCycleKeysPerLoop      int
	ActiveExpireCycleTimeLimitUsage   int
	ActiveExpireCycleThresholdPercent int

	// Eviction settings
	EvictionPolicy   EvictionPolicy
	EvictionPoolSize int
	MaxmemorySamples int
	MaxmemoryLimit   int64

	// LFU settings
	LFUInitVal   uint8
	LFULogFactor int
	LFUDecayTime uint32
}

// NewConfig returns a Config with default values.
func NewConfig() *Config {
	return &Config{
		Host:          "0.0.0.0",
		Port:          6379,
		MaxConnection: 10000,

		ListMaxListpackSize: "8KiB",

		SetMaxIntsetEntries: 512,

		BFDefaultErrorRate: 0.01,
		BFDefaultCapacity:  100,
		BFDefaultExpansion: 2,
		BFMinCapacity:      1,
		BFMaxCapacity:      1073741824,
		BFMinExpansion:     1,
		BFMaxExpansion:     32768,

		CFDefaultBucketSize:      4,
		CFMaxBucketSize:          255,
		CFMinBucketSize:          1,
		CFDefaultInitialSize:     1024,
		CFMaxInitialSize:         1048576,
		CFDefaultExpansionFactor: 1,
		CFMinExpansionFactor:     0,
		CFMaxExpansionFactor:     32768,
		CFDefaultMaxExpansions:   32,
		CFDefaultMaxIterations:   20,
		CFMinMaxIterations:       1,
		CFMaxMaxIterations:       65535,

		ActiveExpireCycleMs:               100,
		ActiveExpireCycleKeysPerLoop:      20,
		ActiveExpireCycleTimeLimitUsage:   1000,
		ActiveExpireCycleThresholdPercent: 25,

		EvictionPolicy:   AllKeysLRU,
		EvictionPoolSize: 16,
		MaxmemorySamples: 10,
		MaxmemoryLimit:   3 * 1024 * 1024 * 1024, // 3GiB

		LFUInitVal:   5,
		LFULogFactor: 10,
		LFUDecayTime: 1,
	}
}
