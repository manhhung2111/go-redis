package config

var HOST string = "0.0.0.0"
var PORT int = 6379
var MAX_CONNECTION int = 10000

var LIST_MAX_LISTPACK_SIZE = "8KiB"
var SET_MAX_INTSET_ENTRIES = 512

var BF_DEFAULT_ERROR_RATE = 0.01
var BF_DEFAULT_CAPACITY = 100
var BF_DEFAULT_EXPANSION = 2
var BF_MIN_CAPACITY = 1
var BF_MAX_CAPACITY = 1073741824
var BF_MIN_EXPANSION = 1
var BF_MAX_EXPANSION = 32768

var CF_DEFAULT_BUCKET_SIZE = 4
var CF_MAX_BUCKET_SIZE = 255
var CF_MIN_BUCKET_SIZE = 1
var CF_DEFAULT_INITIAL_SIZE = 1024
var CF_MAX_INITIAL_SIZE = 1048576
var CF_DEFAULT_EXPANSION_FACTOR = 1
var CF_MIN_EXPANSION_FACTOR = 0
var CF_MAX_EXPANSION_FACTOR = 32768
var CF_DEFAULT_MAX_EXPANSIONS = 32
var CF_DEFAULT_MAX_ITERATIONS = 20
var CF_MIN_MAX_ITERATIONS = 1
var CF_MAX_MAX_ITERATIONS = 65535

var ACTIVE_EXPIRE_CYCLE_MS = 100                // Timer interval in milliseconds (eg: 10 times / second)
var ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP = 20      // Keys to sample per iteration
var ACTIVE_EXPIRE_CYCLE_TIME_LIMIT_USAGE = 1000 // Time budget per cycle in microseconds (1ms)
var ACTIVE_EXPIRE_CYCLE_THRESHOLD_PERCENT = 25  // Continue if > 25% expired

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

var EVICTION_POLICY = AllKeysLRU
var EVICTION_POOL_SIZE = 16
var MAXMEMORY_SAMPLES = 10
