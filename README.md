# go-redis

A Redis-compatible server built from scratch in Go. The server is compatible with standard Redis clients including `redis-cli`, making it a drop-in replacement for development and learning purposes.

## Acknowledgements

This project was inspired by ideas from the
[memkv](https://github.com/quangh33/memkv) project.

## Features

- **High-Performance I/O Multiplexing**: Single-threaded, non-blocking TCP server using platform-native mechanisms: kqueue on macOS and epoll on Linux. Handles thousands of concurrent connections efficiently without threading overhead.
- **RESP Compliant**: Full implementation of Redis Serialization Protocol (RESP), ensuring compatibility with all standard Redis clients including `redis-cli`.
- **Core Data Structures**: Strings, Lists, Sets, Hashes, Sorted Sets, and Geo indexes with extensive command support.
- **Probabilistic Data Structures**:
  - **Bloom Filter**: Space-efficient membership testing with configurable false positive rate
  - **Cuckoo Filter**: Membership testing with deletion support and better space efficiency
  - **HyperLogLog**: Cardinality estimation using minimal memory (~12KB for billions of elements)
  - **Count-Min Sketch**: Frequency estimation for streaming data
- **Key Expiration**: Supports TTL-based key expiration with two strategies:
  - **Passive expiration**: Keys are checked and removed when accessed
  - **Active expiration**: A CPU-bounded (1ms) background cycle runs periodically (every 100ms) to sample and remove expired keys
- **Eviction Policies**: Memory management with configurable eviction policies:
  - `noeviction`: Return errors when memory limit is reached
  - `allkeys-lru`: Evict least recently used keys
  - `allkeys-lfu`: Evict least frequently used keys
  - `allkeys-random`: Evict random keys
  - `volatile-lru`: Evict least recently used keys with TTL
  - `volatile-lfu`: Evict least frequently used keys with TTL
  - `volatile-ttl`: Evict keys with shortest TTL
  - `volatile-random`: Evict random keys with TTL

## Getting Started with Docker

```bash
# Pull the image
docker pull manhhung2111/go-redis:1.0.0

# Run the server
docker run -d --name go-redis -p 6379:6379 manhhung2111/go-redis:1.0.0

# Interact with the server using redis-cli
docker exec -it go-redis redis-cli
```

## Build from Source

### Prerequisites

- Go 1.25 or later

### Build and Run

```bash
# Clone the repository
git clone https://github.com/manhhung2111/go-redis.git
cd go-redis

# Build
go build -o go-redis ./cmd

# Run
./go-redis
```

## Supported Commands

### General

- `PING [message]`
- `DEL key [key ...]`
- `TTL key`
- `EXPIRE key seconds [NX | XX | GT | LT]`

### Strings

- `SET key value [NX | XX] [EX seconds]`
- `GET key`
- `INCR key`
- `INCRBY key increment`
- `DECR key`
- `DECRBY key decrement`
- `MGET key [key ...]`
- `MSET key value [key value ...]`

### Lists

- `LPUSH key element [element ...]`
- `LPUSHX key element [element ...]`
- `LPOP key [count]`
- `RPUSH key element [element ...]`
- `RPUSHX key element [element ...]`
- `RPOP key [count]`
- `LRANGE key start stop`
- `LINDEX key index`
- `LLEN key`
- `LREM key count element`
- `LSET key index element`
- `LTRIM key start stop`

### Sets

- `SADD key member [member ...]`
- `SCARD key`
- `SISMEMBER key member`
- `SMEMBERS key`
- `SMISMEMBER key member [member ...]`
- `SREM key member [member ...]`
- `SPOP key [count]`
- `SRANDMEMBER key [count]`

### Hashes

- `HSET key field value [field value ...]`
- `HSETNX key field value`
- `HGET key field`
- `HGETALL key`
- `HMGET key field [field ...]`
- `HINCRBY key field increment`
- `HKEYS key`
- `HVALS key`
- `HLEN key`
- `HDEL key field [field ...]`
- `HEXISTS key field`

### Sorted Sets

- `ZADD key [NX | XX] [GT | LT] [CH] score member [score member ...]`
- `ZCARD key`
- `ZCOUNT key min max`
- `ZINCRBY key increment member`
- `ZLEXCOUNT key min max`
- `ZMSCORE key member [member ...]`
- `ZPOPMAX key [count]`
- `ZPOPMIN key [count]`
- `ZRANDMEMBER key [count [WITHSCORES]]`
- `ZRANGE key start stop [BYSCORE | BYLEX] [REV] [WITHSCORES]`
- `ZRANK key member [WITHSCORE]`
- `ZREM key member [member ...]`
- `ZREVRANK key member [WITHSCORE]`
- `ZSCORE key member`

### Geo

- `GEOADD key [NX | XX] [CH] longitude latitude member [longitude latitude member ...]`
- `GEODIST key member1 member2 [M | KM | FT | MI]`
- `GEOHASH key member [member ...]`
- `GEOPOS key member [member ...]`
- `GEOSEARCH key [FROMMEMBER member | FROMLONLAT longitude latitude] [BYRADIUS radius M | KM | FT | MI | BYBOX width height M | KM | FT | MI] [ASC | DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]`

### Bloom Filter

- `BF.ADD key item`
- `BF.CARD key`
- `BF.EXISTS key item`
- `BF.INFO key [CAPACITY | SIZE | FILTERS | ITEMS | EXPANSION]`
- `BF.MADD key item [item ...]`
- `BF.MEXISTS key item [item ...]`
- `BF.RESERVE key error_rate capacity [EXPANSION expansion]`

### Cuckoo Filter

- `CF.ADD key item`
- `CF.ADDNX key item`
- `CF.COUNT key item`
- `CF.DEL key item`
- `CF.EXISTS key item`
- `CF.INFO key`
- `CF.MEXISTS key item [item ...]`
- `CF.RESERVE key capacity [BUCKETSIZE bucketsize] [MAXITERATIONS maxiterations] [EXPANSION expansion]`

### HyperLogLog

- `PFADD key [element [element ...]]`
- `PFCOUNT key [key ...]`
- `PFMERGE destkey [sourcekey [sourcekey ...]]`

### Count-Min Sketch

- `CMS.INCRBY key item increment [item increment ...]`
- `CMS.INFO key`
- `CMS.INITBYDIM key width depth`
- `CMS.INITBYPROB key error probability`
- `CMS.QUERY key item [item ...]`
