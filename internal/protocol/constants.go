package protocol

// Pre-encoded RESP responses for common operations
var (
	RespNilBulkString = []byte("$-1\r\n")
	RespOK            = []byte("+OK\r\n")
)

// TTL response constants
var (
	RespTTLKeyNotExist      = []byte(":-2\r\n")
	RespTTLKeyExistNoExpire = []byte(":-1\r\n")
)

// Error responses
var (
	RespWrongTypeOperation = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	RespSyntaxError        = []byte("-syntax error\r\n")
)

// Expire command responses
var (
	RespExpireOptionsNotCompatible = []byte("-NX and XX, GT or LT options at the same time are not compatible\r\n")
	RespExpireTimeoutNotSet        = []byte(":0\r\n")
	RespExpireTimeoutSet           = []byte(":1\r\n")
)

// Value validation errors
var (
	RespValueNotIntegerOrOutOfRange = []byte("-value is not an integer or out of range\r\n")
	RespValueOutOfRangeMustPositive = []byte("-value is out of range, must be positive\r\n")
	RespValueNotValidFloat          = []byte("-value is not a valid float\r\n")
	RespMinOrMaxNotFloat            = []byte("-min or max is not a float\r\n")
)

// ZSet errors
var (
	RespXXNXNotCompatible           = []byte("-XX and NX options at the same time are not compatible\r\n")
	RespGTLTNXNotCompatible         = []byte("-GT, LT, and/or NX options at the same time are not compatible\r\n")
	RespWithScoresNotSupportedByLex = []byte("-syntax error, WITHSCORES not supported in combination with BYLEX\r\n")
)

// Geo errors
var (
	RespInvalidLongitudeLatitude       = []byte("-ERR invalid longitude,latitude pair\r\n")
	RespGeoFromMemberOrFromLonLatReq   = []byte("-ERR exactly one of FROMMEMBER or FROMLONLAT is required\r\n")
	RespGeoByRadiusOrByBoxReq          = []byte("-ERR exactly one of BYRADIUS or BYBOX is required\r\n")
)

// Bloom filter errors
var (
	RespBadErrorRate          = []byte("-bad error rate\r\n")
	RespErrorRateInvalidRange = []byte("-error rate must be in the range (0.000000, 1.000000)\r\n")
	RespBadCapacity           = []byte("-bad capacity\r\n")
	RespCapacityInvalidRange  = []byte("-capacity must be in the range [1, 1073741824]\r\n")
	RespBadExpansion          = []byte("-bad expansion\r\n")
	RespExpansionInvalidRange = []byte("-expansion must be in the range [0, 32768]\r\n")
	RespItemExists            = []byte("-item exists\r\n")
	RespNotFound              = []byte("-not found\r\n")
)

// Cuckoo filter errors
var (
	RespBadBucketSize           = []byte("-bad bucket size\r\n")
	RespBucketSizeInvalidRange  = []byte("-bucket size must be in the range [1, 255]\r\n")
	RespBadMaxIterations        = []byte("-bad max iterations\r\n")
	RespMaxIterationsInvalidRange = []byte("-max iterations must be in the range [1, 65535]\r\n")
)

// Count-Min Sketch errors
var (
	RespCMSKeyDoesNotExist       = []byte("-CMS: key does not exist\r\n")
	RespCMSKeyAlreadyExists      = []byte("-CMS: key already exists\r\n")
	RespCMSBadIncrement          = []byte("-invalid increment value\r\n")
	RespCMSBadWidth              = []byte("-invalid width value\r\n")
	RespCMSBadDepth              = []byte("-invalid depth value\r\n")
	RespCMSBadProbability        = []byte("-invalid probability value\r\n")
	RespCMSProbabilityInvalidRange = []byte("-probability must be in the range (0, 1)\r\n")
)

// General errors
var (
	RespErrNoSuchKey = []byte("-ERR no such key\r\n")
)

// TTL constants
const (
	NoExpire      int64 = -1
	KeyNotExists  int64 = -2
)
