package constant

var RESP_NIL_BULK_STRING []byte = []byte("$-1\r\n")
var RESP_OK []byte = []byte("+OK\r\n")
var RESP_TTL_KEY_NOT_EXIST []byte = []byte(":-2\r\n")
var RESP_TTL_KEY_EXIST_NO_EXPIRE []byte = []byte(":-1\r\n")

var RESP_WRONGTYPE_OPERATION_AGAINST_KEY []byte = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

var RESP_EXPIRE_OPTIONS_NOT_COMPATIBLE []byte = []byte("-NX and XX, GT or LT options at the same time are not compatible\r\n")
var RESP_EXPIRE_TIMEOUT_NOT_SET []byte = []byte(":0\r\n")
var RESP_EXPIRE_TIMEOUT_SET []byte = []byte(":1\r\n")

var RESP_VALUE_IS_NOT_INTEGER_OR_OUT_OF_RANGE []byte = []byte("-value is not an integer or out of range\r\n")
var RESP_VALUE_IS_OUT_OF_RANGE_MUST_BE_POSITIVE []byte = []byte("-value is out of range, must be positive\r\n")

var RESP_SYNTAX_ERROR []byte = []byte("-syntax error")
var RESP_XX_NX_NOT_COMPATIBLE []byte = []byte("-XX and NX options at the same time are not compatible")
var RESP_GT_LT_NX_NOT_COMPATIBLE []byte = []byte("-GT, LT, and/or NX options at the same time are not compatible")
var RESP_VALUE_IS_NOT_VALID_FLOAT []byte = []byte("-value is not a valid float")
var RESP_MIN_OR_MAX_IS_NOT_FLOAT []byte = []byte("-min or max is not a float")
var RESP_WITHSCORES_NOT_SUPPORTED_WITH_BYLEX []byte = []byte("-syntax error, WITHSCORES not supported in combination with BYLEX")

var RESP_INVALID_LONGITUDE_LATITUDE []byte = []byte("-ERR invalid longitude,latitude pair\r\n")
var RESP_GEO_FROMMEMBER_OR_FROMLONLAT_REQUIRED []byte = []byte("-ERR exactly one of FROMMEMBER or FROMLONLAT is required\r\n")
var RESP_GEO_BYRADIUS_OR_BYBOX_REQUIRED []byte = []byte("-ERR exactly one of BYRADIUS or BYBOX is required\r\n")

const NO_EXPIRE int64 = -1
const KEY_NOT_EXISTS int64 = -2

/** Errors */
