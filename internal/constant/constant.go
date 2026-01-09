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

var RESP_SYNTAX_ERROR []byte = []byte("-syntax error\r\n")
var RESP_XX_NX_NOT_COMPATIBLE []byte = []byte("-XX and NX options at the same time are not compatible\r\n")
var RESP_GT_LT_NX_NOT_COMPATIBLE []byte = []byte("-GT, LT, and/or NX options at the same time are not compatible\r\n")
var RESP_VALUE_IS_NOT_VALID_FLOAT []byte = []byte("-value is not a valid float\r\n")
var RESP_MIN_OR_MAX_IS_NOT_FLOAT []byte = []byte("-min or max is not a float\r\n")
var RESP_WITHSCORES_NOT_SUPPORTED_WITH_BYLEX []byte = []byte("-syntax error, WITHSCORES not supported in combination with BYLEX\r\n")

var RESP_INVALID_LONGITUDE_LATITUDE []byte = []byte("-ERR invalid longitude,latitude pair\r\n")
var RESP_GEO_FROMMEMBER_OR_FROMLONLAT_REQUIRED []byte = []byte("-ERR exactly one of FROMMEMBER or FROMLONLAT is required\r\n")
var RESP_GEO_BYRADIUS_OR_BYBOX_REQUIRED []byte = []byte("-ERR exactly one of BYRADIUS or BYBOX is required\r\n")

var RESP_BAD_ERROR_RATE []byte = []byte("-bad error rate\r\n")
var RESP_ERROR_RATE_INVALID_RANGE []byte = []byte("-error rate must be in the range (0.000000, 1.000000)\r\n")
var RESP_BAD_CAPACITY []byte = []byte("-bad capacity\r\n")
var RESP_CAPACITY_INVALID_RANGE []byte = []byte("-capacity must be in the range [1, 1073741824]\r\n")
var RESP_BAD_EXPANSION []byte = []byte("-bad expansion\r\n")
var RESP_EXPANSION_INVALID_RANGE []byte = []byte("-expansion must be in the range [0, 32768]\r\n")
var RESP_ITEM_EXISTS []byte = []byte("-item exists\r\n")
var RESP_NOT_FOUND []byte = []byte("-not found\r\n")

var RESP_BAD_BUCKET_SIZE []byte = []byte("-bad bucket size\r\n")
var RESP_BUCKET_SIZE_INVALID_RANGE []byte = []byte("-bucket size must be in the range [1, 255]\r\n")
var RESP_BAD_MAX_ITERATIONS []byte = []byte("-bad max iterations\r\n")
var RESP_MAX_ITERATIONS_INVALID_RANGE []byte = []byte("-max iterations must be in the range [1, 65535]\r\n")

var RESP_CMS_KEY_DOES_NOT_EXIST []byte = []byte("-CMS: key does not exist\r\n")
var RESP_CMS_KEY_ALREADY_EXISTS []byte = []byte("-CMS: key already exists\r\n")
var RESP_CMS_BAD_INCREMENT []byte = []byte("-invalid increment value\r\n")
var RESP_CMS_BAD_WIDTH []byte = []byte("-invalid width value\r\n")
var RESP_CMS_BAD_DEPTH []byte = []byte("-invalid depth value\r\n")
var RESP_CMS_BAD_PROBABILITY []byte = []byte("-invalid probability value\r\n")
var RESP_CMS_PROBABILITY_INVALID_RANGE []byte = []byte("-probability must be in the range (0, 1)\r\n")

const NO_EXPIRE int64 = -1
const KEY_NOT_EXISTS int64 = -2

/** Errors */
