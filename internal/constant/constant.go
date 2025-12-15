package constant

var RESP_NIL_BULK_STRING []byte = []byte("$-1\r\n")
var RESP_OK []byte = []byte("+OK\r\n")
var RESP_TTL_KEY_NOT_EXIST []byte = []byte(":-2\r\n")
var RESP_TTL_KEY_EXIST_NO_EXPIRE []byte = []byte(":-1\r\n")

var RESP_WRONGTYPE_OPERATION_AGAINST_KEY []byte = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

var RESP_EXPIRE_OPTIONS_NOT_COMPATIBLE []byte = []byte("-NX and XX, GT or LT options at the same time are not compatible\r\n")
var RESP_EXPIRE_TIMEOUT_NOT_SET []byte = []byte(":0\r\n")
var RESP_EXPIRE_TIMEOUT_SET []byte = []byte(":1\r\n")

const NO_EXPIRE int64 = -1
/** Errors */