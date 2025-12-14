package constant

var RESP_NIL_BULK_STRING []byte = []byte("$-1\r\n")
var RESP_OK []byte = []byte("+OK\r\n")
var TTL_KEY_NOT_EXIST []byte = []byte(":-2\r\n")
var TTL_KEY_EXIST_NO_EXPIRE []byte = []byte(":-1\r\n")

const NO_EXPIRE int64 = -1
/** Errors */