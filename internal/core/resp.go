package core

import (
	"errors"
	"fmt"
	"strings"
)

const (
	SIMPLE_STRING = '+'
	ERROR         = '-'
	INTEGER       = ':'
	BULK_STRING   = '$'
	ARRAY         = '*'
	CR            = '\r'
	LF            = '\n'
	CRLF          = "\r\n"
)

/** +OK\r\n -> OK, 5 */
func readSimpleString(data []byte) (string, int, error) {
	idx := 1 // skip the first byte
	for data[idx] != CR {
		idx++
	}
	return string(data[1:idx]), idx + 2, nil
}

/** :123\r\n -> 123, 6 */
func readInt64(data []byte) (int64, int, error) {
	var res int64 = 0
	idx := 1
	for data[idx] != CR {
		res = res*10 + int64(data[idx]-'0')
		idx++
	}

	return res, idx + 2, nil
}

func readError(data []byte) (string, int, error) {
	return readSimpleString(data)
}

/** $5\r\hello\r\n -> 5, 4 */
func readLen(data []byte) (int, int) {
	res, idx, _ := readInt64(data)
	return int(res), idx
}

/** $5\r\nhello\r\n -> "hello" */
func readBulkString(data []byte) (string, int, error) {
	strLen, idx := readLen(data)
	endIdx := idx + strLen

	return string(data[idx:endIdx]), endIdx + 2, nil
}

/** *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n -> {"hello", "world"} */
func readArray(data []byte) (interface{}, int, error) {
	arrLen, idx := readLen(data)

	var res []interface{} = make([]interface{}, arrLen)

	for i := range arrLen {
		element, delta, err := Decode(data[idx:])
		if err != nil {
			return nil, 0, err
		}

		res[i] = element
		idx += delta
	}

	return res, idx, nil
}

func Decode(data []byte) (interface{}, int, error) {
	if len(data) == 0 {
		return nil, 0, errors.New("no data to decode")
	}

	switch data[0] {
	case SIMPLE_STRING:
		return readSimpleString(data)
	case INTEGER:
		return readInt64(data)
	case ERROR:
		return readError(data)
	case BULK_STRING:
		return readBulkString(data)
	case ARRAY:
		return readArray(data)
	default:
		return nil, 0, errors.New(fmt.Sprintf("unsupported data type %c", data[0]))
	}
}

func Encode(value interface{}, isSimpleString bool) []byte {
	switch v := value.(type) {
	case string:
		if isSimpleString {
			return []byte(fmt.Sprintf("+%s%s", v, CRLF))
		}
		return []byte(fmt.Sprintf("$%d%s%s%s", len(v), CRLF, v, CRLF))
	}
	return []byte{}
}

func ParseCmd(data []byte) (*RedisCmd, error) {
	value, _, err := Decode(data)
	if err != nil {
		return nil, err
	}

	arr := value.([]interface{})
	tokens := make([]string, len(arr))

	for i := range tokens {
		tokens[i] = arr[i].(string)
	}

	redisCmd := &RedisCmd{
		Cmd:  strings.ToUpper(tokens[0]),
		Args: tokens[1:],
	}

	return redisCmd, err
}
