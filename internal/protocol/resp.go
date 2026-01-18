package protocol

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

type RespType byte

const (
	RespSimpleString RespType = '+'
	RespError        RespType = '-'
	RespInteger      RespType = ':'
	RespBulkString   RespType = '$'
	RespArray        RespType = '*'
)

const (
	CR byte = '\r'
	LF byte = '\n'
)

var CRLF = []byte{CR, LF}

var (
	ErrInvalidRESP = errors.New("invalid RESP")
	ErrIncomplete  = errors.New("incomplete RESP frame")
)

var NilBulkString = struct{}{}

/** +OK\r\n -> OK, 5 */
/** -Error message\r\n -> Error message */
func readSimpleString(data []byte) (string, int, error) {
	for i := 1; i+1 < len(data); i++ {
		if data[i] == CR && data[i+1] == LF {
			return string(data[1:i]), i + 2, nil
		}
	}
	return "", 0, ErrIncomplete
}

/** :[<+|->]<value>\r\n -> <value> */
func readInt64(data []byte) (int64, int, error) {
	idx := 1
	sign := int64(1)

	if idx >= len(data) {
		return 0, 0, fmt.Errorf("unexpected end")
	}

	switch data[idx] {
	case '-':
		sign = -1
		idx++
	case '+':
		idx++
	}

	if idx >= len(data) || data[idx] < '0' || data[idx] > '9' {
		return 0, 0, fmt.Errorf("invalid integer format")
	}

	var res int64 = 0
	for idx < len(data) && data[idx] != '\r' {
		if data[idx] < '0' || data[idx] > '9' {
			return 0, 0, fmt.Errorf("invalid digit")
		}

		// overflow check
		if res > (math.MaxInt64-int64(data[idx]-'0'))/10 {
			return 0, 0, fmt.Errorf("integer overflow")
		}

		res = res*10 + int64(data[idx]-'0')
		idx++
	}

	if idx+1 >= len(data) || data[idx] != '\r' || data[idx+1] != '\n' {
		return 0, 0, fmt.Errorf("missing CRLF")
	}

	return sign * res, idx + 2, nil
}

/** $5\r\nhello\r\n -> "hello" */
func readBulkString(data []byte) (interface{}, int, error) {
	strLen64, idx, err := readInt64(data)
	if err != nil {
		return nil, 0, err
	}

	if strLen64 == -1 {
		return nil, idx, nil // NULL bulk string
	}

	strLen := int(strLen64)
	if idx+strLen+2 > len(data) {
		return nil, 0, ErrIncomplete
	}

	if data[idx+strLen] != CR || data[idx+strLen+1] != LF {
		return nil, 0, ErrInvalidRESP
	}

	return string(data[idx : idx+strLen]), idx + strLen + 2, nil
}

/** *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n -> {"hello", "world"} */
func readArray(data []byte) (interface{}, int, error) {
	count64, idx, err := readInt64(data)
	if err != nil {
		return nil, 0, err
	}

	if count64 == -1 {
		return nil, idx, nil // NULL array
	}

	count := int(count64)
	res := make([]interface{}, count)

	for i := 0; i < count; i++ {
		if idx >= len(data) {
			return nil, 0, ErrIncomplete
		}

		val, consumed, err := DecodeResp(data[idx:])
		if err != nil {
			return nil, 0, err
		}

		res[i] = val
		idx += consumed
	}

	return res, idx, nil
}

func DecodeResp(data []byte) (interface{}, int, error) {
	if len(data) == 0 {
		return nil, 0, errors.New("no data to decode")
	}

	switch RespType(data[0]) {
	case RespSimpleString, RespError:
		return readSimpleString(data)
	case RespInteger:
		return readInt64(data)
	case RespBulkString:
		return readBulkString(data)
	case RespArray:
		return readArray(data)
	default:
		return nil, 0, ErrInvalidRESP
	}
}

// EncodeResp encodes a value into RESP (REdis Serialization Protocol) format
func EncodeResp(value interface{}, isSimpleString bool) []byte {
	switch v := value.(type) {
	case string:
		return encodeString(v, isSimpleString)
	case int:
		return encodeInteger(int64(v))
	case int64:
		return encodeInteger(v)
	case *int64:
		return encodeInteger(*v)
	case uint64:
		return encodeUnsignedInteger(v)
	case uint32:
		return encodeInteger(int64(v))
	case float64:
		return encodeFloat(v)
	case error:
		return encodeError(v)
	case nil:
		return encodeNil()
	case *string:
		return encodeStringPointer(v)
	case *float64:
		return encodeFloatPointer(v)
	case []string:
		return encodeArray(v)
	case []int64:
		return encodeArray(v)
	case []uint64:
		return encodeArray(v)
	case []int:
		return encodeArray(v)
	case []*string:
		return encodeArray(v)
	case []*float64:
		return encodeArray(v)
	case []any:
		return encodeArray(v)
	default:
		return encodeNil()
	}
}

func encodeString(s string, isSimpleString bool) []byte {
	if isSimpleString {
		return []byte(fmt.Sprintf("+%s%s", s, CRLF))
	}
	return encodeBulkString(s)
}

func encodeBulkString(s string) []byte {
	return []byte(fmt.Sprintf("$%d%s%s%s", len(s), CRLF, s, CRLF))
}

func encodeUnsignedInteger(n uint64) []byte {
	return []byte(fmt.Sprintf(":%d%s", n, CRLF))
}

func encodeInteger(n int64) []byte {
	return []byte(fmt.Sprintf(":%d%s", n, CRLF))
}

func encodeFloat(f float64) []byte {
	s := strconv.FormatFloat(f, 'f', -1, 64)
	return encodeBulkString(s)
}

func encodeError(err error) []byte {
	return []byte(fmt.Sprintf("-%s%s", err.Error(), CRLF))
}

func encodeNil() []byte {
	return []byte(fmt.Sprintf("$-1%s", CRLF))
}

func encodeStringPointer(s *string) []byte {
	if s == nil {
		return encodeNil()
	}
	return encodeBulkString(*s)
}

func encodeFloatPointer(f *float64) []byte {
	if f == nil {
		return encodeNil()
	}
	return encodeFloat(*f)
}

func encodeArray[T any](arr []T) []byte {
	var buf strings.Builder

	// Write array header
	buf.WriteString(fmt.Sprintf("*%d%s", len(arr), CRLF))

	// Write each element
	for _, elem := range arr {
		buf.Write(EncodeResp(elem, false))
	}

	return []byte(buf.String())
}

func ParseCmd(data []byte) (*RedisCmd, error) {
	val, _, err := DecodeResp(data)
	if err != nil {
		return nil, err
	}

	arr, ok := val.([]interface{})
	if !ok || len(arr) == 0 {
		return nil, ErrInvalidRESP
	}

	cmd, ok := arr[0].(string)
	if !ok {
		return nil, ErrInvalidRESP
	}

	args := make([]string, len(arr)-1)
	for i := 1; i < len(arr); i++ {
		s, ok := arr[i].(string)
		if !ok {
			return nil, ErrInvalidRESP
		}
		args[i-1] = s
	}

	return &RedisCmd{
		Cmd:  strings.ToUpper(cmd),
		Args: args,
	}, nil
}
