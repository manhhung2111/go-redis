package core

import (
	"errors"
	"testing"

	"github.com/manhhung2111/go-redis/internal/constant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimpleString(t *testing.T) {
	cases := map[string]string{
		"+OK\r\n": "OK",
	}

	for input, expected := range cases {
		value, _, err := DecodeResp([]byte(input))
		require.NoError(t, err)

		assert.Equal(t, expected, value)
	}
}

func TestError(t *testing.T) {
	cases := map[string]string{
		"-Error message\r\n": "Error message",
	}

	for input, expected := range cases {
		value, _, err := DecodeResp([]byte(input))
		require.NoError(t, err)

		assert.Equal(t, expected, value)
	}
}

func TestInt64(t *testing.T) {
	cases := map[string]int64{
		":0\r\n":    0,
		":1000\r\n": 1000,
	}

	for input, expected := range cases {
		value, _, err := DecodeResp([]byte(input))
		require.NoError(t, err)

		assert.Equal(t, expected, value)
	}
}

func TestBulkString(t *testing.T) {
	cases := map[string]string{
		"$5\r\nhello\r\n": "hello",
		"$0\r\n\r\n":      "",
	}

	for input, expected := range cases {
		value, _, err := DecodeResp([]byte(input))
		require.NoError(t, err)

		str, ok := value.(string)
		require.True(t, ok)

		assert.Equal(t, expected, str)
	}
}

func TestArray(t *testing.T) {
	cases := map[string][]interface{}{
		"*0\r\n": {},
		"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n": {
			"hello", "world",
		},
		"*3\r\n:1\r\n:2\r\n:3\r\n": {
			int64(1), int64(2), int64(3),
		},
		"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n": {
			int64(1), int64(2), int64(3), int64(4), "hello",
		},
		"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n": {
			[]interface{}{int64(1), int64(2), int64(3)},
			[]interface{}{"Hello", "World"},
		},
	}

	for input, expected := range cases {
		value, _, err := DecodeResp([]byte(input))
		require.NoError(t, err)

		array, ok := value.([]interface{})
		require.True(t, ok)
		require.Len(t, array, len(expected))

		assert.Equal(t, expected, array)
	}
}

func TestParseCmd(t *testing.T) {
	cases := map[string]RedisCmd{
		"*3\r\n$3\r\nput\r\n$5\r\nhello\r\n$5\r\nworld\r\n": {
			Cmd:  "PUT",
			Args: []string{"hello", "world"},
		},
	}

	for input, expected := range cases {
		cmd, err := ParseCmd([]byte(input))
		require.NoError(t, err)

		assert.Equal(t, expected.Cmd, cmd.Cmd)
		assert.Equal(t, expected.Args, cmd.Args)
	}
}

func TestDecodeResp_EmptyInput(t *testing.T) {
	_, _, err := DecodeResp(nil)
	assert.Error(t, err)
}

func TestDecodeResp_UnknownPrefix(t *testing.T) {
	_, _, err := DecodeResp([]byte("?\r\n"))
	assert.ErrorIs(t, err, ErrInvalidRESP)
}

func TestReadSimpleString_Incomplete(t *testing.T) {
	_, _, err := readSimpleString([]byte("+OK"))
	assert.ErrorIs(t, err, ErrIncomplete)
}

func TestReadInt64_InvalidFormat(t *testing.T) {
	_, _, err := readInt64([]byte(":abc\r\n"))
	assert.Error(t, err)
}

func TestReadInt64_MissingCRLF(t *testing.T) {
	_, _, err := readInt64([]byte(":123"))
	assert.Error(t, err)
}

func TestReadInt64_InvalidDigit(t *testing.T) {
	_, _, err := readInt64([]byte(":12a\r\n"))
	assert.Error(t, err)
}

func TestReadInt64_Overflow(t *testing.T) {
	_, _, err := readInt64([]byte(":9223372036854775808\r\n"))
	assert.Error(t, err)
}

func TestReadInt64_PlusSign(t *testing.T) {
	val, _, err := readInt64([]byte(":+123\r\n"))
	require.NoError(t, err)
	assert.Equal(t, int64(123), val)
}

func TestReadBulkString_Null(t *testing.T) {
	val, _, err := readBulkString([]byte("$-1\r\n"))
	require.NoError(t, err)
	assert.Nil(t, val)
}

func TestReadBulkString_Incomplete(t *testing.T) {
	_, _, err := readBulkString([]byte("$5\r\nhel"))
	assert.ErrorIs(t, err, ErrIncomplete)
}

func TestReadBulkString_InvalidCRLF(t *testing.T) {
	_, _, err := readBulkString([]byte("$5\r\nhelloX"))
	assert.ErrorIs(t, err, ErrIncomplete)
}

func TestReadBulkString_WrongCRLF(t *testing.T) {
	// correct length + wrong terminator
	_, _, err := readBulkString([]byte("$5\r\nhelloXY"))
	assert.ErrorIs(t, err, ErrInvalidRESP)
}

func TestReadArray_Null(t *testing.T) {
	val, _, err := readArray([]byte("*-1\r\n"))
	require.NoError(t, err)
	assert.Nil(t, val)
}

func TestReadArray_Incomplete(t *testing.T) {
	_, _, err := readArray([]byte("*2\r\n"))
	assert.ErrorIs(t, err, ErrIncomplete)
}

func TestReadArray_InvalidNested(t *testing.T) {
	_, _, err := readArray([]byte("*1\r\n?\r\n"))
	assert.Error(t, err)
}

func TestEncodeResp_SimpleString(t *testing.T) {
	out := EncodeResp("OK", true)
	assert.Equal(t, []byte("+OK\r\n"), out)
}

func TestEncodeResp_BulkString(t *testing.T) {
	out := EncodeResp("hello", false)
	assert.Equal(t, []byte("$5\r\nhello\r\n"), out)
}

func TestEncodeResp_Int64(t *testing.T) {
	out := EncodeResp(int64(10), false)
	assert.Equal(t, []byte(":10\r\n"), out)
}

func TestEncodeResp_Uint32(t *testing.T) {
	out := EncodeResp(uint32(5), false)
	assert.Equal(t, []byte(":5\r\n"), out)
}

func TestEncodeResp_Error(t *testing.T) {
	out := EncodeResp(errors.New("ERR"), false)
	assert.Equal(t, []byte("-ERR\r\n"), out)
}

func TestEncodeResp_Nil(t *testing.T) {
	out := EncodeResp(nil, false)
	assert.Equal(t, constant.RESP_NIL_BULK_STRING, out)
}

func TestEncodeResp_StringArray(t *testing.T) {
	out := EncodeResp([]string{"a", "b"}, false)
	assert.Equal(t, []byte("*2\r\n$1\r\na\r\n$1\r\nb\r\n"), out)
}

func TestEncodeResp_IntArray(t *testing.T) {
	out := EncodeResp([]int64{1, 2}, false)
	assert.Equal(t, []byte("*2\r\n:1\r\n:2\r\n"), out)
}

func TestEncodeResp_UnsupportedType(t *testing.T) {
	out := EncodeResp(struct{}{}, false)
	assert.Equal(t, constant.RESP_NIL_BULK_STRING, out)
}

func TestParseCmd_InvalidRoot(t *testing.T) {
	_, err := ParseCmd([]byte("+OK\r\n"))
	assert.Error(t, err)
}

func TestParseCmd_EmptyArray(t *testing.T) {
	_, err := ParseCmd([]byte("*0\r\n"))
	assert.Error(t, err)
}

func TestParseCmd_NonStringCommand(t *testing.T) {
	_, err := ParseCmd([]byte("*1\r\n:123\r\n"))
	assert.Error(t, err)
}

func TestParseCmd_NonStringArg(t *testing.T) {
	_, err := ParseCmd([]byte("*2\r\n$3\r\nGET\r\n:1\r\n"))
	assert.Error(t, err)
}
