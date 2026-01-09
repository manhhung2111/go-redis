package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manhhung2111/go-redis/internal/constant"
)

func TestCMSInitByDim(t *testing.T) {
	r := newTestRedis()

	resp := r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "100", "5"))
	assert.Equal(t, constant.RESP_OK, resp)

	// Verify CMS was created by getting info
	resp = r.CMSInfo(cmd("CMS.INFO", "cms"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestCMSInitByDimKeyExists(t *testing.T) {
	r := newTestRedis()

	resp := r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "100", "5"))
	assert.Equal(t, constant.RESP_OK, resp)

	// Try to create again - should fail
	resp = r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "200", "10"))
	assert.Equal(t, constant.RESP_CMS_KEY_ALREADY_EXISTS, resp)
}

func TestCMSInitByDimKeyExistsOtherType(t *testing.T) {
	r := newTestRedis()

	// Create a string key
	r.Set(cmd("SET", "k", "v"))

	// Try to create CMS with same key - should fail
	resp := r.CMSInitByDim(cmd("CMS.INITBYDIM", "k", "100", "5"))
	assert.Equal(t, constant.RESP_CMS_KEY_ALREADY_EXISTS, resp)
}

func TestCMSInitByDimWrongArgs(t *testing.T) {
	r := newTestRedis()

	// Too few args
	resp := r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	resp = r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "100"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	// Too many args
	resp = r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "100", "5", "extra"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestCMSInitByDimBadWidth(t *testing.T) {
	r := newTestRedis()

	resp := r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "invalid", "5"))
	assert.Equal(t, constant.RESP_CMS_BAD_WIDTH, resp)

	resp = r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "0", "5"))
	assert.Equal(t, constant.RESP_CMS_BAD_WIDTH, resp)

	resp = r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "-1", "5"))
	assert.Equal(t, constant.RESP_CMS_BAD_WIDTH, resp)
}

func TestCMSInitByDimBadDepth(t *testing.T) {
	r := newTestRedis()

	resp := r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "100", "invalid"))
	assert.Equal(t, constant.RESP_CMS_BAD_DEPTH, resp)

	resp = r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "100", "0"))
	assert.Equal(t, constant.RESP_CMS_BAD_DEPTH, resp)

	resp = r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "100", "-1"))
	assert.Equal(t, constant.RESP_CMS_BAD_DEPTH, resp)
}

func TestCMSInitByProb(t *testing.T) {
	r := newTestRedis()

	resp := r.CMSInitByProb(cmd("CMS.INITBYPROB", "cms", "0.01", "0.01"))
	assert.Equal(t, constant.RESP_OK, resp)

	// Verify CMS was created
	resp = r.CMSInfo(cmd("CMS.INFO", "cms"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestCMSInitByProbKeyExists(t *testing.T) {
	r := newTestRedis()

	resp := r.CMSInitByProb(cmd("CMS.INITBYPROB", "cms", "0.01", "0.01"))
	assert.Equal(t, constant.RESP_OK, resp)

	// Try to create again - should fail
	resp = r.CMSInitByProb(cmd("CMS.INITBYPROB", "cms", "0.1", "0.1"))
	assert.Equal(t, constant.RESP_CMS_KEY_ALREADY_EXISTS, resp)
}

func TestCMSInitByProbKeyExistsOtherType(t *testing.T) {
	r := newTestRedis()

	r.Set(cmd("SET", "k", "v"))

	resp := r.CMSInitByProb(cmd("CMS.INITBYPROB", "k", "0.01", "0.01"))
	assert.Equal(t, constant.RESP_CMS_KEY_ALREADY_EXISTS, resp)
}

func TestCMSInitByProbWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.CMSInitByProb(cmd("CMS.INITBYPROB", "cms"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	resp = r.CMSInitByProb(cmd("CMS.INITBYPROB", "cms", "0.01"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	resp = r.CMSInitByProb(cmd("CMS.INITBYPROB", "cms", "0.01", "0.01", "extra"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestCMSInitByProbBadErrorRate(t *testing.T) {
	r := newTestRedis()

	resp := r.CMSInitByProb(cmd("CMS.INITBYPROB", "cms", "invalid", "0.01"))
	assert.Equal(t, constant.RESP_BAD_ERROR_RATE, resp)
}

func TestCMSInitByProbErrorRateOutOfRange(t *testing.T) {
	r := newTestRedis()

	// Error rate <= 0
	resp := r.CMSInitByProb(cmd("CMS.INITBYPROB", "cms", "0", "0.01"))
	assert.Equal(t, constant.RESP_ERROR_RATE_INVALID_RANGE, resp)

	resp = r.CMSInitByProb(cmd("CMS.INITBYPROB", "cms", "-0.1", "0.01"))
	assert.Equal(t, constant.RESP_ERROR_RATE_INVALID_RANGE, resp)

	// Error rate >= 1
	resp = r.CMSInitByProb(cmd("CMS.INITBYPROB", "cms", "1", "0.01"))
	assert.Equal(t, constant.RESP_ERROR_RATE_INVALID_RANGE, resp)

	resp = r.CMSInitByProb(cmd("CMS.INITBYPROB", "cms", "1.5", "0.01"))
	assert.Equal(t, constant.RESP_ERROR_RATE_INVALID_RANGE, resp)
}

func TestCMSInitByProbBadProbability(t *testing.T) {
	r := newTestRedis()

	resp := r.CMSInitByProb(cmd("CMS.INITBYPROB", "cms", "0.01", "invalid"))
	assert.Equal(t, constant.RESP_CMS_BAD_PROBABILITY, resp)
}

func TestCMSInitByProbProbabilityOutOfRange(t *testing.T) {
	r := newTestRedis()

	// Probability <= 0
	resp := r.CMSInitByProb(cmd("CMS.INITBYPROB", "cms", "0.01", "0"))
	assert.Equal(t, constant.RESP_CMS_PROBABILITY_INVALID_RANGE, resp)

	resp = r.CMSInitByProb(cmd("CMS.INITBYPROB", "cms", "0.01", "-0.1"))
	assert.Equal(t, constant.RESP_CMS_PROBABILITY_INVALID_RANGE, resp)

	// Probability >= 1
	resp = r.CMSInitByProb(cmd("CMS.INITBYPROB", "cms", "0.01", "1"))
	assert.Equal(t, constant.RESP_CMS_PROBABILITY_INVALID_RANGE, resp)

	resp = r.CMSInitByProb(cmd("CMS.INITBYPROB", "cms", "0.01", "1.5"))
	assert.Equal(t, constant.RESP_CMS_PROBABILITY_INVALID_RANGE, resp)
}

func TestCMSIncrBy(t *testing.T) {
	r := newTestRedis()

	r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "100", "5"))

	resp := r.CMSIncrBy(cmd("CMS.INCRBY", "cms", "item1", "5"))
	expected := "*1\r\n:5\r\n"
	assert.Equal(t, expected, string(resp))
}

func TestCMSIncrByMultipleItems(t *testing.T) {
	r := newTestRedis()

	r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "100", "5"))

	resp := r.CMSIncrBy(cmd("CMS.INCRBY", "cms", "item1", "5", "item2", "10", "item3", "15"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestCMSIncrByAccumulate(t *testing.T) {
	r := newTestRedis()

	r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "100", "5"))

	// First increment
	resp := r.CMSIncrBy(cmd("CMS.INCRBY", "cms", "item1", "5"))
	expected := "*1\r\n:5\r\n"
	assert.Equal(t, expected, string(resp))

	// Second increment
	resp = r.CMSIncrBy(cmd("CMS.INCRBY", "cms", "item1", "3"))
	expected = "*1\r\n:8\r\n"
	assert.Equal(t, expected, string(resp))

	// Verify via query
	resp = r.CMSQuery(cmd("CMS.QUERY", "cms", "item1"))
	expected = "*1\r\n:8\r\n"
	assert.Equal(t, expected, string(resp))
}

func TestCMSIncrByNonExistingKey(t *testing.T) {
	r := newTestRedis()

	resp := r.CMSIncrBy(cmd("CMS.INCRBY", "nonexistent", "item1", "5"))
	assert.Equal(t, constant.RESP_CMS_KEY_DOES_NOT_EXIST, resp)
}

func TestCMSIncrByWrongType(t *testing.T) {
	r := newTestRedis()

	r.Set(cmd("SET", "k", "v"))

	resp := r.CMSIncrBy(cmd("CMS.INCRBY", "k", "item1", "5"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestCMSIncrByWrongArgs(t *testing.T) {
	r := newTestRedis()

	r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "100", "5"))

	// Too few args
	resp := r.CMSIncrBy(cmd("CMS.INCRBY", "cms"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	resp = r.CMSIncrBy(cmd("CMS.INCRBY", "cms", "item1"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	// Odd number of item/increment pairs (missing increment)
	resp = r.CMSIncrBy(cmd("CMS.INCRBY", "cms", "item1", "5", "item2"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestCMSIncrByBadIncrement(t *testing.T) {
	r := newTestRedis()

	r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "100", "5"))

	resp := r.CMSIncrBy(cmd("CMS.INCRBY", "cms", "item1", "invalid"))
	assert.Equal(t, constant.RESP_CMS_BAD_INCREMENT, resp)

	resp = r.CMSIncrBy(cmd("CMS.INCRBY", "cms", "item1", "-5"))
	assert.Equal(t, constant.RESP_CMS_BAD_INCREMENT, resp)
}

func TestCMSQuery(t *testing.T) {
	r := newTestRedis()

	r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "100", "5"))
	r.CMSIncrBy(cmd("CMS.INCRBY", "cms", "item1", "10"))

	resp := r.CMSQuery(cmd("CMS.QUERY", "cms", "item1"))
	expected := "*1\r\n:10\r\n"
	assert.Equal(t, expected, string(resp))
}

func TestCMSQueryMultipleItems(t *testing.T) {
	r := newTestRedis()

	r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "100", "5"))
	r.CMSIncrBy(cmd("CMS.INCRBY", "cms", "apple", "5"))
	r.CMSIncrBy(cmd("CMS.INCRBY", "cms", "banana", "10"))
	r.CMSIncrBy(cmd("CMS.INCRBY", "cms", "cherry", "15"))

	resp := r.CMSQuery(cmd("CMS.QUERY", "cms", "apple", "banana", "cherry"))
	expected := "*3\r\n:5\r\n:10\r\n:15\r\n"
	assert.Equal(t, expected, string(resp))
}

func TestCMSQueryNonExistentItem(t *testing.T) {
	r := newTestRedis()

	r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "100", "5"))
	r.CMSIncrBy(cmd("CMS.INCRBY", "cms", "item1", "10"))

	resp := r.CMSQuery(cmd("CMS.QUERY", "cms", "nonexistent"))
	expected := "*1\r\n:0\r\n"
	assert.Equal(t, expected, string(resp))
}

func TestCMSQueryNonExistingKey(t *testing.T) {
	r := newTestRedis()

	resp := r.CMSQuery(cmd("CMS.QUERY", "nonexistent", "item1"))
	assert.Equal(t, constant.RESP_CMS_KEY_DOES_NOT_EXIST, resp)
}

func TestCMSQueryWrongType(t *testing.T) {
	r := newTestRedis()

	r.Set(cmd("SET", "k", "v"))

	resp := r.CMSQuery(cmd("CMS.QUERY", "k", "item1"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestCMSQueryWrongArgs(t *testing.T) {
	r := newTestRedis()

	r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "100", "5"))

	// Too few args
	resp := r.CMSQuery(cmd("CMS.QUERY", "cms"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestCMSInfo(t *testing.T) {
	r := newTestRedis()

	r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "100", "5"))
	r.CMSIncrBy(cmd("CMS.INCRBY", "cms", "item1", "50"))

	resp := r.CMSInfo(cmd("CMS.INFO", "cms"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
	// Should contain width, depth, count
	assert.Contains(t, string(resp), "width")
	assert.Contains(t, string(resp), "depth")
	assert.Contains(t, string(resp), "count")
}

func TestCMSInfoNonExistingKey(t *testing.T) {
	r := newTestRedis()

	resp := r.CMSInfo(cmd("CMS.INFO", "nonexistent"))
	assert.Equal(t, constant.RESP_CMS_KEY_DOES_NOT_EXIST, resp)
}

func TestCMSInfoWrongType(t *testing.T) {
	r := newTestRedis()

	r.Set(cmd("SET", "k", "v"))

	resp := r.CMSInfo(cmd("CMS.INFO", "k"))
	assert.Equal(t, constant.RESP_WRONGTYPE_OPERATION_AGAINST_KEY, resp)
}

func TestCMSInfoWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.CMSInfo(cmd("CMS.INFO"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	resp = r.CMSInfo(cmd("CMS.INFO", "cms", "extra"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestCMSWorkflow(t *testing.T) {
	r := newTestRedis()

	// Initialize CMS by dimensions
	resp := r.CMSInitByDim(cmd("CMS.INITBYDIM", "pageviews", "1000", "5"))
	assert.Equal(t, constant.RESP_OK, resp)

	// Track page views
	r.CMSIncrBy(cmd("CMS.INCRBY", "pageviews", "/home", "100", "/about", "50", "/contact", "25"))

	// More views on home page
	r.CMSIncrBy(cmd("CMS.INCRBY", "pageviews", "/home", "50"))

	// Query counts
	resp = r.CMSQuery(cmd("CMS.QUERY", "pageviews", "/home", "/about", "/contact"))
	expected := "*3\r\n:150\r\n:50\r\n:25\r\n"
	assert.Equal(t, expected, string(resp))

	// Check info
	resp = r.CMSInfo(cmd("CMS.INFO", "pageviews"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestCMSWorkflowByProb(t *testing.T) {
	r := newTestRedis()

	// Initialize CMS by probability
	resp := r.CMSInitByProb(cmd("CMS.INITBYPROB", "events", "0.001", "0.01"))
	assert.Equal(t, constant.RESP_OK, resp)

	// Track events
	r.CMSIncrBy(cmd("CMS.INCRBY", "events", "click", "1000", "scroll", "5000", "purchase", "50"))

	// Query counts
	clickCount := r.CMSQuery(cmd("CMS.QUERY", "events", "click"))
	assert.Contains(t, string(clickCount), ":1000\r\n")

	scrollCount := r.CMSQuery(cmd("CMS.QUERY", "events", "scroll"))
	assert.Contains(t, string(scrollCount), ":5000\r\n")

	purchaseCount := r.CMSQuery(cmd("CMS.QUERY", "events", "purchase"))
	assert.Contains(t, string(purchaseCount), ":50\r\n")
}

func TestCMSMultipleKeys(t *testing.T) {
	r := newTestRedis()

	// Create multiple CMS instances
	r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms1", "100", "5"))
	r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms2", "200", "10"))

	// Add to each
	r.CMSIncrBy(cmd("CMS.INCRBY", "cms1", "item", "10"))
	r.CMSIncrBy(cmd("CMS.INCRBY", "cms2", "item", "20"))

	// Verify they are independent
	resp1 := r.CMSQuery(cmd("CMS.QUERY", "cms1", "item"))
	resp2 := r.CMSQuery(cmd("CMS.QUERY", "cms2", "item"))

	assert.Equal(t, "*1\r\n:10\r\n", string(resp1))
	assert.Equal(t, "*1\r\n:20\r\n", string(resp2))
}

func TestCMSLargeIncrement(t *testing.T) {
	r := newTestRedis()

	r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "100", "5"))

	// Large increment
	resp := r.CMSIncrBy(cmd("CMS.INCRBY", "cms", "item1", "1000000"))
	assert.Contains(t, string(resp), ":1000000\r\n")

	// Query to verify
	resp = r.CMSQuery(cmd("CMS.QUERY", "cms", "item1"))
	assert.Contains(t, string(resp), ":1000000\r\n")
}

func TestCMSZeroIncrement(t *testing.T) {
	r := newTestRedis()

	r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "100", "5"))

	// Zero increment should be valid
	resp := r.CMSIncrBy(cmd("CMS.INCRBY", "cms", "item1", "0"))
	expected := "*1\r\n:0\r\n"
	assert.Equal(t, expected, string(resp))
}

func TestCMSSpecialCharacters(t *testing.T) {
	r := newTestRedis()

	r.CMSInitByDim(cmd("CMS.INITBYDIM", "cms", "100", "5"))

	// Items with special characters
	r.CMSIncrBy(cmd("CMS.INCRBY", "cms", "hello world", "1"))
	r.CMSIncrBy(cmd("CMS.INCRBY", "cms", "unicode:你好", "2"))

	resp := r.CMSQuery(cmd("CMS.QUERY", "cms", "hello world"))
	assert.Contains(t, string(resp), ":1\r\n")

	resp = r.CMSQuery(cmd("CMS.QUERY", "cms", "unicode:你好"))
	assert.Contains(t, string(resp), ":2\r\n")
}
