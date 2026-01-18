package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manhhung2111/go-redis/internal/core"
)

// ==================== GEOADD Tests ====================

func TestGeoAddBasic(t *testing.T) {
	r := newTestRedis()

	resp := r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "san_francisco"))
	assert.Equal(t, []byte(":1\r\n"), resp)
}

func TestGeoAddMultiple(t *testing.T) {
	r := newTestRedis()

	resp := r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf", "-118.2437", "34.0522", "la"))
	assert.Equal(t, []byte(":2\r\n"), resp)
}

func TestGeoAddUpdate(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))
	resp := r.GeoAdd(cmd("GEOADD", "geo", "-122.0", "37.0", "sf"))
	assert.Equal(t, []byte(":0\r\n"), resp)
}

func TestGeoAddNX(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))
	resp := r.GeoAdd(cmd("GEOADD", "geo", "NX", "-122.0", "37.0", "sf"))
	assert.Equal(t, []byte(":0\r\n"), resp)

	// NX should allow adding new members
	resp = r.GeoAdd(cmd("GEOADD", "geo", "NX", "-118.2437", "34.0522", "la"))
	assert.Equal(t, []byte(":1\r\n"), resp)
}

func TestGeoAddXX(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	// XX should block adding new members
	resp := r.GeoAdd(cmd("GEOADD", "geo", "XX", "-118.2437", "34.0522", "la"))
	assert.Equal(t, []byte(":0\r\n"), resp)

	// XX should allow updating existing members
	resp = r.GeoAdd(cmd("GEOADD", "geo", "XX", "-122.0", "37.0", "sf"))
	assert.Equal(t, []byte(":0\r\n"), resp)
}

func TestGeoAddCH(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))
	resp := r.GeoAdd(cmd("GEOADD", "geo", "CH", "-122.0", "37.0", "sf"))
	assert.Equal(t, []byte(":1\r\n"), resp)
}

func TestGeoAddWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.GeoAdd(cmd("GEOADD", "geo"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	resp = r.GeoAdd(cmd("GEOADD", "geo", "-122.4194"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	resp = r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestGeoAddWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.GeoAdd(cmd("GEOADD", "k", "-122.4194", "37.7749", "sf"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestGeoAddInvalidLongitude(t *testing.T) {
	r := newTestRedis()

	// "abc" is parsed as option first, so it returns syntax error
	resp := r.GeoAdd(cmd("GEOADD", "geo", "abc", "37.7749", "sf"))
	assert.Equal(t, core.RespSyntaxError, resp)
}

func TestGeoAddInvalidLatitude(t *testing.T) {
	r := newTestRedis()

	resp := r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "abc", "sf"))
	assert.Equal(t, core.RespValueNotValidFloat, resp)
}

func TestGeoAddInvalidCoordinates(t *testing.T) {
	r := newTestRedis()

	// Longitude out of range
	resp := r.GeoAdd(cmd("GEOADD", "geo", "200.0", "37.7749", "sf"))
	assert.Equal(t, core.RespInvalidLongitudeLatitude, resp)

	// Latitude out of range
	resp = r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "90.0", "sf"))
	assert.Equal(t, core.RespInvalidLongitudeLatitude, resp)
}

func TestGeoAddNXXXConflict(t *testing.T) {
	r := newTestRedis()

	resp := r.GeoAdd(cmd("GEOADD", "geo", "NX", "XX", "-122.4194", "37.7749", "sf"))
	assert.Equal(t, core.RespXXNXNotCompatible, resp)
}

func TestGeoAddInvalidOption(t *testing.T) {
	r := newTestRedis()

	resp := r.GeoAdd(cmd("GEOADD", "geo", "INVALID", "-122.4194", "37.7749", "sf"))
	assert.Equal(t, core.RespSyntaxError, resp)
}

func TestGeoAddIncompleteTriplet(t *testing.T) {
	r := newTestRedis()

	// Only longitude and latitude, missing member
	resp := r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf", "-118.2437", "34.0522"))
	assert.Equal(t, core.RespSyntaxError, resp)
}

// ==================== GEODIST Tests ====================

func TestGeoDistBasic(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf", "-118.2437", "34.0522", "la"))

	resp := r.GeoDist(cmd("GEODIST", "geo", "sf", "la"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('$'), resp[0])
}

func TestGeoDistWithUnit(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf", "-118.2437", "34.0522", "la"))

	resp := r.GeoDist(cmd("GEODIST", "geo", "sf", "la", "km"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('$'), resp[0])

	resp = r.GeoDist(cmd("GEODIST", "geo", "sf", "la", "mi"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('$'), resp[0])

	resp = r.GeoDist(cmd("GEODIST", "geo", "sf", "la", "ft"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('$'), resp[0])

	resp = r.GeoDist(cmd("GEODIST", "geo", "sf", "la", "m"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('$'), resp[0])
}

func TestGeoDistMissingKey(t *testing.T) {
	r := newTestRedis()

	resp := r.GeoDist(cmd("GEODIST", "geo", "sf", "la"))
	assert.Equal(t, core.RespNilBulkString, resp)
}

func TestGeoDistMissingMember(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	resp := r.GeoDist(cmd("GEODIST", "geo", "sf", "la"))
	assert.Equal(t, core.RespNilBulkString, resp)

	resp = r.GeoDist(cmd("GEODIST", "geo", "unknown", "sf"))
	assert.Equal(t, core.RespNilBulkString, resp)
}

func TestGeoDistWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.GeoDist(cmd("GEODIST", "geo", "sf"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])

	resp = r.GeoDist(cmd("GEODIST", "geo", "sf", "la", "km", "extra"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestGeoDistWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.GeoDist(cmd("GEODIST", "k", "sf", "la"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestGeoDistInvalidUnit(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf", "-118.2437", "34.0522", "la"))

	resp := r.GeoDist(cmd("GEODIST", "geo", "sf", "la", "invalid"))
	assert.Equal(t, core.RespSyntaxError, resp)
}

// ==================== GEOHASH Tests ====================

func TestGeoHashBasic(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	resp := r.GeoHash(cmd("GEOHASH", "geo", "sf"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoHashMultiple(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf", "-118.2437", "34.0522", "la"))

	resp := r.GeoHash(cmd("GEOHASH", "geo", "sf", "la"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoHashMissingMember(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	resp := r.GeoHash(cmd("GEOHASH", "geo", "sf", "unknown"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoHashMissingKey(t *testing.T) {
	r := newTestRedis()

	resp := r.GeoHash(cmd("GEOHASH", "geo", "sf"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoHashWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.GeoHash(cmd("GEOHASH", "geo"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestGeoHashWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.GeoHash(cmd("GEOHASH", "k", "sf"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

// ==================== GEOPOS Tests ====================

func TestGeoPosBasic(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	resp := r.GeoPos(cmd("GEOPOS", "geo", "sf"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoPosMultiple(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf", "-118.2437", "34.0522", "la"))

	resp := r.GeoPos(cmd("GEOPOS", "geo", "sf", "la"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoPosMissingMember(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	resp := r.GeoPos(cmd("GEOPOS", "geo", "sf", "unknown"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoPosMissingKey(t *testing.T) {
	r := newTestRedis()

	resp := r.GeoPos(cmd("GEOPOS", "geo", "sf"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoPosWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.GeoPos(cmd("GEOPOS", "geo"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestGeoPosWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.GeoPos(cmd("GEOPOS", "k", "sf"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

// ==================== GEOSEARCH Tests ====================

func TestGeoSearchByRadiusFromMember(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf", "-121.8863", "37.3382", "sj"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100", "km"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoSearchByRadiusFromLonLat(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf", "-121.8863", "37.3382", "sj"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMLONLAT", "-122.4194", "37.7749", "BYRADIUS", "100", "km"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoSearchByBox(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf", "-121.8863", "37.3382", "sj"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYBOX", "200", "200", "km"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoSearchASC(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf", "-121.8863", "37.3382", "sj"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100", "km", "ASC"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoSearchDESC(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf", "-121.8863", "37.3382", "sj"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100", "km", "DESC"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoSearchCount(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf", "-121.8863", "37.3382", "sj", "-122.0322", "37.3688", "sv"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100", "km", "COUNT", "2"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoSearchCountAny(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf", "-121.8863", "37.3382", "sj", "-122.0322", "37.3688", "sv"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100", "km", "COUNT", "2", "ANY"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoSearchWithCoord(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100", "km", "WITHCOORD"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoSearchWithDist(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100", "km", "WITHDIST"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoSearchWithHash(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100", "km", "WITHHASH"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoSearchWithAllOptions(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf", "-121.8863", "37.3382", "sj"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100", "km", "ASC", "COUNT", "10", "WITHCOORD", "WITHDIST", "WITHHASH"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoSearchMissingKey(t *testing.T) {
	r := newTestRedis()

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100", "km"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
	assert.Equal(t, []byte("*0\r\n"), resp)
}

func TestGeoSearchWrongArgs(t *testing.T) {
	r := newTestRedis()

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('-'), resp[0])
}

func TestGeoSearchWrongType(t *testing.T) {
	r := newTestRedis()
	r.Set(cmd("SET", "k", "v"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "k", "FROMMEMBER", "sf", "BYRADIUS", "100", "km"))
	assert.Equal(t, core.RespWrongTypeOperation, resp)
}

func TestGeoSearchMissingFrom(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "BYRADIUS", "100", "km"))
	assert.Equal(t, core.RespGeoFromMemberOrFromLonLatReq, resp)
}

func TestGeoSearchBothFromOptions(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "FROMLONLAT", "-122.0", "37.0", "BYRADIUS", "100", "km"))
	assert.Equal(t, core.RespGeoFromMemberOrFromLonLatReq, resp)
}

func TestGeoSearchMissingBy(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	// Need at least 4 args, provide extra args but no BYRADIUS/BYBOX
	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "ASC"))
	assert.Equal(t, core.RespGeoByRadiusOrByBoxReq, resp)
}

func TestGeoSearchBothByOptions(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100", "km", "BYBOX", "200", "200", "km"))
	assert.Equal(t, core.RespGeoByRadiusOrByBoxReq, resp)
}

func TestGeoSearchASCDESCConflict(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100", "km", "ASC", "DESC"))
	assert.Equal(t, core.RespSyntaxError, resp)
}

func TestGeoSearchInvalidFromLonLat(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	// Invalid longitude
	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMLONLAT", "abc", "37.0", "BYRADIUS", "100", "km"))
	assert.Equal(t, core.RespValueNotValidFloat, resp)

	// Invalid latitude
	resp = r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMLONLAT", "-122.0", "abc", "BYRADIUS", "100", "km"))
	assert.Equal(t, core.RespValueNotValidFloat, resp)
}

func TestGeoSearchInvalidCoordinates(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	// Longitude out of range
	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMLONLAT", "200.0", "37.0", "BYRADIUS", "100", "km"))
	assert.Equal(t, core.RespInvalidLongitudeLatitude, resp)

	// Latitude out of range
	resp = r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMLONLAT", "-122.0", "90.0", "BYRADIUS", "100", "km"))
	assert.Equal(t, core.RespInvalidLongitudeLatitude, resp)
}

func TestGeoSearchInvalidRadius(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "abc", "km"))
	assert.Equal(t, core.RespValueNotValidFloat, resp)

	// Negative radius
	resp = r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "-100", "km"))
	assert.Equal(t, core.RespValueNotValidFloat, resp)
}

func TestGeoSearchInvalidBoxDimensions(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	// Invalid width
	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYBOX", "abc", "200", "km"))
	assert.Equal(t, core.RespValueNotValidFloat, resp)

	// Invalid height
	resp = r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYBOX", "200", "abc", "km"))
	assert.Equal(t, core.RespValueNotValidFloat, resp)

	// Negative width
	resp = r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYBOX", "-200", "200", "km"))
	assert.Equal(t, core.RespValueNotValidFloat, resp)

	// Negative height
	resp = r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYBOX", "200", "-200", "km"))
	assert.Equal(t, core.RespValueNotValidFloat, resp)
}

func TestGeoSearchInvalidUnit(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100", "invalid"))
	assert.Equal(t, core.RespSyntaxError, resp)

	resp = r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYBOX", "200", "200", "invalid"))
	assert.Equal(t, core.RespSyntaxError, resp)
}

func TestGeoSearchInvalidCount(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100", "km", "COUNT", "abc"))
	assert.Equal(t, core.RespValueOutOfRangeMustPositive, resp)

	resp = r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100", "km", "COUNT", "-1"))
	assert.Equal(t, core.RespValueOutOfRangeMustPositive, resp)
}

func TestGeoSearchInvalidOption(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100", "km", "INVALID"))
	assert.Equal(t, core.RespSyntaxError, resp)
}

func TestGeoSearchFromMemberNotFound(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "unknown", "BYRADIUS", "100", "km"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
	assert.Equal(t, []byte("*0\r\n"), resp)
}

func TestGeoSearchIncompleteFromMember(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	// FROMMEMBER at end without member - needs more context to trigger syntax error
	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "BYRADIUS", "100", "km", "FROMMEMBER"))
	assert.Equal(t, core.RespSyntaxError, resp)
}

func TestGeoSearchIncompleteFromLonLat(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	// FROMLONLAT with only one coordinate
	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "BYRADIUS", "100", "km", "FROMLONLAT", "-122.0"))
	assert.Equal(t, core.RespSyntaxError, resp)
}

func TestGeoSearchIncompleteByRadius(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100"))
	assert.Equal(t, core.RespSyntaxError, resp)
}

func TestGeoSearchIncompleteByBox(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYBOX", "200", "200"))
	assert.Equal(t, core.RespSyntaxError, resp)
}

func TestGeoSearchIncompleteCount(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100", "km", "COUNT"))
	assert.Equal(t, core.RespSyntaxError, resp)
}

func TestGeoSearchAllUnits(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf", "-121.8863", "37.3382", "sj"))

	units := []string{"m", "km", "mi", "ft"}
	for _, unit := range units {
		resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100000", unit))
		require.NotEmpty(t, resp, "unit: %s", unit)
		assert.Equal(t, byte('*'), resp[0], "unit: %s", unit)
	}
}

func TestGeoSearchByBoxAllUnits(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf", "-121.8863", "37.3382", "sj"))

	units := []string{"m", "km", "mi", "ft"}
	for _, unit := range units {
		resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYBOX", "200000", "200000", unit))
		require.NotEmpty(t, resp, "unit: %s", unit)
		assert.Equal(t, byte('*'), resp[0], "unit: %s", unit)
	}
}

func TestGeoSearchResultsNil(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))
	// Search from a member that doesn't exist - should return nil from store
	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "nonexistent", "BYRADIUS", "100", "km"))
	require.NotEmpty(t, resp)
	assert.Equal(t, []byte("*0\r\n"), resp)
}

// ==================== Integration Tests ====================

func TestGeoIntegration(t *testing.T) {
	r := newTestRedis()

	// Add cities
	resp := r.GeoAdd(cmd("GEOADD", "cities", "13.361389", "52.519444", "berlin", "2.349014", "48.864716", "paris", "-0.118092", "51.509865", "london"))
	assert.Equal(t, []byte(":3\r\n"), resp)

	// Get distance
	resp = r.GeoDist(cmd("GEODIST", "cities", "berlin", "paris", "km"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('$'), resp[0])

	// Get geohash
	resp = r.GeoHash(cmd("GEOHASH", "cities", "berlin", "paris"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])

	// Get positions
	resp = r.GeoPos(cmd("GEOPOS", "cities", "berlin", "london"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])

	// Search
	resp = r.GeoSearch(cmd("GEOSEARCH", "cities", "FROMMEMBER", "paris", "BYRADIUS", "1000", "km", "ASC", "WITHCOORD", "WITHDIST"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoSearchWithDistOnlyOption(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf", "-121.8863", "37.3382", "sj"))

	// Test with only WITHDIST (should return array of arrays)
	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100", "km", "WITHDIST"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoSearchWithHashOnlyOption(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	// Test with only WITHHASH (should return array of arrays)
	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100", "km", "WITHHASH"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoSearchWithCoordOnlyOption(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf"))

	// Test with only WITHCOORD (should return array of arrays)
	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100", "km", "WITHCOORD"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}

func TestGeoSearchNoWithOptions(t *testing.T) {
	r := newTestRedis()

	r.GeoAdd(cmd("GEOADD", "geo", "-122.4194", "37.7749", "sf", "-121.8863", "37.3382", "sj"))

	// Without any WITH options - should return simple array of member names
	resp := r.GeoSearch(cmd("GEOSEARCH", "geo", "FROMMEMBER", "sf", "BYRADIUS", "100", "km"))
	require.NotEmpty(t, resp)
	assert.Equal(t, byte('*'), resp[0])
}
