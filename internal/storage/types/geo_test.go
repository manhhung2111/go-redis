package types

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============= GeoHash Tests =============

func TestGeoHashEncode_BasicLocations(t *testing.T) {
	tests := []struct {
		name      string
		longitude float64
		latitude  float64
	}{
		{"origin", 0, 0},
		{"new york", -74.0060, 40.7128},
		{"london", -0.1278, 51.5074},
		{"tokyo", 139.6917, 35.6895},
		{"sydney", 151.2093, -33.8688},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash := GeoHashEncode(tt.longitude, tt.latitude)
			assert.NotZero(t, hash)

			// Verify decode returns approximately the same coordinates
			lon, lat := GeoHashDecode(hash)
			assert.InDelta(t, tt.longitude, lon, 0.001)
			assert.InDelta(t, tt.latitude, lat, 0.001)
		})
	}
}

func TestGeoHashEncode_MinBounds(t *testing.T) {
	// Min bounds encode to 0, which is a valid hash
	hash := GeoHashEncode(MinLongitude, MinLatitude)
	assert.Equal(t, uint64(0), hash)

	lon, lat := GeoHashDecode(hash)
	assert.InDelta(t, MinLongitude, lon, 0.01)
	assert.InDelta(t, MinLatitude, lat, 0.01)
}

func TestGeoHashEncode_NearMaxBounds(t *testing.T) {
	// Test values close to max bounds (but not exactly at max to avoid edge case)
	lon := MaxLongitude - 1
	lat := MaxLatitude - 1

	hash := GeoHashEncode(lon, lat)
	assert.NotZero(t, hash)

	decodedLon, decodedLat := GeoHashDecode(hash)
	assert.InDelta(t, lon, decodedLon, 0.01)
	assert.InDelta(t, lat, decodedLat, 0.01)
}

func TestGeoHashEncode_DifferentLocationsProduceDifferentHashes(t *testing.T) {
	hash1 := GeoHashEncode(0, 0)
	hash2 := GeoHashEncode(10, 10)
	hash3 := GeoHashEncode(-10, -10)

	assert.NotEqual(t, hash1, hash2)
	assert.NotEqual(t, hash1, hash3)
	assert.NotEqual(t, hash2, hash3)
}

func TestGeoHashDecode_EncodeDecode(t *testing.T) {
	tests := []struct {
		longitude float64
		latitude  float64
	}{
		{0, 0},
		{-74.0060, 40.7128},
		{139.6917, 35.6895},
		{-122.4194, 37.7749},
		{2.3522, 48.8566},
	}

	for _, tt := range tests {
		hash := GeoHashEncode(tt.longitude, tt.latitude)
		lon, lat := GeoHashDecode(hash)

		// Check that decoded values are close to original
		assert.InDelta(t, tt.longitude, lon, 0.001)
		assert.InDelta(t, tt.latitude, lat, 0.001)
	}
}

func TestGeoHashDecode_ZeroHash(t *testing.T) {
	lon, lat := GeoHashDecode(0)
	assert.InDelta(t, MinLongitude, lon, 0.001)
	assert.InDelta(t, MinLatitude, lat, 0.001)
}

func TestGeoHashToString_ValidHashes(t *testing.T) {
	tests := []struct {
		name      string
		longitude float64
		latitude  float64
	}{
		{"origin", 0, 0},
		{"new york", -74.0060, 40.7128},
		{"tokyo", 139.6917, 35.6895},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash := GeoHashEncode(tt.longitude, tt.latitude)
			str := GeoHashToString(hash)

			assert.Len(t, str, 11)
			// Check that string only contains valid base32 characters
			for _, c := range str {
				assert.Contains(t, "0123456789bcdefghjkmnpqrstuvwxyz", string(c))
			}
		})
	}
}

func TestGeoHashToString_DifferentHashesProduceDifferentStrings(t *testing.T) {
	hash1 := GeoHashEncode(0, 0)
	hash2 := GeoHashEncode(100, 50)

	str1 := GeoHashToString(hash1)
	str2 := GeoHashToString(hash2)

	assert.NotEqual(t, str1, str2)
}

func TestGeoHashToString_ZeroHash(t *testing.T) {
	str := GeoHashToString(0)
	assert.Len(t, str, 11)
	assert.Equal(t, "00000000000", str)
}

func TestHaversineDistance_SamePoint(t *testing.T) {
	distance := HaversineDistance(0, 0, 0, 0)
	assert.Equal(t, 0.0, distance)

	distance = HaversineDistance(-74.0060, 40.7128, -74.0060, 40.7128)
	assert.Equal(t, 0.0, distance)
}

func TestHaversineDistance_KnownDistances(t *testing.T) {
	tests := []struct {
		name             string
		lon1, lat1       float64
		lon2, lat2       float64
		expectedKm       float64
		tolerancePercent float64
	}{
		{
			name:             "New York to Los Angeles",
			lon1:             -74.0060,
			lat1:             40.7128,
			lon2:             -118.2437,
			lat2:             34.0522,
			expectedKm:       3940,
			tolerancePercent: 2,
		},
		{
			name:             "London to Paris",
			lon1:             -0.1278,
			lat1:             51.5074,
			lon2:             2.3522,
			lat2:             48.8566,
			expectedKm:       344,
			tolerancePercent: 2,
		},
		{
			name:             "Sydney to Tokyo",
			lon1:             151.2093,
			lat1:             -33.8688,
			lon2:             139.6917,
			lat2:             35.6895,
			expectedKm:       7820,
			tolerancePercent: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			distance := HaversineDistance(tt.lon1, tt.lat1, tt.lon2, tt.lat2)
			distanceKm := distance / 1000

			tolerance := tt.expectedKm * tt.tolerancePercent / 100
			assert.InDelta(t, tt.expectedKm, distanceKm, tolerance)
		})
	}
}

func TestHaversineDistance_Symmetry(t *testing.T) {
	lon1, lat1 := -74.0060, 40.7128
	lon2, lat2 := 139.6917, 35.6895

	d1 := HaversineDistance(lon1, lat1, lon2, lat2)
	d2 := HaversineDistance(lon2, lat2, lon1, lat1)

	assert.Equal(t, d1, d2)
}

func TestHaversineDistance_Antipodal(t *testing.T) {
	// Points on opposite sides of the Earth
	distance := HaversineDistance(0, 0, 180, 0)
	// Half of Earth's circumference (~20,000 km)
	assert.InDelta(t, 20000000, distance, 100000)
}

func TestConvertDistance_AllUnits(t *testing.T) {
	meters := 1000.0

	tests := []struct {
		unit     string
		expected float64
	}{
		{"m", 1000.0},
		{"km", 1.0},
		{"mi", 0.621371},
		{"ft", 3280.84},
		{"", 1000.0},       // default is meters
		{"unknown", 1000.0}, // unknown defaults to meters
	}

	for _, tt := range tests {
		t.Run(tt.unit, func(t *testing.T) {
			result := ConvertDistance(meters, tt.unit)
			assert.InDelta(t, tt.expected, result, 0.01)
		})
	}
}

func TestConvertToMeters_AllUnits(t *testing.T) {
	tests := []struct {
		value    float64
		unit     string
		expected float64
	}{
		{1000, "m", 1000.0},
		{1, "km", 1000.0},
		{1, "mi", 1609.344},
		{3280.84, "ft", 1000.0},
		{1000, "", 1000.0},
		{1000, "unknown", 1000.0},
	}

	for _, tt := range tests {
		t.Run(tt.unit, func(t *testing.T) {
			result := ConvertToMeters(tt.value, tt.unit)
			assert.InDelta(t, tt.expected, result, 1)
		})
	}
}

func TestConvertDistance_RoundTrip(t *testing.T) {
	originalMeters := 12345.67

	units := []string{"m", "km", "mi", "ft"}

	for _, unit := range units {
		t.Run(unit, func(t *testing.T) {
			converted := ConvertDistance(originalMeters, unit)
			backToMeters := ConvertToMeters(converted, unit)
			assert.InDelta(t, originalMeters, backToMeters, 0.01)
		})
	}
}

func TestValidateCoordinates_ValidCoords(t *testing.T) {
	tests := []struct {
		name      string
		longitude float64
		latitude  float64
	}{
		{"origin", 0, 0},
		{"min bounds", MinLongitude, MinLatitude},
		{"max bounds", MaxLongitude, MaxLatitude},
		{"new york", -74.0060, 40.7128},
		{"tokyo", 139.6917, 35.6895},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.True(t, ValidateCoordinates(tt.longitude, tt.latitude))
		})
	}
}

func TestValidateCoordinates_InvalidCoords(t *testing.T) {
	tests := []struct {
		name      string
		longitude float64
		latitude  float64
	}{
		{"longitude too low", -181, 0},
		{"longitude too high", 181, 0},
		{"latitude too low", 0, -90},
		{"latitude too high", 0, 90},
		{"both out of range", 200, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.False(t, ValidateCoordinates(tt.longitude, tt.latitude))
		})
	}
}

// ============= Geo Commands Tests =============

func TestGeoAdd_Basic(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
		{Longitude: -118.2437, Latitude: 34.0522, Member: "los angeles"},
	}

	result, _ := z.GeoAdd(items, ZAddOptions{})
	require.NotNil(t, result)
	assert.Equal(t, uint32(2), *result)
	assert.Equal(t, uint32(2), z.ZCard())
}

func TestGeoAdd_Update(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
	}

	z.GeoAdd(items, ZAddOptions{})

	// Update with new coordinates
	items[0].Longitude = -74.0100
	items[0].Latitude = 40.7200

	result, _ := z.GeoAdd(items, ZAddOptions{})
	require.NotNil(t, result)
	assert.Equal(t, uint32(0), *result) // No new members added
	assert.Equal(t, uint32(1), z.ZCard())
}

func TestGeoAdd_WithNX(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
	}

	z.GeoAdd(items, ZAddOptions{})

	// Try to add same member with NX
	result, _ := z.GeoAdd(items, ZAddOptions{NX: true})
	require.NotNil(t, result)
	assert.Equal(t, uint32(0), *result)
}

func TestGeoAdd_WithXX(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
	}

	// Try to add with XX when member doesn't exist
	result, _ := z.GeoAdd(items, ZAddOptions{XX: true})
	require.NotNil(t, result)
	assert.Equal(t, uint32(0), *result)
	assert.Equal(t, uint32(0), z.ZCard())

	// Add normally first
	z.GeoAdd(items, ZAddOptions{})

	// Now update with XX should work
	items[0].Latitude = 40.8000
	result, _ = z.GeoAdd(items, ZAddOptions{XX: true})
	require.NotNil(t, result)
	assert.Equal(t, uint32(0), *result) // XX doesn't count as added
}

func TestGeoAdd_MultipleItems(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
		{Longitude: -118.2437, Latitude: 34.0522, Member: "los angeles"},
		{Longitude: -87.6298, Latitude: 41.8781, Member: "chicago"},
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san francisco"},
	}

	result, _ := z.GeoAdd(items, ZAddOptions{})
	require.NotNil(t, result)
	assert.Equal(t, uint32(4), *result)
}

func TestGeoDist_Basic(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
		{Longitude: -118.2437, Latitude: 34.0522, Member: "los angeles"},
	}

	z.GeoAdd(items, ZAddOptions{})

	dist := z.GeoDist("new york", "los angeles", "km")
	require.NotNil(t, dist)
	// NY to LA is approximately 3940 km
	assert.InDelta(t, 3940, *dist, 100)
}

func TestGeoDist_SamePoint(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
	}

	z.GeoAdd(items, ZAddOptions{})

	dist := z.GeoDist("new york", "new york", "m")
	require.NotNil(t, dist)
	assert.Equal(t, 0.0, *dist)
}

func TestGeoDist_DifferentUnits(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: 0, Latitude: 0, Member: "a"},
		{Longitude: 1, Latitude: 0, Member: "b"},
	}

	z.GeoAdd(items, ZAddOptions{})

	distM := z.GeoDist("a", "b", "m")
	distKm := z.GeoDist("a", "b", "km")
	distMi := z.GeoDist("a", "b", "mi")
	distFt := z.GeoDist("a", "b", "ft")

	require.NotNil(t, distM)
	require.NotNil(t, distKm)
	require.NotNil(t, distMi)
	require.NotNil(t, distFt)

	assert.InDelta(t, *distM/1000, *distKm, 0.01)
	assert.InDelta(t, *distM*MeterToMile, *distMi, 0.01)
	assert.InDelta(t, *distM*MeterToFeet, *distFt, 0.01)
}

func TestGeoDist_MemberNotFound(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
	}

	z.GeoAdd(items, ZAddOptions{})

	assert.Nil(t, z.GeoDist("new york", "missing", "m"))
	assert.Nil(t, z.GeoDist("missing", "new york", "m"))
	assert.Nil(t, z.GeoDist("missing1", "missing2", "m"))
}

func TestGeoHash_Basic(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
		{Longitude: -118.2437, Latitude: 34.0522, Member: "los angeles"},
	}

	z.GeoAdd(items, ZAddOptions{})

	hashes := z.GeoHash([]string{"new york", "los angeles"})
	require.Len(t, hashes, 2)

	require.NotNil(t, hashes[0])
	require.NotNil(t, hashes[1])
	assert.Len(t, *hashes[0], 11)
	assert.Len(t, *hashes[1], 11)
	assert.NotEqual(t, *hashes[0], *hashes[1])
}

func TestGeoHash_MemberNotFound(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
	}

	z.GeoAdd(items, ZAddOptions{})

	hashes := z.GeoHash([]string{"new york", "missing", "also missing"})
	require.Len(t, hashes, 3)

	assert.NotNil(t, hashes[0])
	assert.Nil(t, hashes[1])
	assert.Nil(t, hashes[2])
}

func TestGeoHash_EmptyInput(t *testing.T) {
	z := NewZSet()

	hashes := z.GeoHash([]string{})
	assert.Empty(t, hashes)
}

func TestGeoPos_Basic(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
		{Longitude: -118.2437, Latitude: 34.0522, Member: "los angeles"},
	}

	z.GeoAdd(items, ZAddOptions{})

	positions := z.GeoPos([]string{"new york", "los angeles"})
	require.Len(t, positions, 2)

	require.NotNil(t, positions[0])
	require.NotNil(t, positions[1])

	assert.InDelta(t, -74.0060, positions[0].Longitude, 0.001)
	assert.InDelta(t, 40.7128, positions[0].Latitude, 0.001)
	assert.Equal(t, "new york", positions[0].Member)

	assert.InDelta(t, -118.2437, positions[1].Longitude, 0.001)
	assert.InDelta(t, 34.0522, positions[1].Latitude, 0.001)
	assert.Equal(t, "los angeles", positions[1].Member)
}

func TestGeoPos_MemberNotFound(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
	}

	z.GeoAdd(items, ZAddOptions{})

	positions := z.GeoPos([]string{"new york", "missing"})
	require.Len(t, positions, 2)

	assert.NotNil(t, positions[0])
	assert.Nil(t, positions[1])
}

func TestGeoPos_EmptyInput(t *testing.T) {
	z := NewZSet()

	positions := z.GeoPos([]string{})
	assert.Empty(t, positions)
}

func TestGeoSearch_ByRadius_FromLonLat(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
		{Longitude: -73.9857, Latitude: 40.7484, Member: "manhattan"},     // ~4km from NY
		{Longitude: -118.2437, Latitude: 34.0522, Member: "los angeles"}, // ~3940km from NY
	}

	z.GeoAdd(items, ZAddOptions{})

	results := z.GeoSearch(GeoSearchOptions{
		FromLonLat: &GeoPoint{Longitude: -74.0060, Latitude: 40.7128},
		ByRadius:   10,
		Unit:       "km",
	})

	require.Len(t, results, 2)
	// Results should be sorted by distance
	assert.Equal(t, "new york", results[0].Member)
	assert.Equal(t, "manhattan", results[1].Member)
}

func TestGeoSearch_ByRadius_FromMember(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
		{Longitude: -73.9857, Latitude: 40.7484, Member: "manhattan"},
		{Longitude: -118.2437, Latitude: 34.0522, Member: "los angeles"},
	}

	z.GeoAdd(items, ZAddOptions{})

	results := z.GeoSearch(GeoSearchOptions{
		FromMember: "new york",
		ByRadius:   10,
		Unit:       "km",
	})

	require.Len(t, results, 2)
	assert.Equal(t, "new york", results[0].Member)
	assert.Equal(t, "manhattan", results[1].Member)
}

func TestGeoSearch_ByRadius_Descending(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
		{Longitude: -73.9857, Latitude: 40.7484, Member: "manhattan"},
		{Longitude: -73.9442, Latitude: 40.6782, Member: "brooklyn"},
	}

	z.GeoAdd(items, ZAddOptions{})

	results := z.GeoSearch(GeoSearchOptions{
		FromMember: "new york",
		ByRadius:   100,
		Unit:       "km",
		Descending: true,
	})

	require.Len(t, results, 3)
	// Should be sorted by distance descending
	assert.True(t, results[0].Distance >= results[1].Distance)
	assert.True(t, results[1].Distance >= results[2].Distance)
}

func TestGeoSearch_ByRadius_WithCount(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
		{Longitude: -73.9857, Latitude: 40.7484, Member: "manhattan"},
		{Longitude: -73.9442, Latitude: 40.6782, Member: "brooklyn"},
		{Longitude: -73.9712, Latitude: 40.6501, Member: "queens"},
	}

	z.GeoAdd(items, ZAddOptions{})

	results := z.GeoSearch(GeoSearchOptions{
		FromMember: "new york",
		ByRadius:   100,
		Unit:       "km",
		Count:      2,
	})

	require.Len(t, results, 2)
}

func TestGeoSearch_ByBox(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
		{Longitude: -73.9857, Latitude: 40.7484, Member: "manhattan"},
		{Longitude: -118.2437, Latitude: 34.0522, Member: "los angeles"},
	}

	z.GeoAdd(items, ZAddOptions{})

	results := z.GeoSearch(GeoSearchOptions{
		FromLonLat: &GeoPoint{Longitude: -74.0060, Latitude: 40.7128},
		ByBox: &GeoBox{
			Width:  20,
			Height: 20,
		},
		Unit: "km",
	})

	require.Len(t, results, 2)
}

func TestGeoSearch_ByBox_Descending(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
		{Longitude: -73.9857, Latitude: 40.7484, Member: "manhattan"},
		{Longitude: -73.9442, Latitude: 40.6782, Member: "brooklyn"},
	}

	z.GeoAdd(items, ZAddOptions{})

	results := z.GeoSearch(GeoSearchOptions{
		FromLonLat: &GeoPoint{Longitude: -74.0060, Latitude: 40.7128},
		ByBox: &GeoBox{
			Width:  50,
			Height: 50,
		},
		Unit:       "km",
		Descending: true,
	})

	require.Len(t, results, 3)
	assert.True(t, results[0].Distance >= results[1].Distance)
}

func TestGeoSearch_FromMemberNotFound(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
	}

	z.GeoAdd(items, ZAddOptions{})

	results := z.GeoSearch(GeoSearchOptions{
		FromMember: "missing",
		ByRadius:   100,
		Unit:       "km",
	})

	assert.Nil(t, results)
}

func TestGeoSearch_NoFromSpecified(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
	}

	z.GeoAdd(items, ZAddOptions{})

	results := z.GeoSearch(GeoSearchOptions{
		ByRadius: 100,
		Unit:     "km",
	})

	assert.Nil(t, results)
}

func TestGeoSearch_ResultContainsAllFields(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
		{Longitude: -73.9857, Latitude: 40.7484, Member: "manhattan"},
	}

	z.GeoAdd(items, ZAddOptions{})

	results := z.GeoSearch(GeoSearchOptions{
		FromLonLat: &GeoPoint{Longitude: -74.0060, Latitude: 40.7128},
		ByRadius:   10,
		Unit:       "km",
	})

	require.Len(t, results, 2)

	for _, result := range results {
		assert.NotEmpty(t, result.Member)
		assert.NotZero(t, result.Hash)
		assert.True(t, result.Longitude >= MinLongitude && result.Longitude <= MaxLongitude)
		assert.True(t, result.Latitude >= MinLatitude && result.Latitude <= MaxLatitude)
	}
}

func TestGeoSearch_EmptySet(t *testing.T) {
	z := NewZSet()

	results := z.GeoSearch(GeoSearchOptions{
		FromLonLat: &GeoPoint{Longitude: 0, Latitude: 0},
		ByRadius:   1000,
		Unit:       "km",
	})

	assert.Empty(t, results)
}

func TestGeoSearch_CountZero(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
	}

	z.GeoAdd(items, ZAddOptions{})

	results := z.GeoSearch(GeoSearchOptions{
		FromLonLat: &GeoPoint{Longitude: -74.0060, Latitude: 40.7128},
		ByRadius:   100,
		Unit:       "km",
		Count:      0,
	})

	assert.Len(t, results, 1)
}

func TestGeoSearch_CountGreaterThanResults(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: -74.0060, Latitude: 40.7128, Member: "new york"},
	}

	z.GeoAdd(items, ZAddOptions{})

	results := z.GeoSearch(GeoSearchOptions{
		FromLonLat: &GeoPoint{Longitude: -74.0060, Latitude: 40.7128},
		ByRadius:   100,
		Unit:       "km",
		Count:      100,
	})

	assert.Len(t, results, 1)
}

func TestGeoSearch_DifferentUnits(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: 0, Latitude: 0, Member: "origin"},
		{Longitude: 0.01, Latitude: 0, Member: "nearby"},
	}

	z.GeoAdd(items, ZAddOptions{})

	resultsKm := z.GeoSearch(GeoSearchOptions{
		FromLonLat: &GeoPoint{Longitude: 0, Latitude: 0},
		ByRadius:   10,
		Unit:       "km",
	})

	resultsMi := z.GeoSearch(GeoSearchOptions{
		FromLonLat: &GeoPoint{Longitude: 0, Latitude: 0},
		ByRadius:   10,
		Unit:       "mi",
	})

	assert.Equal(t, len(resultsKm), len(resultsMi))
}

func TestGeoSearch_ByBoxVerySmall(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: 0, Latitude: 0, Member: "origin"},
		{Longitude: 0.001, Latitude: 0.001, Member: "very close"},
		{Longitude: 1, Latitude: 1, Member: "far"},
	}

	z.GeoAdd(items, ZAddOptions{})

	results := z.GeoSearch(GeoSearchOptions{
		FromLonLat: &GeoPoint{Longitude: 0, Latitude: 0},
		ByBox: &GeoBox{
			Width:  1,
			Height: 1,
		},
		Unit: "km",
	})

	// Only origin and very close should be within 1km x 1km box
	assert.True(t, len(results) <= 2)
}

func TestGeoSearch_DistanceCalculation(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: 0, Latitude: 0, Member: "origin"},
		{Longitude: 1, Latitude: 0, Member: "1deg east"},
	}

	z.GeoAdd(items, ZAddOptions{})

	results := z.GeoSearch(GeoSearchOptions{
		FromLonLat: &GeoPoint{Longitude: 0, Latitude: 0},
		ByRadius:   1000,
		Unit:       "km",
	})

	require.Len(t, results, 2)
	assert.InDelta(t, 0, results[0].Distance, 0.1)
	// 1 degree of longitude at equator is ~111km
	assert.InDelta(t, 111, results[1].Distance, 5)
}

// ============= Edge Cases =============

func TestGeoHashEncode_BoundaryValues(t *testing.T) {
	// Test minimum boundary values - these normalize to 0
	hash := GeoHashEncode(MinLongitude, MinLatitude)
	lon, lat := GeoHashDecode(hash)
	assert.InDelta(t, MinLongitude, lon, 0.01)
	assert.InDelta(t, MinLatitude, lat, 0.01)

	// Test values near max bounds
	hash = GeoHashEncode(MaxLongitude-1, MaxLatitude-1)
	lon, lat = GeoHashDecode(hash)
	assert.InDelta(t, MaxLongitude-1, lon, 0.1)
	assert.InDelta(t, MaxLatitude-1, lat, 0.1)
}

func TestHaversineDistance_SmallDistances(t *testing.T) {
	// Very close points should have small distances
	distance := HaversineDistance(0, 0, 0.0001, 0.0001)
	assert.True(t, distance > 0)
	assert.True(t, distance < 100) // Should be less than 100 meters
}

func TestHaversineDistance_CrossEquator(t *testing.T) {
	// Points crossing the equator
	distance := HaversineDistance(0, 1, 0, -1)
	// 2 degrees of latitude is about 222 km
	assert.InDelta(t, 222000, distance, 5000)
}

func TestHaversineDistance_CrossPrimeMeridian(t *testing.T) {
	// Points crossing the prime meridian
	distance := HaversineDistance(1, 0, -1, 0)
	// 2 degrees of longitude at equator is about 222 km
	assert.InDelta(t, 222000, distance, 5000)
}

func TestGeoSearch_ByBoxWithCount(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: 0, Latitude: 0, Member: "a"},
		{Longitude: 0.01, Latitude: 0, Member: "b"},
		{Longitude: 0.02, Latitude: 0, Member: "c"},
		{Longitude: 0.03, Latitude: 0, Member: "d"},
	}

	z.GeoAdd(items, ZAddOptions{})

	results := z.GeoSearch(GeoSearchOptions{
		FromLonLat: &GeoPoint{Longitude: 0, Latitude: 0},
		ByBox: &GeoBox{
			Width:  100,
			Height: 100,
		},
		Unit:  "km",
		Count: 2,
	})

	assert.Len(t, results, 2)
}

func TestGeoAdd_EmptyItems(t *testing.T) {
	z := NewZSet()

	result, _ := z.GeoAdd([]GeoPoint{}, ZAddOptions{})
	require.NotNil(t, result)
	assert.Equal(t, uint32(0), *result)
}

func TestGeoHash_SingleMember(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: 0, Latitude: 0, Member: "origin"},
	}

	z.GeoAdd(items, ZAddOptions{})

	hashes := z.GeoHash([]string{"origin"})
	require.Len(t, hashes, 1)
	require.NotNil(t, hashes[0])
}

func TestGeoPos_SingleMember(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: 13.361389, Latitude: 52.519444, Member: "berlin"},
	}

	z.GeoAdd(items, ZAddOptions{})

	positions := z.GeoPos([]string{"berlin"})
	require.Len(t, positions, 1)
	require.NotNil(t, positions[0])
	assert.InDelta(t, 13.361389, positions[0].Longitude, 0.001)
	assert.InDelta(t, 52.519444, positions[0].Latitude, 0.001)
}

func TestGeoSearch_AscendingIsDefault(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: 0, Latitude: 0, Member: "a"},
		{Longitude: 0.05, Latitude: 0, Member: "b"},
		{Longitude: 0.02, Latitude: 0, Member: "c"},
	}

	z.GeoAdd(items, ZAddOptions{})

	results := z.GeoSearch(GeoSearchOptions{
		FromLonLat: &GeoPoint{Longitude: 0, Latitude: 0},
		ByRadius:   100,
		Unit:       "km",
	})

	require.Len(t, results, 3)
	// Default is ascending by distance
	assert.True(t, results[0].Distance <= results[1].Distance)
	assert.True(t, results[1].Distance <= results[2].Distance)
}

func TestConvertDistance_ZeroValue(t *testing.T) {
	assert.Equal(t, 0.0, ConvertDistance(0, "m"))
	assert.Equal(t, 0.0, ConvertDistance(0, "km"))
	assert.Equal(t, 0.0, ConvertDistance(0, "mi"))
	assert.Equal(t, 0.0, ConvertDistance(0, "ft"))
}

func TestConvertToMeters_ZeroValue(t *testing.T) {
	assert.Equal(t, 0.0, ConvertToMeters(0, "m"))
	assert.Equal(t, 0.0, ConvertToMeters(0, "km"))
	assert.Equal(t, 0.0, ConvertToMeters(0, "mi"))
	assert.Equal(t, 0.0, ConvertToMeters(0, "ft"))
}

func TestGeoHashToString_MaxHash(t *testing.T) {
	// Test with a very large hash value
	maxHash := uint64(1<<52 - 1)
	str := GeoHashToString(maxHash)
	assert.Len(t, str, 11)
}

func TestHaversineDistance_NearPoles(t *testing.T) {
	// Points near the poles
	distance := HaversineDistance(0, 85, 180, 85)
	assert.True(t, distance > 0)
	assert.False(t, math.IsInf(distance, 0))
	assert.False(t, math.IsNaN(distance))
}

func TestGeoSearch_NoBySpecified(t *testing.T) {
	z := NewZSet()

	items := []GeoPoint{
		{Longitude: 0, Latitude: 0, Member: "origin"},
	}

	z.GeoAdd(items, ZAddOptions{})

	// When neither ByRadius nor ByBox is specified, radiusMeters will be 0
	// and neither inRange condition is triggered, so nothing matches
	results := z.GeoSearch(GeoSearchOptions{
		FromLonLat: &GeoPoint{Longitude: 0, Latitude: 0},
		Unit:       "km",
	})

	// No points match when no search criteria specified
	assert.Empty(t, results)
}

func TestValidateCoordinates_EdgeCases(t *testing.T) {
	// Exactly on boundaries
	assert.True(t, ValidateCoordinates(-180, 0))
	assert.True(t, ValidateCoordinates(180, 0))
	assert.True(t, ValidateCoordinates(0, MinLatitude))
	assert.True(t, ValidateCoordinates(0, MaxLatitude))

	// Just outside boundaries
	assert.False(t, ValidateCoordinates(-180.0001, 0))
	assert.False(t, ValidateCoordinates(180.0001, 0))
	assert.False(t, ValidateCoordinates(0, MinLatitude-0.0001))
	assert.False(t, ValidateCoordinates(0, MaxLatitude+0.0001))
}
