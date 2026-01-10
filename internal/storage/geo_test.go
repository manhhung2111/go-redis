package storage

import (
	"math"
	"testing"

	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
	"github.com/stretchr/testify/assert"
)

func TestGeoAdd_NewKey(t *testing.T) {
	s := NewStore()

	got, err := s.GeoAdd("geo1", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
	}, data_structure.ZAddOptions{})

	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.Equal(t, uint32(1), *got)
}

func TestGeoAdd_MultiplePoints(t *testing.T) {
	s := NewStore()

	got, err := s.GeoAdd("geo1", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
		{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
		{Longitude: -73.9352, Latitude: 40.7306, Member: "new_york"},
	}, data_structure.ZAddOptions{})

	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.Equal(t, uint32(3), *got)
}

func TestGeoAdd_ExistingKey(t *testing.T) {
	s := NewStore()
	s.GeoAdd("geo1", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
	}, data_structure.ZAddOptions{})

	got, err := s.GeoAdd("geo1", []data_structure.GeoPoint{
		{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
	}, data_structure.ZAddOptions{})

	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.Equal(t, uint32(1), *got)
}

func TestGeoAdd_UpdateExistingMember(t *testing.T) {
	s := NewStore()
	s.GeoAdd("geo1", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "city"},
	}, data_structure.ZAddOptions{})

	got, err := s.GeoAdd("geo1", []data_structure.GeoPoint{
		{Longitude: -118.2437, Latitude: 34.0522, Member: "city"},
	}, data_structure.ZAddOptions{})

	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.Equal(t, uint32(0), *got)
}

func TestGeoAdd_NXOption(t *testing.T) {
	s := NewStore()
	s.GeoAdd("geo1", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
	}, data_structure.ZAddOptions{})

	got, err := s.GeoAdd("geo1", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
		{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
	}, data_structure.ZAddOptions{NX: true})

	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.Equal(t, uint32(1), *got)
}

func TestGeoAdd_XXOption(t *testing.T) {
	s := NewStore()
	s.GeoAdd("geo1", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
	}, data_structure.ZAddOptions{})

	got, err := s.GeoAdd("geo1", []data_structure.GeoPoint{
		{Longitude: -122.0, Latitude: 37.0, Member: "san_francisco"},
		{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
	}, data_structure.ZAddOptions{XX: true})

	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.Equal(t, uint32(0), *got)
}

func TestGeoAdd_WrongType(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "string_value")

	got, err := s.GeoAdd("mykey", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "sf"},
	}, data_structure.ZAddOptions{})

	assert.Error(t, err)
	assert.Nil(t, got)
}

func TestGeoDist_NonExistentKey(t *testing.T) {
	s := NewStore()

	got, err := s.GeoDist("geo1", "city1", "city2", "m")
	assert.NoError(t, err)
	assert.Nil(t, got)
}

func TestGeoDist_MemberNotFound(t *testing.T) {
	s := NewStore()
	s.GeoAdd("geo1", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
	}, data_structure.ZAddOptions{})

	got, err := s.GeoDist("geo1", "unknown", "san_francisco", "m")
	assert.NoError(t, err)
	assert.Nil(t, got)

	got, err = s.GeoDist("geo1", "san_francisco", "unknown", "m")
	assert.NoError(t, err)
	assert.Nil(t, got)
}

func TestGeoDist_InMeters(t *testing.T) {
	s := NewStore()
	s.GeoAdd("geo1", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
		{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
	}, data_structure.ZAddOptions{})

	got, err := s.GeoDist("geo1", "san_francisco", "los_angeles", "m")
	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.InDelta(t, 559000.0, *got, 5000.0)
}

func TestGeoDist_InKilometers(t *testing.T) {
	s := NewStore()
	s.GeoAdd("geo1", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
		{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
	}, data_structure.ZAddOptions{})

	got, err := s.GeoDist("geo1", "san_francisco", "los_angeles", "km")
	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.InDelta(t, 559.0, *got, 5.0)
}

func TestGeoDist_InMiles(t *testing.T) {
	s := NewStore()
	s.GeoAdd("geo1", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
		{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
	}, data_structure.ZAddOptions{})

	got, err := s.GeoDist("geo1", "san_francisco", "los_angeles", "mi")
	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.InDelta(t, 347.0, *got, 5.0)
}

func TestGeoDist_SameMember(t *testing.T) {
	s := NewStore()
	s.GeoAdd("geo1", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
	}, data_structure.ZAddOptions{})

	got, err := s.GeoDist("geo1", "san_francisco", "san_francisco", "m")
	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.InDelta(t, 0.0, *got, 0.001)
}

func TestGeoDist_WrongType(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "string_value")

	got, err := s.GeoDist("mykey", "a", "b", "m")
	assert.Error(t, err)
	assert.Nil(t, got)
}

func TestGeoHash_NonExistentKey(t *testing.T) {
	s := NewStore()

	got, err := s.GeoHash("geo1", []string{"m1", "m2"})
	assert.NoError(t, err)
	assert.Len(t, got, 2)
	assert.Nil(t, got[0])
	assert.Nil(t, got[1])
}

func TestGeoHash_SomeMembersExist(t *testing.T) {
	s := NewStore()
	s.GeoAdd("geo1", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
		{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
	}, data_structure.ZAddOptions{})

	got, err := s.GeoHash("geo1", []string{"san_francisco", "unknown", "los_angeles"})
	assert.NoError(t, err)
	assert.Len(t, got, 3)
	assert.NotNil(t, got[0])
	assert.Nil(t, got[1])
	assert.NotNil(t, got[2])
	assert.Len(t, *got[0], 11)
	assert.Len(t, *got[2], 11)
}

func TestGeoHash_WrongType(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "string_value")

	got, err := s.GeoHash("mykey", []string{"a"})
	assert.Error(t, err)
	assert.Nil(t, got)
}

func TestGeoPos_NonExistentKey(t *testing.T) {
	s := NewStore()

	got, err := s.GeoPos("geo1", []string{"m1", "m2"})
	assert.NoError(t, err)
	assert.Len(t, got, 2)
	assert.Nil(t, got[0])
	assert.Nil(t, got[1])
}

func TestGeoPos_SomeMembersExist(t *testing.T) {
	s := NewStore()
	s.GeoAdd("geo1", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
		{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
	}, data_structure.ZAddOptions{})

	got, err := s.GeoPos("geo1", []string{"san_francisco", "unknown", "los_angeles"})
	assert.NoError(t, err)
	assert.Len(t, got, 3)
	assert.NotNil(t, got[0])
	assert.Nil(t, got[1])
	assert.NotNil(t, got[2])
}

func TestGeoPos_Coordinates(t *testing.T) {
	s := NewStore()
	originalLon := -122.4194
	originalLat := 37.7749

	s.GeoAdd("geo1", []data_structure.GeoPoint{
		{Longitude: originalLon, Latitude: originalLat, Member: "san_francisco"},
	}, data_structure.ZAddOptions{})

	result, err := s.GeoPos("geo1", []string{"san_francisco"})
	assert.NoError(t, err)
	assert.NotNil(t, result[0])
	assert.InDelta(t, originalLon, result[0].Longitude, 0.001)
	assert.InDelta(t, originalLat, result[0].Latitude, 0.001)
	assert.Equal(t, "san_francisco", result[0].Member)
}

func TestGeoPos_WrongType(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "string_value")

	got, err := s.GeoPos("mykey", []string{"a"})
	assert.Error(t, err)
	assert.Nil(t, got)
}

func TestGeoSearch_NonExistentKey(t *testing.T) {
	s := NewStore()

	got, err := s.GeoSearch("geo1", data_structure.GeoSearchOptions{
		FromLonLat: &data_structure.GeoPoint{Longitude: -122.0, Latitude: 37.0},
		ByRadius:   100,
		Unit:       "km",
	})
	assert.NoError(t, err)
	assert.Empty(t, got)
}

func TestGeoSearch_ByRadiusFromLonLat(t *testing.T) {
	s := NewStore()
	s.GeoAdd("cities", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
		{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
		{Longitude: -121.8863, Latitude: 37.3382, Member: "san_jose"},
	}, data_structure.ZAddOptions{})

	got, err := s.GeoSearch("cities", data_structure.GeoSearchOptions{
		FromLonLat: &data_structure.GeoPoint{Longitude: -122.4194, Latitude: 37.7749},
		ByRadius:   100,
		Unit:       "km",
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(got))
}

func TestGeoSearch_ByRadiusFromMember(t *testing.T) {
	s := NewStore()
	s.GeoAdd("cities", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
		{Longitude: -121.8863, Latitude: 37.3382, Member: "san_jose"},
	}, data_structure.ZAddOptions{})

	got, err := s.GeoSearch("cities", data_structure.GeoSearchOptions{
		FromMember: "san_francisco",
		ByRadius:   100,
		Unit:       "km",
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(got))
}

func TestGeoSearch_NonExistentMember(t *testing.T) {
	s := NewStore()
	s.GeoAdd("cities", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
	}, data_structure.ZAddOptions{})

	got, err := s.GeoSearch("cities", data_structure.GeoSearchOptions{
		FromMember: "unknown_city",
		ByRadius:   100,
		Unit:       "km",
	})
	assert.NoError(t, err)
	assert.Empty(t, got)
}

func TestGeoSearch_WithCountLimit(t *testing.T) {
	s := NewStore()
	s.GeoAdd("cities", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
		{Longitude: -121.8863, Latitude: 37.3382, Member: "san_jose"},
		{Longitude: -122.0322, Latitude: 37.3688, Member: "sunnyvale"},
	}, data_structure.ZAddOptions{})

	got, err := s.GeoSearch("cities", data_structure.GeoSearchOptions{
		FromMember: "san_francisco",
		ByRadius:   100,
		Unit:       "km",
		Count:      2,
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(got))
}

func TestGeoSearch_Descending(t *testing.T) {
	s := NewStore()
	s.GeoAdd("cities", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
		{Longitude: -121.8863, Latitude: 37.3382, Member: "san_jose"},
		{Longitude: -122.0322, Latitude: 37.3688, Member: "sunnyvale"},
	}, data_structure.ZAddOptions{})

	ascResults, _ := s.GeoSearch("cities", data_structure.GeoSearchOptions{
		FromMember: "san_francisco",
		ByRadius:   100,
		Unit:       "km",
	})

	descResults, _ := s.GeoSearch("cities", data_structure.GeoSearchOptions{
		FromMember: "san_francisco",
		ByRadius:   100,
		Unit:       "km",
		Descending: true,
	})

	assert.Greater(t, len(ascResults), 1)
	assert.Greater(t, len(descResults), 1)

	for i := 1; i < len(ascResults); i++ {
		assert.LessOrEqual(t, ascResults[i-1].Distance, ascResults[i].Distance)
	}

	for i := 1; i < len(descResults); i++ {
		assert.GreaterOrEqual(t, descResults[i-1].Distance, descResults[i].Distance)
	}
}

func TestGeoSearch_ByBox(t *testing.T) {
	s := NewStore()
	s.GeoAdd("cities", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
		{Longitude: -121.8863, Latitude: 37.3382, Member: "san_jose"},
	}, data_structure.ZAddOptions{})

	got, err := s.GeoSearch("cities", data_structure.GeoSearchOptions{
		FromLonLat: &data_structure.GeoPoint{Longitude: -122.4194, Latitude: 37.7749},
		ByBox: &data_structure.GeoBox{
			Width:  200,
			Height: 200,
		},
		Unit: "km",
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(got))
}

func TestGeoSearch_WrongType(t *testing.T) {
	s := NewStore()
	s.Set("mykey", "string_value")

	got, err := s.GeoSearch("mykey", data_structure.GeoSearchOptions{
		FromLonLat: &data_structure.GeoPoint{Longitude: 0.0, Latitude: 0.0},
		ByRadius:   100,
		Unit:       "km",
	})
	assert.Error(t, err)
	assert.Nil(t, got)
}

func TestGeoIntegration(t *testing.T) {
	s := NewStore()

	result, err := s.GeoAdd("locations", []data_structure.GeoPoint{
		{Longitude: 13.361389, Latitude: 52.519444, Member: "berlin"},
		{Longitude: 2.349014, Latitude: 48.864716, Member: "paris"},
		{Longitude: -0.118092, Latitude: 51.509865, Member: "london"},
	}, data_structure.ZAddOptions{})
	assert.NoError(t, err)
	assert.Equal(t, uint32(3), *result)

	dist, err := s.GeoDist("locations", "berlin", "paris", "km")
	assert.NoError(t, err)
	assert.NotNil(t, dist)
	assert.InDelta(t, 878.0, *dist, 50.0)

	hashes, err := s.GeoHash("locations", []string{"berlin", "paris", "unknown"})
	assert.NoError(t, err)
	assert.NotNil(t, hashes[0])
	assert.NotNil(t, hashes[1])
	assert.Nil(t, hashes[2])

	positions, err := s.GeoPos("locations", []string{"berlin", "london"})
	assert.NoError(t, err)
	assert.NotNil(t, positions[0])
	assert.NotNil(t, positions[1])
	assert.InDelta(t, 13.361389, positions[0].Longitude, 0.01)
	assert.InDelta(t, 52.519444, positions[0].Latitude, 0.01)

	searchResults, err := s.GeoSearch("locations", data_structure.GeoSearchOptions{
		FromMember: "paris",
		ByRadius:   1000,
		Unit:       "km",
	})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(searchResults))
}

func TestGeoSearchResultFields(t *testing.T) {
	s := NewStore()
	s.GeoAdd("cities", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
	}, data_structure.ZAddOptions{})

	results, err := s.GeoSearch("cities", data_structure.GeoSearchOptions{
		FromLonLat: &data_structure.GeoPoint{Longitude: -122.4194, Latitude: 37.7749},
		ByRadius:   10,
		Unit:       "km",
	})

	assert.NoError(t, err)
	assert.Len(t, results, 1)
	result := results[0]

	assert.Equal(t, "san_francisco", result.Member)
	assert.InDelta(t, 0.0, result.Distance, 0.01)
	assert.NotZero(t, result.Hash)
	assert.InDelta(t, -122.4194, result.Longitude, 0.01)
	assert.InDelta(t, 37.7749, result.Latitude, 0.01)
}

func TestGeoDistAllUnits(t *testing.T) {
	s := NewStore()
	s.GeoAdd("test", []data_structure.GeoPoint{
		{Longitude: 0.0, Latitude: 0.0, Member: "a"},
		{Longitude: 0.009, Latitude: 0.0, Member: "b"},
	}, data_structure.ZAddOptions{})

	metersResult, _ := s.GeoDist("test", "a", "b", "m")
	kmResult, _ := s.GeoDist("test", "a", "b", "km")
	miResult, _ := s.GeoDist("test", "a", "b", "mi")
	ftResult, _ := s.GeoDist("test", "a", "b", "ft")

	assert.NotNil(t, metersResult)
	assert.NotNil(t, kmResult)
	assert.NotNil(t, miResult)
	assert.NotNil(t, ftResult)

	assert.InDelta(t, *metersResult/1000, *kmResult, 0.001)
	assert.InDelta(t, *metersResult*3.28084, *ftResult, 1.0)
	assert.InDelta(t, *metersResult*0.000621371, *miResult, 0.001)
}

func TestGeoHashFormat(t *testing.T) {
	s := NewStore()
	s.GeoAdd("test", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "sf"},
	}, data_structure.ZAddOptions{})

	hashes, err := s.GeoHash("test", []string{"sf"})
	assert.NoError(t, err)
	assert.NotNil(t, hashes[0])

	hash := *hashes[0]
	assert.Len(t, hash, 11)

	validChars := "0123456789bcdefghjkmnpqrstuvwxyz"
	for _, c := range hash {
		assert.Contains(t, validChars, string(c))
	}
}

func TestGeoSearchNoResults(t *testing.T) {
	s := NewStore()
	s.GeoAdd("test", []data_structure.GeoPoint{
		{Longitude: 0.0, Latitude: 0.0, Member: "origin"},
	}, data_structure.ZAddOptions{})

	results, err := s.GeoSearch("test", data_structure.GeoSearchOptions{
		FromLonLat: &data_structure.GeoPoint{Longitude: 100.0, Latitude: 50.0},
		ByRadius:   1,
		Unit:       "km",
	})

	assert.NoError(t, err)
	assert.Empty(t, results)
}

func TestGeoMath(t *testing.T) {
	s := NewStore()
	s.GeoAdd("us", []data_structure.GeoPoint{
		{Longitude: -73.9352, Latitude: 40.7306, Member: "new_york"},
		{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
	}, data_structure.ZAddOptions{})

	dist, err := s.GeoDist("us", "new_york", "los_angeles", "km")
	assert.NoError(t, err)
	assert.NotNil(t, dist)
	assert.InDelta(t, 3940.0, *dist, 200.0)
}

func TestGeoSearchCountZero(t *testing.T) {
	s := NewStore()
	s.GeoAdd("test", []data_structure.GeoPoint{
		{Longitude: 0.0, Latitude: 0.0, Member: "a"},
		{Longitude: 0.001, Latitude: 0.0, Member: "b"},
		{Longitude: 0.002, Latitude: 0.0, Member: "c"},
	}, data_structure.ZAddOptions{})

	results, err := s.GeoSearch("test", data_structure.GeoSearchOptions{
		FromMember: "a",
		ByRadius:   100,
		Unit:       "km",
		Count:      0,
	})

	assert.NoError(t, err)
	assert.Equal(t, 3, len(results))
}

func TestGeoPosReturnsCorrectMember(t *testing.T) {
	s := NewStore()
	s.GeoAdd("test", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "sf"},
		{Longitude: -118.2437, Latitude: 34.0522, Member: "la"},
	}, data_structure.ZAddOptions{})

	results, err := s.GeoPos("test", []string{"sf", "la"})
	assert.NoError(t, err)

	assert.Equal(t, "sf", results[0].Member)
	assert.Equal(t, "la", results[1].Member)
}

func TestGeoSearchInfinity(t *testing.T) {
	s := NewStore()
	s.GeoAdd("test", []data_structure.GeoPoint{
		{Longitude: 0.0, Latitude: 0.0, Member: "origin"},
		{Longitude: 179.0, Latitude: 85.0, Member: "far"},
	}, data_structure.ZAddOptions{})

	results, err := s.GeoSearch("test", data_structure.GeoSearchOptions{
		FromMember: "origin",
		ByRadius:   math.MaxFloat64 / 2,
		Unit:       "m",
	})

	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))
}

func TestGeoSearchRadiusBoundary(t *testing.T) {
	s := NewStore()

	s.GeoAdd("test", []data_structure.GeoPoint{
		{Longitude: 0.0, Latitude: 0.0, Member: "origin"},
		{Longitude: 1.0, Latitude: 0.0, Member: "far"},
	}, data_structure.ZAddOptions{})

	results, _ := s.GeoSearch("test", data_structure.GeoSearchOptions{
		FromMember: "origin",
		ByRadius:   50,
		Unit:       "km",
	})
	assert.Equal(t, 1, len(results))
	assert.Equal(t, "origin", results[0].Member)

	results, _ = s.GeoSearch("test", data_structure.GeoSearchOptions{
		FromMember: "origin",
		ByRadius:   200,
		Unit:       "km",
	})
	assert.Equal(t, 2, len(results))
}

func TestGeoSearchResultsContainCorrectDistance(t *testing.T) {
	s := NewStore()
	s.GeoAdd("test", []data_structure.GeoPoint{
		{Longitude: 0.0, Latitude: 0.0, Member: "origin"},
		{Longitude: 1.0, Latitude: 0.0, Member: "far"},
	}, data_structure.ZAddOptions{})

	directDist, _ := s.GeoDist("test", "origin", "far", "km")

	results, _ := s.GeoSearch("test", data_structure.GeoSearchOptions{
		FromMember: "origin",
		ByRadius:   200,
		Unit:       "km",
	})

	var searchDist float64
	for _, r := range results {
		if r.Member == "far" {
			searchDist = r.Distance
			break
		}
	}

	assert.InDelta(t, *directDist, searchDist, 0.1)
}
