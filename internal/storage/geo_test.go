package storage

import (
	"math"
	"testing"

	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
	"github.com/stretchr/testify/assert"
)

func TestGeoAdd(t *testing.T) {
	tests := map[string]struct {
		setup   func(s Store)
		key     string
		items   []data_structure.GeoPoint
		options data_structure.ZAddOptions
		want    *uint32
	}{
		"add single point to new key": {
			setup: func(s Store) {},
			key:   "geo1",
			items: []data_structure.GeoPoint{
				{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
			},
			options: data_structure.ZAddOptions{},
			want:    ptr(uint32(1)),
		},
		"add multiple points to new key": {
			setup: func(s Store) {},
			key:   "geo1",
			items: []data_structure.GeoPoint{
				{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
				{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
				{Longitude: -73.9352, Latitude: 40.7306, Member: "new_york"},
			},
			options: data_structure.ZAddOptions{},
			want:    ptr(uint32(3)),
		},
		"add point to existing geo key": {
			setup: func(s Store) {
				s.GeoAdd("geo1", []data_structure.GeoPoint{
					{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
				}, data_structure.ZAddOptions{})
			},
			key: "geo1",
			items: []data_structure.GeoPoint{
				{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
			},
			options: data_structure.ZAddOptions{},
			want:    ptr(uint32(1)),
		},
		"update existing member": {
			setup: func(s Store) {
				s.GeoAdd("geo1", []data_structure.GeoPoint{
					{Longitude: -122.4194, Latitude: 37.7749, Member: "city"},
				}, data_structure.ZAddOptions{})
			},
			key: "geo1",
			items: []data_structure.GeoPoint{
				{Longitude: -118.2437, Latitude: 34.0522, Member: "city"},
			},
			options: data_structure.ZAddOptions{},
			want:    ptr(uint32(0)),
		},
		"NX option - only add new members": {
			setup: func(s Store) {
				s.GeoAdd("geo1", []data_structure.GeoPoint{
					{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
				}, data_structure.ZAddOptions{})
			},
			key: "geo1",
			items: []data_structure.GeoPoint{
				{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
				{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
			},
			options: data_structure.ZAddOptions{NX: true},
			want:    ptr(uint32(1)),
		},
		"XX option - only update existing members": {
			setup: func(s Store) {
				s.GeoAdd("geo1", []data_structure.GeoPoint{
					{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
				}, data_structure.ZAddOptions{})
			},
			key: "geo1",
			items: []data_structure.GeoPoint{
				{Longitude: -122.0, Latitude: 37.0, Member: "san_francisco"},
				{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
			},
			options: data_structure.ZAddOptions{XX: true},
			want:    ptr(uint32(0)),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			tc.setup(s)

			got := s.GeoAdd(tc.key, tc.items, tc.options)
			if tc.want == nil {
				assert.Nil(t, got)
			} else {
				assert.NotNil(t, got)
				assert.Equal(t, *tc.want, *got)
			}
		})
	}
}

func TestGeoAddPanic(t *testing.T) {
	s := NewStore()
	// Set a string value
	s.Set("mykey", "string_value")

	assert.Panics(t, func() {
		s.GeoAdd("mykey", []data_structure.GeoPoint{
			{Longitude: -122.4194, Latitude: 37.7749, Member: "sf"},
		}, data_structure.ZAddOptions{})
	})
}

func TestGeoDist(t *testing.T) {
	tests := map[string]struct {
		setup   func(s Store)
		key     string
		member1 string
		member2 string
		unit    string
		want    *float64
		epsilon float64
	}{
		"non-existent key": {
			setup:   func(s Store) {},
			key:     "geo1",
			member1: "city1",
			member2: "city2",
			unit:    "m",
			want:    nil,
		},
		"first member not found": {
			setup: func(s Store) {
				s.GeoAdd("geo1", []data_structure.GeoPoint{
					{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
				}, data_structure.ZAddOptions{})
			},
			key:     "geo1",
			member1: "unknown",
			member2: "san_francisco",
			unit:    "m",
			want:    nil,
		},
		"second member not found": {
			setup: func(s Store) {
				s.GeoAdd("geo1", []data_structure.GeoPoint{
					{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
				}, data_structure.ZAddOptions{})
			},
			key:     "geo1",
			member1: "san_francisco",
			member2: "unknown",
			unit:    "m",
			want:    nil,
		},
		"distance in meters": {
			setup: func(s Store) {
				// San Francisco and Los Angeles
				s.GeoAdd("geo1", []data_structure.GeoPoint{
					{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
					{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
				}, data_structure.ZAddOptions{})
			},
			key:     "geo1",
			member1: "san_francisco",
			member2: "los_angeles",
			unit:    "m",
			want:    ptr(559000.0), // approximately 559 km
			epsilon: 5000.0,        // allow 5km tolerance due to geohash precision
		},
		"distance in kilometers": {
			setup: func(s Store) {
				s.GeoAdd("geo1", []data_structure.GeoPoint{
					{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
					{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
				}, data_structure.ZAddOptions{})
			},
			key:     "geo1",
			member1: "san_francisco",
			member2: "los_angeles",
			unit:    "km",
			want:    ptr(559.0),
			epsilon: 5.0,
		},
		"distance in miles": {
			setup: func(s Store) {
				s.GeoAdd("geo1", []data_structure.GeoPoint{
					{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
					{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
				}, data_structure.ZAddOptions{})
			},
			key:     "geo1",
			member1: "san_francisco",
			member2: "los_angeles",
			unit:    "mi",
			want:    ptr(347.0), // approximately 347 miles
			epsilon: 5.0,
		},
		"distance in feet": {
			setup: func(s Store) {
				s.GeoAdd("geo1", []data_structure.GeoPoint{
					{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
					{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
				}, data_structure.ZAddOptions{})
			},
			key:     "geo1",
			member1: "san_francisco",
			member2: "los_angeles",
			unit:    "ft",
			want:    ptr(1834000.0), // approximately 1.83 million feet
			epsilon: 20000.0,
		},
		"same member distance is zero": {
			setup: func(s Store) {
				s.GeoAdd("geo1", []data_structure.GeoPoint{
					{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
				}, data_structure.ZAddOptions{})
			},
			key:     "geo1",
			member1: "san_francisco",
			member2: "san_francisco",
			unit:    "m",
			want:    ptr(0.0),
			epsilon: 0.001,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			tc.setup(s)

			got := s.GeoDist(tc.key, tc.member1, tc.member2, tc.unit)
			if tc.want == nil {
				assert.Nil(t, got)
			} else {
				assert.NotNil(t, got)
				assert.InDelta(t, *tc.want, *got, tc.epsilon)
			}
		})
	}
}

func TestGeoHash(t *testing.T) {
	tests := map[string]struct {
		setup   func(s Store)
		key     string
		members []string
		wantNil []bool
	}{
		"non-existent key": {
			setup:   func(s Store) {},
			key:     "geo1",
			members: []string{"m1", "m2"},
			wantNil: []bool{true, true},
		},
		"some members exist": {
			setup: func(s Store) {
				s.GeoAdd("geo1", []data_structure.GeoPoint{
					{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
					{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
				}, data_structure.ZAddOptions{})
			},
			key:     "geo1",
			members: []string{"san_francisco", "unknown", "los_angeles"},
			wantNil: []bool{false, true, false},
		},
		"all members exist": {
			setup: func(s Store) {
				s.GeoAdd("geo1", []data_structure.GeoPoint{
					{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
					{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
				}, data_structure.ZAddOptions{})
			},
			key:     "geo1",
			members: []string{"san_francisco", "los_angeles"},
			wantNil: []bool{false, false},
		},
		"no members exist": {
			setup: func(s Store) {
				s.GeoAdd("geo1", []data_structure.GeoPoint{
					{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
				}, data_structure.ZAddOptions{})
			},
			key:     "geo1",
			members: []string{"unknown1", "unknown2"},
			wantNil: []bool{true, true},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			tc.setup(s)

			got := s.GeoHash(tc.key, tc.members)
			assert.Equal(t, len(tc.members), len(got))
			for i, wantNil := range tc.wantNil {
				if wantNil {
					assert.Nil(t, got[i])
				} else {
					assert.NotNil(t, got[i])
					assert.Len(t, *got[i], 11) // geohash string is 11 characters
				}
			}
		})
	}
}

func TestGeoPos(t *testing.T) {
	tests := map[string]struct {
		setup   func(s Store)
		key     string
		members []string
		wantNil []bool
	}{
		"non-existent key": {
			setup:   func(s Store) {},
			key:     "geo1",
			members: []string{"m1", "m2"},
			wantNil: []bool{true, true},
		},
		"some members exist": {
			setup: func(s Store) {
				s.GeoAdd("geo1", []data_structure.GeoPoint{
					{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
					{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
				}, data_structure.ZAddOptions{})
			},
			key:     "geo1",
			members: []string{"san_francisco", "unknown", "los_angeles"},
			wantNil: []bool{false, true, false},
		},
		"all members exist": {
			setup: func(s Store) {
				s.GeoAdd("geo1", []data_structure.GeoPoint{
					{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
				}, data_structure.ZAddOptions{})
			},
			key:     "geo1",
			members: []string{"san_francisco"},
			wantNil: []bool{false},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			tc.setup(s)

			got := s.GeoPos(tc.key, tc.members)
			assert.Equal(t, len(tc.members), len(got))
			for i, wantNil := range tc.wantNil {
				if wantNil {
					assert.Nil(t, got[i])
				} else {
					assert.NotNil(t, got[i])
				}
			}
		})
	}
}

func TestGeoPosCoordinates(t *testing.T) {
	s := NewStore()
	originalLon := -122.4194
	originalLat := 37.7749

	s.GeoAdd("geo1", []data_structure.GeoPoint{
		{Longitude: originalLon, Latitude: originalLat, Member: "san_francisco"},
	}, data_structure.ZAddOptions{})

	result := s.GeoPos("geo1", []string{"san_francisco"})
	assert.NotNil(t, result[0])
	// Allow some tolerance due to geohash encoding/decoding
	assert.InDelta(t, originalLon, result[0].Longitude, 0.001)
	assert.InDelta(t, originalLat, result[0].Latitude, 0.001)
	assert.Equal(t, "san_francisco", result[0].Member)
}

func TestGeoSearch(t *testing.T) {
	setupCities := func(s Store) {
		s.GeoAdd("cities", []data_structure.GeoPoint{
			{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
			{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
			{Longitude: -122.3321, Latitude: 47.6062, Member: "seattle"},
			{Longitude: -73.9352, Latitude: 40.7306, Member: "new_york"},
			{Longitude: -121.8863, Latitude: 37.3382, Member: "san_jose"},
		}, data_structure.ZAddOptions{})
	}

	tests := map[string]struct {
		setup       func(s Store)
		key         string
		options     data_structure.GeoSearchOptions
		wantLen     int
		wantMembers []string
	}{
		"non-existent key": {
			setup: func(s Store) {},
			key:   "geo1",
			options: data_structure.GeoSearchOptions{
				FromLonLat: &data_structure.GeoPoint{Longitude: -122.0, Latitude: 37.0},
				ByRadius:   100,
				Unit:       "km",
			},
			wantLen: 0,
		},
		"search by radius from lonlat - meters": {
			setup: setupCities,
			key:   "cities",
			options: data_structure.GeoSearchOptions{
				FromLonLat: &data_structure.GeoPoint{Longitude: -122.4194, Latitude: 37.7749},
				ByRadius:   100000, // 100 km in meters
				Unit:       "m",
			},
			wantLen:     2, // san_francisco and san_jose
			wantMembers: []string{"san_francisco", "san_jose"},
		},
		"search by radius from lonlat - kilometers": {
			setup: setupCities,
			key:   "cities",
			options: data_structure.GeoSearchOptions{
				FromLonLat: &data_structure.GeoPoint{Longitude: -122.4194, Latitude: 37.7749},
				ByRadius:   100,
				Unit:       "km",
			},
			wantLen:     2,
			wantMembers: []string{"san_francisco", "san_jose"},
		},
		"search by radius from member": {
			setup: setupCities,
			key:   "cities",
			options: data_structure.GeoSearchOptions{
				FromMember: "san_francisco",
				ByRadius:   100,
				Unit:       "km",
			},
			wantLen:     2,
			wantMembers: []string{"san_francisco", "san_jose"},
		},
		"search from non-existent member": {
			setup: setupCities,
			key:   "cities",
			options: data_structure.GeoSearchOptions{
				FromMember: "unknown_city",
				ByRadius:   100,
				Unit:       "km",
			},
			wantLen: 0,
		},
		"search with count limit": {
			setup: setupCities,
			key:   "cities",
			options: data_structure.GeoSearchOptions{
				FromLonLat: &data_structure.GeoPoint{Longitude: -122.4194, Latitude: 37.7749},
				ByRadius:   1000,
				Unit:       "km",
				Count:      2,
			},
			wantLen: 2,
		},
		"search descending order": {
			setup: setupCities,
			key:   "cities",
			options: data_structure.GeoSearchOptions{
				FromLonLat: &data_structure.GeoPoint{Longitude: -122.4194, Latitude: 37.7749},
				ByRadius:   100,
				Unit:       "km",
				Descending: true,
			},
			wantLen: 2,
		},
		"search by box": {
			setup: setupCities,
			key:   "cities",
			options: data_structure.GeoSearchOptions{
				FromLonLat: &data_structure.GeoPoint{Longitude: -122.4194, Latitude: 37.7749},
				ByBox: &data_structure.GeoBox{
					Width:  200,
					Height: 200,
				},
				Unit: "km",
			},
			wantLen: 2, // san_francisco and san_jose within box
		},
		"search with no center specified": {
			setup: setupCities,
			key:   "cities",
			options: data_structure.GeoSearchOptions{
				ByRadius: 100,
				Unit:     "km",
			},
			wantLen: 0, // No center, returns nil
		},
		"large radius includes all": {
			setup: setupCities,
			key:   "cities",
			options: data_structure.GeoSearchOptions{
				FromLonLat: &data_structure.GeoPoint{Longitude: -100.0, Latitude: 40.0},
				ByRadius:   5000,
				Unit:       "km",
			},
			wantLen: 5, // all cities
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			tc.setup(s)

			got := s.GeoSearch(tc.key, tc.options)
			assert.Equal(t, tc.wantLen, len(got))

			if tc.wantMembers != nil {
				gotMembers := make([]string, len(got))
				for i, r := range got {
					gotMembers[i] = r.Member
				}
				for _, wantMember := range tc.wantMembers {
					assert.Contains(t, gotMembers, wantMember)
				}
			}
		})
	}
}

func TestGeoSearchOrdering(t *testing.T) {
	s := NewStore()
	s.GeoAdd("cities", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
		{Longitude: -121.8863, Latitude: 37.3382, Member: "san_jose"},
		{Longitude: -122.0322, Latitude: 37.3688, Member: "sunnyvale"},
	}, data_structure.ZAddOptions{})

	// Test ascending order (default)
	ascResults := s.GeoSearch("cities", data_structure.GeoSearchOptions{
		FromMember: "san_francisco",
		ByRadius:   100,
		Unit:       "km",
	})

	assert.Greater(t, len(ascResults), 1)
	// First result should be san_francisco itself (distance 0)
	assert.Equal(t, "san_francisco", ascResults[0].Member)
	// Verify ascending order
	for i := 1; i < len(ascResults); i++ {
		assert.LessOrEqual(t, ascResults[i-1].Distance, ascResults[i].Distance)
	}

	// Test descending order
	descResults := s.GeoSearch("cities", data_structure.GeoSearchOptions{
		FromMember: "san_francisco",
		ByRadius:   100,
		Unit:       "km",
		Descending: true,
	})

	assert.Greater(t, len(descResults), 1)
	// Verify descending order
	for i := 1; i < len(descResults); i++ {
		assert.GreaterOrEqual(t, descResults[i-1].Distance, descResults[i].Distance)
	}
}

func TestGeoSearchResultFields(t *testing.T) {
	s := NewStore()
	s.GeoAdd("cities", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
	}, data_structure.ZAddOptions{})

	results := s.GeoSearch("cities", data_structure.GeoSearchOptions{
		FromLonLat: &data_structure.GeoPoint{Longitude: -122.4194, Latitude: 37.7749},
		ByRadius:   10,
		Unit:       "km",
	})

	assert.Len(t, results, 1)
	result := results[0]

	assert.Equal(t, "san_francisco", result.Member)
	assert.InDelta(t, 0.0, result.Distance, 0.01) // distance from self should be ~0
	assert.NotZero(t, result.Hash)
	assert.InDelta(t, -122.4194, result.Longitude, 0.01)
	assert.InDelta(t, 37.7749, result.Latitude, 0.01)
}

func TestGeoSearchByBoxLargeArea(t *testing.T) {
	s := NewStore()
	// Add points in a grid pattern
	s.GeoAdd("grid", []data_structure.GeoPoint{
		{Longitude: 0.0, Latitude: 0.0, Member: "center"},
		{Longitude: 0.5, Latitude: 0.0, Member: "east"},
		{Longitude: -0.5, Latitude: 0.0, Member: "west"},
		{Longitude: 0.0, Latitude: 0.5, Member: "north"},
		{Longitude: 0.0, Latitude: -0.5, Member: "south"},
	}, data_structure.ZAddOptions{})

	// Search with a box that should include center point and nearby points
	results := s.GeoSearch("grid", data_structure.GeoSearchOptions{
		FromLonLat: &data_structure.GeoPoint{Longitude: 0.0, Latitude: 0.0},
		ByBox: &data_structure.GeoBox{
			Width:  200,
			Height: 200,
		},
		Unit: "km",
	})

	// At least the center should be found
	assert.GreaterOrEqual(t, len(results), 1)
	found := false
	for _, r := range results {
		if r.Member == "center" {
			found = true
			break
		}
	}
	assert.True(t, found, "center point should be in results")
}

func TestGeoSearchWithCountLimit(t *testing.T) {
	s := NewStore()
	s.GeoAdd("cities", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "san_francisco"},
		{Longitude: -121.8863, Latitude: 37.3382, Member: "san_jose"},
		{Longitude: -122.0322, Latitude: 37.3688, Member: "sunnyvale"},
		{Longitude: -122.0819, Latitude: 37.3861, Member: "mountain_view"},
		{Longitude: -122.1430, Latitude: 37.4419, Member: "palo_alto"},
	}, data_structure.ZAddOptions{})

	// Search with count limit
	results := s.GeoSearch("cities", data_structure.GeoSearchOptions{
		FromMember: "san_francisco",
		ByRadius:   100,
		Unit:       "km",
		Count:      3,
	})

	assert.Equal(t, 3, len(results))
	// First should be san_francisco (distance 0)
	assert.Equal(t, "san_francisco", results[0].Member)
}

func TestGeoSearchDistanceUnits(t *testing.T) {
	s := NewStore()
	s.GeoAdd("test", []data_structure.GeoPoint{
		{Longitude: 0.0, Latitude: 0.0, Member: "origin"},
		{Longitude: 1.0, Latitude: 0.0, Member: "east"},
	}, data_structure.ZAddOptions{})

	// Search with different units should return results with appropriate distance values
	unitsToTest := []string{"m", "km", "mi", "ft"}

	for _, unit := range unitsToTest {
		results := s.GeoSearch("test", data_structure.GeoSearchOptions{
			FromMember: "origin",
			ByRadius:   1000000, // very large radius to include both points
			Unit:       unit,
		})
		assert.Equal(t, 2, len(results), "unit %s should find 2 results", unit)
	}
}

func TestGeoIntegration(t *testing.T) {
	s := NewStore()

	// Add some geo points
	result := s.GeoAdd("locations", []data_structure.GeoPoint{
		{Longitude: 13.361389, Latitude: 52.519444, Member: "berlin"},
		{Longitude: 2.349014, Latitude: 48.864716, Member: "paris"},
		{Longitude: -0.118092, Latitude: 51.509865, Member: "london"},
	}, data_structure.ZAddOptions{})
	assert.Equal(t, uint32(3), *result)

	// Get distance between cities
	dist := s.GeoDist("locations", "berlin", "paris", "km")
	assert.NotNil(t, dist)
	assert.InDelta(t, 878.0, *dist, 50.0) // approximately 878 km

	// Get geohash
	hashes := s.GeoHash("locations", []string{"berlin", "paris", "unknown"})
	assert.NotNil(t, hashes[0])
	assert.NotNil(t, hashes[1])
	assert.Nil(t, hashes[2])

	// Get positions
	positions := s.GeoPos("locations", []string{"berlin", "london"})
	assert.NotNil(t, positions[0])
	assert.NotNil(t, positions[1])
	assert.InDelta(t, 13.361389, positions[0].Longitude, 0.01)
	assert.InDelta(t, 52.519444, positions[0].Latitude, 0.01)

	// Search for cities near Paris within 1000 km
	searchResults := s.GeoSearch("locations", data_structure.GeoSearchOptions{
		FromMember: "paris",
		ByRadius:   1000,
		Unit:       "km",
	})
	assert.Equal(t, 3, len(searchResults)) // All three cities are within 1000 km of Paris
}

func TestGeoSearchEmptyZSet(t *testing.T) {
	s := NewStore()
	// Create an empty zset by adding and then removing
	s.ZAdd("empty", map[string]float64{"temp": 1.0}, data_structure.ZAddOptions{})
	s.ZRem("empty", []string{"temp"})

	results := s.GeoSearch("empty", data_structure.GeoSearchOptions{
		FromLonLat: &data_structure.GeoPoint{Longitude: 0.0, Latitude: 0.0},
		ByRadius:   1000,
		Unit:       "km",
	})

	assert.Empty(t, results)
}

func TestGeoDistSameLocation(t *testing.T) {
	s := NewStore()
	s.GeoAdd("test", []data_structure.GeoPoint{
		{Longitude: 0.0, Latitude: 0.0, Member: "origin"},
	}, data_structure.ZAddOptions{})

	dist := s.GeoDist("test", "origin", "origin", "m")
	assert.NotNil(t, dist)
	assert.InDelta(t, 0.0, *dist, 0.001)
}

func TestGeoHashFormat(t *testing.T) {
	s := NewStore()
	s.GeoAdd("test", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "sf"},
	}, data_structure.ZAddOptions{})

	hashes := s.GeoHash("test", []string{"sf"})
	assert.NotNil(t, hashes[0])

	hash := *hashes[0]
	// Geohash should be 11 characters long
	assert.Len(t, hash, 11)

	// Geohash should only contain valid base32 characters
	validChars := "0123456789bcdefghjkmnpqrstuvwxyz"
	for _, c := range hash {
		assert.Contains(t, validChars, string(c))
	}
}

func TestGeoSearchByRadiusVsBox(t *testing.T) {
	s := NewStore()
	// Create a cross pattern of points
	s.GeoAdd("cross", []data_structure.GeoPoint{
		{Longitude: 0.0, Latitude: 0.0, Member: "center"},
		{Longitude: 0.1, Latitude: 0.0, Member: "e"},
		{Longitude: -0.1, Latitude: 0.0, Member: "w"},
		{Longitude: 0.0, Latitude: 0.1, Member: "n"},
		{Longitude: 0.0, Latitude: -0.1, Member: "s"},
		{Longitude: 0.1, Latitude: 0.1, Member: "ne"},   // corner - might be outside radius
		{Longitude: -0.1, Latitude: 0.1, Member: "nw"},  // corner
		{Longitude: 0.1, Latitude: -0.1, Member: "se"},  // corner
		{Longitude: -0.1, Latitude: -0.1, Member: "sw"}, // corner
	}, data_structure.ZAddOptions{})

	// Box search should find points within the rectangular area
	boxResults := s.GeoSearch("cross", data_structure.GeoSearchOptions{
		FromLonLat: &data_structure.GeoPoint{Longitude: 0.0, Latitude: 0.0},
		ByBox: &data_structure.GeoBox{
			Width:  30, // ~25km at equator for 0.1 degree
			Height: 30,
		},
		Unit: "km",
	})

	// Should find at least the center and cardinal direction points
	assert.GreaterOrEqual(t, len(boxResults), 5)

	foundCenter := false
	for _, r := range boxResults {
		if r.Member == "center" {
			foundCenter = true
			break
		}
	}
	assert.True(t, foundCenter)
}

func TestGeoAddExpiredKey(t *testing.T) {
	s := NewStore()

	// Add a key with short TTL
	s.SetEx("geo1", "old_value", 0) // expires immediately

	// GeoAdd should create a new key after expiration
	result := s.GeoAdd("geo1", []data_structure.GeoPoint{
		{Longitude: 0.0, Latitude: 0.0, Member: "point"},
	}, data_structure.ZAddOptions{})

	assert.NotNil(t, result)
	assert.Equal(t, uint32(1), *result)
}

func TestGeoSearchRadiusBoundary(t *testing.T) {
	s := NewStore()

	// Add a point exactly at a known distance
	// At equator, 1 degree longitude ≈ 111.32 km
	s.GeoAdd("test", []data_structure.GeoPoint{
		{Longitude: 0.0, Latitude: 0.0, Member: "origin"},
		{Longitude: 1.0, Latitude: 0.0, Member: "far"},
	}, data_structure.ZAddOptions{})

	// Search with radius just under the distance - should only find origin
	results := s.GeoSearch("test", data_structure.GeoSearchOptions{
		FromMember: "origin",
		ByRadius:   50,
		Unit:       "km",
	})
	assert.Equal(t, 1, len(results))
	assert.Equal(t, "origin", results[0].Member)

	// Search with radius that includes both
	results = s.GeoSearch("test", data_structure.GeoSearchOptions{
		FromMember: "origin",
		ByRadius:   200,
		Unit:       "km",
	})
	assert.Equal(t, 2, len(results))
}

func TestGeoDistAllUnits(t *testing.T) {
	s := NewStore()
	// Two points roughly 1km apart
	s.GeoAdd("test", []data_structure.GeoPoint{
		{Longitude: 0.0, Latitude: 0.0, Member: "a"},
		{Longitude: 0.009, Latitude: 0.0, Member: "b"}, // ~1km at equator
	}, data_structure.ZAddOptions{})

	metersResult := s.GeoDist("test", "a", "b", "m")
	kmResult := s.GeoDist("test", "a", "b", "km")
	miResult := s.GeoDist("test", "a", "b", "mi")
	ftResult := s.GeoDist("test", "a", "b", "ft")

	assert.NotNil(t, metersResult)
	assert.NotNil(t, kmResult)
	assert.NotNil(t, miResult)
	assert.NotNil(t, ftResult)

	// Verify relative conversions
	assert.InDelta(t, *metersResult/1000, *kmResult, 0.001)
	assert.InDelta(t, *metersResult*3.28084, *ftResult, 1.0)
	assert.InDelta(t, *metersResult*0.000621371, *miResult, 0.001)
}

func TestGeoSearchResultsContainCorrectDistance(t *testing.T) {
	s := NewStore()
	s.GeoAdd("test", []data_structure.GeoPoint{
		{Longitude: 0.0, Latitude: 0.0, Member: "origin"},
		{Longitude: 1.0, Latitude: 0.0, Member: "far"},
	}, data_structure.ZAddOptions{})

	// Get distance using GeoDist
	directDist := s.GeoDist("test", "origin", "far", "km")

	// Get distance from search result
	results := s.GeoSearch("test", data_structure.GeoSearchOptions{
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

	// Distances should match
	assert.InDelta(t, *directDist, searchDist, 0.1)
}

func TestGeoSearchNoResults(t *testing.T) {
	s := NewStore()
	s.GeoAdd("test", []data_structure.GeoPoint{
		{Longitude: 0.0, Latitude: 0.0, Member: "origin"},
	}, data_structure.ZAddOptions{})

	// Search very far away with small radius
	results := s.GeoSearch("test", data_structure.GeoSearchOptions{
		FromLonLat: &data_structure.GeoPoint{Longitude: 100.0, Latitude: 50.0},
		ByRadius:   1,
		Unit:       "km",
	})

	assert.Empty(t, results)
}

func TestGeoMath(t *testing.T) {
	// Test that our geo calculations are roughly accurate
	// Known distance: New York to Los Angeles is approximately 3,940 km

	s := NewStore()
	s.GeoAdd("us", []data_structure.GeoPoint{
		{Longitude: -73.9352, Latitude: 40.7306, Member: "new_york"},
		{Longitude: -118.2437, Latitude: 34.0522, Member: "los_angeles"},
	}, data_structure.ZAddOptions{})

	dist := s.GeoDist("us", "new_york", "los_angeles", "km")
	assert.NotNil(t, dist)
	// Allow 5% tolerance for geohash precision
	assert.InDelta(t, 3940.0, *dist, 200.0)
}

func TestGeoSearchCountZero(t *testing.T) {
	s := NewStore()
	s.GeoAdd("test", []data_structure.GeoPoint{
		{Longitude: 0.0, Latitude: 0.0, Member: "a"},
		{Longitude: 0.001, Latitude: 0.0, Member: "b"},
		{Longitude: 0.002, Latitude: 0.0, Member: "c"},
	}, data_structure.ZAddOptions{})

	// Count of 0 should return all results (no limit)
	results := s.GeoSearch("test", data_structure.GeoSearchOptions{
		FromMember: "a",
		ByRadius:   100,
		Unit:       "km",
		Count:      0,
	})

	assert.Equal(t, 3, len(results))
}

func TestGeoPosReturnsCorrectMember(t *testing.T) {
	s := NewStore()
	s.GeoAdd("test", []data_structure.GeoPoint{
		{Longitude: -122.4194, Latitude: 37.7749, Member: "sf"},
		{Longitude: -118.2437, Latitude: 34.0522, Member: "la"},
	}, data_structure.ZAddOptions{})

	results := s.GeoPos("test", []string{"sf", "la"})

	assert.Equal(t, "sf", results[0].Member)
	assert.Equal(t, "la", results[1].Member)
}

func TestGeoSearchInfinity(t *testing.T) {
	s := NewStore()
	s.GeoAdd("test", []data_structure.GeoPoint{
		{Longitude: 0.0, Latitude: 0.0, Member: "origin"},
		{Longitude: 179.0, Latitude: 85.0, Member: "far"},
	}, data_structure.ZAddOptions{})

	// Large radius should still work
	results := s.GeoSearch("test", data_structure.GeoSearchOptions{
		FromMember: "origin",
		ByRadius:   math.MaxFloat64 / 2, // very large but not infinity
		Unit:       "m",
	})

	assert.Equal(t, 2, len(results))
}
