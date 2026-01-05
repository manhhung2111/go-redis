package data_structure

import (
	"math"
	"sort"
)

// GeoAdd adds geospatial items to the sorted set
func (zset *zSet) GeoAdd(items []GeoPoint, options ZAddOptions) *uint32 {
	scoreMember := make(map[string]float64, len(items))
	for _, item := range items {
		hash := GeoHashEncode(item.Longitude, item.Latitude)
		scoreMember[item.Member] = float64(hash)
	}
	return zset.ZAdd(scoreMember, options)
}

// GeoDist returns the distance between two members in the specified unit
func (zset *zSet) GeoDist(member1, member2 string, unit string) *float64 {
	score1, exists1 := zset.data[member1]
	score2, exists2 := zset.data[member2]

	if !exists1 || !exists2 {
		return nil
	}

	lon1, lat1 := GeoHashDecode(uint64(score1))
	lon2, lat2 := GeoHashDecode(uint64(score2))

	distance := HaversineDistance(lon1, lat1, lon2, lat2)
	converted := ConvertDistance(distance, unit)

	return &converted
}

// GeoHash returns the geohash strings for the specified members
func (zset *zSet) GeoHash(members []string) []*string {
	result := make([]*string, len(members))

	for i, member := range members {
		score, exists := zset.data[member]
		if !exists {
			result[i] = nil
			continue
		}

		hashStr := GeoHashToString(uint64(score))
		result[i] = &hashStr
	}

	return result
}

// GeoPos returns the longitude and latitude for the specified members
func (zset *zSet) GeoPos(members []string) []*GeoPoint {
	result := make([]*GeoPoint, len(members))

	for i, member := range members {
		score, exists := zset.data[member]
		if !exists {
			result[i] = nil
			continue
		}

		lon, lat := GeoHashDecode(uint64(score))
		result[i] = &GeoPoint{
			Longitude: lon,
			Latitude:  lat,
			Member:    member,
		}
	}

	return result
}

// GeoSearch searches for members within the specified area
func (zset *zSet) GeoSearch(options GeoSearchOptions) []GeoResult {
	var centerLon, centerLat float64

	// Determine center point
	if options.FromLonLat != nil {
		centerLon = options.FromLonLat.Longitude
		centerLat = options.FromLonLat.Latitude
	} else if options.FromMember != "" {
		score, exists := zset.data[options.FromMember]
		if !exists {
			return nil
		}
		centerLon, centerLat = GeoHashDecode(uint64(score))
	} else {
		return nil
	}

	// Calculate search radius in meters
	var radiusMeters float64
	var halfWidthMeters, halfHeightMeters float64

	if options.ByRadius > 0 {
		radiusMeters = ConvertToMeters(options.ByRadius, options.Unit)
	} else if options.ByBox != nil {
		// For box search, calculate half dimensions
		halfWidthMeters = ConvertToMeters(options.ByBox.Width/2, options.Unit)
		halfHeightMeters = ConvertToMeters(options.ByBox.Height/2, options.Unit)
		// Use diagonal as radius for initial broad filtering
		radiusMeters = math.Sqrt(halfWidthMeters*halfWidthMeters+halfHeightMeters*halfHeightMeters) * 2
	}

	// Collect all candidates and filter by actual distance
	var results []GeoResult

	for member, score := range zset.data {
		lon, lat := GeoHashDecode(uint64(score))
		distance := HaversineDistance(centerLon, centerLat, lon, lat)

		var inRange bool
		if options.ByRadius > 0 {
			inRange = distance <= radiusMeters
		} else if options.ByBox != nil {
			// Check if point is within the box
			// Calculate approximate distances for longitude and latitude separately
			lonDist := HaversineDistance(centerLon, centerLat, lon, centerLat)
			latDist := HaversineDistance(centerLon, centerLat, centerLon, lat)

			inRange = lonDist <= halfWidthMeters && latDist <= halfHeightMeters
		}

		if inRange {
			results = append(results, GeoResult{
				Member:    member,
				Distance:  ConvertDistance(distance, options.Unit),
				Hash:      uint64(score),
				Longitude: lon,
				Latitude:  lat,
			})
		}
	}

	// Sort results
	if options.Descending {
		sort.Slice(results, func(i, j int) bool {
			return results[i].Distance > results[j].Distance
		})
	} else {
		// Default or ASC
		sort.Slice(results, func(i, j int) bool {
			return results[i].Distance < results[j].Distance
		})
	}

	// Apply count limit
	if options.Count > 0 && len(results) > options.Count {
		results = results[:options.Count]
	}

	return results
}
