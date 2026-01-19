package types

import (
	"math"
)

const (
	GeoHashBits    = 52 // Redis uses 52-bit geohash
	GeoHashMaxStep = 26 // 52 bits / 2

	// Earth radius in meters
	EarthRadiusMeters = 6372797.560856

	// Coordinate bounds
	MinLongitude = -180.0
	MaxLongitude = 180.0
	MinLatitude  = -85.05112878
	MaxLatitude  = 85.05112878

	// Distance unit conversions
	MeterToKm   = 0.001
	MeterToMile = 0.000621371
	MeterToFeet = 3.28084
)

// GeoPoint represents a geographical point
type GeoPoint struct {
	Longitude float64
	Latitude  float64
	Member    string
	Distance  float64 // Used for GEOSEARCH results
	GeoHash   uint64  // Stored geohash
}

// GeoSearchOptions contains options for GEOSEARCH command
type GeoSearchOptions struct {
	FromMember string
	FromLonLat *GeoPoint
	ByRadius   float64
	ByBox      *GeoBox
	Unit       string
	Ascending  bool
	Descending bool
	Count      int
	Any        bool
	WithCoord  bool
	WithDist   bool
	WithHash   bool
}

// GeoBox represents a bounding box for BYBOX searches
type GeoBox struct {
	Width  float64
	Height float64
}

// GeoResult represents a single result from GEOSEARCH
type GeoResult struct {
	Member    string
	Distance  float64
	Hash      uint64
	Longitude float64
	Latitude  float64
}

// GeoHashEncode encodes latitude and longitude to a 52-bit geohash
func GeoHashEncode(longitude, latitude float64) uint64 {
	// Normalize coordinates to [0, 1] range
	lonNorm := (longitude - MinLongitude) / (MaxLongitude - MinLongitude)
	latNorm := (latitude - MinLatitude) / (MaxLatitude - MinLatitude)

	var hash uint64
	lonBits := uint64(lonNorm * float64(uint64(1)<<GeoHashMaxStep))
	latBits := uint64(latNorm * float64(uint64(1)<<GeoHashMaxStep))

	// Interleave bits: longitude gets even positions, latitude gets odd positions
	for i := 0; i < GeoHashMaxStep; i++ {
		hash |= (lonBits & (1 << i)) << i
		hash |= (latBits & (1 << i)) << (i + 1)
	}

	return hash
}

// GeoHashDecode decodes a 52-bit geohash back to latitude and longitude
func GeoHashDecode(hash uint64) (longitude, latitude float64) {
	var lonBits, latBits uint64

	// De-interleave bits
	for i := 0; i < GeoHashMaxStep; i++ {
		lonBits |= ((hash >> (2 * i)) & 1) << i
		latBits |= ((hash >> (2*i + 1)) & 1) << i
	}

	lonNorm := float64(lonBits) / float64(uint64(1)<<GeoHashMaxStep)
	latNorm := float64(latBits) / float64(uint64(1)<<GeoHashMaxStep)

	longitude = lonNorm*(MaxLongitude-MinLongitude) + MinLongitude
	latitude = latNorm*(MaxLatitude-MinLatitude) + MinLatitude

	return longitude, latitude
}

// GeoHashToString converts a 52-bit geohash to an 11-character base32 string
func GeoHashToString(hash uint64) string {
	const base32 = "0123456789bcdefghjkmnpqrstuvwxyz"

	result := make([]byte, 11)
	paddedHash := hash << 3

	for i := 0; i < 11; i++ {
		// Extract 5 bits at a time from the most significant end
		shift := (10 - i) * 5
		idx := (paddedHash >> shift) & 0x1F
		result[i] = base32[idx]
	}

	return string(result)
}

// HaversineDistance calculates the distance between two points in meters
func HaversineDistance(lon1, lat1, lon2, lat2 float64) float64 {
	// Convert to radians
	lon1Rad := lon1 * math.Pi / 180
	lat1Rad := lat1 * math.Pi / 180
	lon2Rad := lon2 * math.Pi / 180
	lat2Rad := lat2 * math.Pi / 180

	dLon := lon2Rad - lon1Rad
	dLat := lat2Rad - lat1Rad

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*math.Sin(dLon/2)*math.Sin(dLon/2)

	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return EarthRadiusMeters * c
}

// ConvertDistance converts meters to the specified unit
func ConvertDistance(meters float64, unit string) float64 {
	switch unit {
	case "km":
		return meters * MeterToKm
	case "mi":
		return meters * MeterToMile
	case "ft":
		return meters * MeterToFeet
	default: // "m"
		return meters
	}
}

// ConvertToMeters converts from specified unit to meters
func ConvertToMeters(distance float64, unit string) float64 {
	switch unit {
	case "km":
		return distance / MeterToKm
	case "mi":
		return distance / MeterToMile
	case "ft":
		return distance / MeterToFeet
	default: // "m"
		return distance
	}
}

// ValidateCoordinates checks if coordinates are within valid bounds
func ValidateCoordinates(longitude, latitude float64) bool {
	return longitude >= MinLongitude && longitude <= MaxLongitude &&
		latitude >= MinLatitude && latitude <= MaxLatitude
}
