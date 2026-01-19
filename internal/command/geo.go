package command

import (
	"strconv"
	"strings"

	"github.com/manhhung2111/go-redis/internal/protocol"
	"github.com/manhhung2111/go-redis/internal/storage/types"
	"github.com/manhhung2111/go-redis/internal/errors"
)

/* Support GEOADD key [NX | XX] [CH] longitude latitude member [longitude latitude member...] */
func (redis *redis) GeoAdd(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 4 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	options := types.ZAddOptions{}
	i := 1

	// Parse options
	for i < len(args) {
		if _, err := strconv.ParseFloat(args[i], 64); err == nil {
			break // First longitude found
		}

		switch strings.ToUpper(args[i]) {
		case "NX":
			options.NX = true
		case "XX":
			options.XX = true
		case "CH":
			options.CH = true
		default:
			return protocol.RespSyntaxError
		}
		i++
	}

	if options.NX && options.XX {
		return protocol.RespXXNXNotCompatible
	}

	// Remaining args should be longitude latitude member triplets
	remaining := len(args) - i
	if remaining == 0 || remaining%3 != 0 {
		return protocol.RespSyntaxError
	}

	items := make([]types.GeoPoint, 0, remaining/3)

	for i < len(args) {
		longitude, err := strconv.ParseFloat(args[i], 64)
		if err != nil {
			return protocol.RespValueNotValidFloat
		}

		latitude, err := strconv.ParseFloat(args[i+1], 64)
		if err != nil {
			return protocol.RespValueNotValidFloat
		}

		if !types.ValidateCoordinates(longitude, latitude) {
			return protocol.RespInvalidLongitudeLatitude
		}

		member := args[i+2]
		items = append(items, types.GeoPoint{
			Longitude: longitude,
			Latitude:  latitude,
			Member:    member,
		})

		i += 3
	}

	result, err := redis.Store.GeoAdd(args[0], items, options)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	if result == nil {
		return protocol.RespSyntaxError
	}

	return protocol.EncodeResp(*result, false)
}

/* Support GEODIST key member1 member2 [M | KM | FT | MI] */
func (redis *redis) GeoDist(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 3 || len(args) > 4 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	unit := "m"
	if len(args) == 4 {
		unit = strings.ToLower(args[3])
		if unit != "m" && unit != "km" && unit != "ft" && unit != "mi" {
			return protocol.RespSyntaxError
		}
	}

	result, err := redis.Store.GeoDist(args[0], args[1], args[2], unit)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	if result == nil {
		return protocol.RespNilBulkString
	}

	return protocol.EncodeResp(*result, false)
}

/* Support GEOHASH key member [member ...] */
func (redis *redis) GeoHash(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	result, err := redis.Store.GeoHash(args[0], args[1:])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	return protocol.EncodeResp(result, false)
}

/* Support GEOPOS key member [member ...] */
func (redis *redis) GeoPos(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 2 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	points, err := redis.Store.GeoPos(args[0], args[1:])
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	// Convert to array of arrays for RESP encoding
	result := make([]any, len(points))
	for i, pt := range points {
		if pt == nil {
			result[i] = nil
		} else {
			result[i] = []string{
				strconv.FormatFloat(pt.Longitude, 'f', -1, 64),
				strconv.FormatFloat(pt.Latitude, 'f', -1, 64),
			}
		}
	}

	return protocol.EncodeResp(result, false)
}

/*
Support GEOSEARCH key [FROMMEMBER member | FROMLONLAT longitude latitude]

	[BYRADIUS radius M | KM | FT | MI | BYBOX width height M | KM | FT | MI]
	[ASC | DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
*/
func (redis *redis) GeoSearch(cmd protocol.RedisCmd) []byte {
	args := cmd.Args
	if len(args) < 4 {
		return protocol.EncodeResp(errors.InvalidNumberOfArgs(cmd.Cmd), false)
	}

	options := types.GeoSearchOptions{
		Unit: "m", // Default unit
	}

	i := 1
	for i < len(args) {
		opt := strings.ToUpper(args[i])

		switch opt {
		case "FROMMEMBER":
			if i+1 >= len(args) {
				return protocol.RespSyntaxError
			}
			options.FromMember = args[i+1]
			i += 2

		case "FROMLONLAT":
			if i+2 >= len(args) {
				return protocol.RespSyntaxError
			}
			lon, err := strconv.ParseFloat(args[i+1], 64)
			if err != nil {
				return protocol.RespValueNotValidFloat
			}
			lat, err := strconv.ParseFloat(args[i+2], 64)
			if err != nil {
				return protocol.RespValueNotValidFloat
			}
			if !types.ValidateCoordinates(lon, lat) {
				return protocol.RespInvalidLongitudeLatitude
			}
			options.FromLonLat = &types.GeoPoint{Longitude: lon, Latitude: lat}
			i += 3

		case "BYRADIUS":
			if i+2 >= len(args) {
				return protocol.RespSyntaxError
			}
			radius, err := strconv.ParseFloat(args[i+1], 64)
			if err != nil || radius < 0 {
				return protocol.RespValueNotValidFloat
			}
			options.ByRadius = radius
			options.Unit = strings.ToLower(args[i+2])
			if !isValidGeoUnit(options.Unit) {
				return protocol.RespSyntaxError
			}
			i += 3

		case "BYBOX":
			if i+3 >= len(args) {
				return protocol.RespSyntaxError
			}
			width, err := strconv.ParseFloat(args[i+1], 64)
			if err != nil || width < 0 {
				return protocol.RespValueNotValidFloat
			}
			height, err := strconv.ParseFloat(args[i+2], 64)
			if err != nil || height < 0 {
				return protocol.RespValueNotValidFloat
			}
			options.ByBox = &types.GeoBox{Width: width, Height: height}
			options.Unit = strings.ToLower(args[i+3])
			if !isValidGeoUnit(options.Unit) {
				return protocol.RespSyntaxError
			}
			i += 4

		case "ASC":
			options.Ascending = true
			i++

		case "DESC":
			options.Descending = true
			i++

		case "COUNT":
			if i+1 >= len(args) {
				return protocol.RespSyntaxError
			}
			count, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil || count < 0 {
				return protocol.RespValueOutOfRangeMustPositive
			}
			options.Count = int(count)
			i += 2
			// Check for optional ANY
			if i < len(args) && strings.ToUpper(args[i]) == "ANY" {
				options.Any = true
				i++
			}

		case "WITHCOORD":
			options.WithCoord = true
			i++

		case "WITHDIST":
			options.WithDist = true
			i++

		case "WITHHASH":
			options.WithHash = true
			i++

		default:
			return protocol.RespSyntaxError
		}
	}

	// Validate required options
	if options.FromMember == "" && options.FromLonLat == nil {
		return protocol.RespGeoFromMemberOrFromLonLatReq
	}
	if options.FromMember != "" && options.FromLonLat != nil {
		return protocol.RespGeoFromMemberOrFromLonLatReq
	}
	if options.ByRadius == 0 && options.ByBox == nil {
		return protocol.RespGeoByRadiusOrByBoxReq
	}
	if options.ByRadius > 0 && options.ByBox != nil {
		return protocol.RespGeoByRadiusOrByBoxReq
	}
	if options.Ascending && options.Descending {
		return protocol.RespSyntaxError
	}

	results, err := redis.Store.GeoSearch(args[0], options)
	if err != nil {
		return protocol.EncodeResp(err, false)
	}

	if results == nil {
		return protocol.EncodeResp([]string{}, false)
	}

	// Format output based on options
	return formatGeoSearchResults(results, options)
}

func isValidGeoUnit(unit string) bool {
	return unit == "m" || unit == "km" || unit == "ft" || unit == "mi"
}

func formatGeoSearchResults(results []types.GeoResult, options types.GeoSearchOptions) []byte {
	if !options.WithCoord && !options.WithDist && !options.WithHash {
		// Simple array of member names
		members := make([]string, len(results))
		for i, r := range results {
			members[i] = r.Member
		}
		return protocol.EncodeResp(members, false)
	}

	// Array of arrays with additional info
	output := make([]any, len(results))
	for i, r := range results {
		entry := []any{r.Member}

		if options.WithDist {
			entry = append(entry, strconv.FormatFloat(r.Distance, 'f', 4, 64))
		}
		if options.WithHash {
			entry = append(entry, r.Hash)
		}
		if options.WithCoord {
			entry = append(entry, []string{
				strconv.FormatFloat(r.Longitude, 'f', -1, 64),
				strconv.FormatFloat(r.Latitude, 'f', -1, 64),
			})
		}

		output[i] = entry
	}

	return protocol.EncodeResp(output, false)
}
