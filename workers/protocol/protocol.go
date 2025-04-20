package protocol

import (
	"bytes"
	"strconv"
)

var name2Id = map[string]int{
	// movies
	"id":                   0,
	"title":                1,
	"release_date":         2,
	"overview":             3,
	"budget":               4,
	"revenue":              5,
	"genres":               6,
	"production_countries": 7,
	"spoken_languages":     8,

	// ratings
	"movieId":   9,
	"rating":    10,
	"timestamp": 11,

	// credits
	"cast": 12,

	// Added
	"rate_revenue_budget": 13,
	"sentiment":           14,
	"query":               15,
}

var id2Name = []string{
	// movies
	"id",
	"title",
	"release_date",
	"overview",
	"budget",
	"revenue",
	"genres",
	"production_countries",
	"spoken_languages",

	// ratings
	"movieId",
	"rating",
	"timestamp",

	// credits
	"cast",

	// Added
	"rate_revenue_budget",
}

func Encode(fields map[string]string, filterCols map[string]struct{}) []byte {
	it := 0
	bytes := make([]byte, 0, 512)
	for k, v := range fields {
		if len(filterCols) > 0 {
			if _, ok := filterCols[k]; !ok {
				it += 1
				continue
			}
		}

		kId := strconv.Itoa(name2Id[k])
		bytes = append(bytes, []byte(kId)...)
		bytes = append(bytes, '=')
		bytes = append(bytes, []byte(v)...)
		if it < len(fields)-1 {
			bytes = append(bytes, ';')
		}

		it += 1
	}

	return bytes
}

func Decode(data []byte) (map[string]string, error) {
	fields := make(map[string]string, 12)

	for kv := range bytes.SplitSeq(data, []byte(";")) {
		pair := bytes.Split(kv, []byte("="))
		if len(pair) != 2 {
			continue
		}

		keyNum, err := strconv.Atoi(string(pair[0]))
		if err != nil || keyNum >= len(id2Name) {
			continue
		}

		keyName := id2Name[keyNum]
		fields[keyName] = string(pair[1])
	}

	return fields, nil
}
