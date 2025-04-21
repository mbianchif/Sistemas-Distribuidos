package impl

import (
	"fmt"
	"strconv"
	"strings"
	"workers/groupby/config"
)

type tuple struct {
	sum float64
	n   int
}

type Mean struct {
	*Groupby
	state map[string]tuple
}

func NewMean(w *Groupby) GroupbyHandler {
	return &Mean{w, make(map[string]tuple)}
}

func (w *Mean) Add(fieldMap map[string]string, con *config.GroupbyConfig) error {
	sumValueStr, ok := fieldMap[con.AggKey]
	if !ok {
		return fmt.Errorf("value %v was not found", con.AggKey)
	}

	sumValue, err := strconv.ParseFloat(sumValueStr, 64)
	if err != nil {
		return fmt.Errorf("the sum value is not numerical %v", sumValueStr)
	}

	keys := make([]string, 0, len(con.GroupKeys))
	for _, key := range con.GroupKeys {
		field, ok := fieldMap[key]
		if !ok {
			return fmt.Errorf("key %v was not found", key)
		}
		keys = append(keys, field)
	}

	compKey := strings.Join(keys, ",")
	if tup, ok := w.state[compKey]; !ok {
		w.state[compKey] = tuple{sumValue, 1}
	} else {
		w.state[compKey] = tuple{tup.sum + sumValue, tup.n + 1}
	}

	return nil
}

func (w *Mean) Result(con *config.GroupbyConfig) []map[string]string {
	fieldMaps := make([]map[string]string, 0, len(w.state))
	for compKey, v := range w.state {
		fieldMap := make(map[string]string)

		keys := strings.Split(compKey, ",")
		for i, key := range keys {
			fieldMap[con.GroupKeys[i]] = key
		}

		fieldMap[con.Storage] = strconv.FormatFloat(v.sum/float64(v.n), 'f', 4, 64)
		fieldMaps = append(fieldMaps, fieldMap)
	}

	fmt.Println(fieldMaps)
	return fieldMaps
}
