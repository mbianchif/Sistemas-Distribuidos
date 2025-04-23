package impl

import (
	"fmt"
	"strconv"
	"strings"
	"workers/groupby/config"
)

type Sum struct {
	*Groupby
	state map[string]int
}

func NewSum(w *Groupby) GroupbyHandler {
	return &Sum{w, make(map[string]int)}
}

func (w *Sum) Add(fieldMap map[string]string, con *config.GroupbyConfig) error {
	sumValueStr, ok := fieldMap[con.AggKey]
	if !ok {
		return fmt.Errorf("value %v was not found", con.AggKey)
	}

	sumValue, err := strconv.Atoi(sumValueStr)
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

	compKey := strings.Join(keys, SEP)
	w.state[compKey] += sumValue
	return nil
}

func (w *Sum) Result(con *config.GroupbyConfig) []map[string]string {
	fieldMaps := make([]map[string]string, 0, len(w.state))
	for compKey, v := range w.state {
		fieldMap := make(map[string]string)

		keys := strings.Split(compKey, SEP)
		for i, key := range keys {
			fieldMap[con.GroupKeys[i]] = key
		}

		fieldMap[con.Storage] = strconv.Itoa(v)
		fieldMaps = append(fieldMaps, fieldMap)
	}

	return fieldMaps
}
