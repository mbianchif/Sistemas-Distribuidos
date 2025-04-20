package impl

import (
	"fmt"
	"strconv"
	"strings"
	"workers/groupby/config"
)

type Sum struct {
	*Groupby
	state map[string]float64
}

func NewSum(w *Groupby) GroupbyHandler {
	return &Sum{w, make(map[string]float64)}
}

func (w *Sum) Add(fieldMap map[string]string, con *config.GroupbyConfig) error {
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
	w.state[compKey] += sumValue
	fmt.Println(w.state)
	return nil
}

func (w *Sum) Result(con *config.GroupbyConfig) []map[string]string {
	fieldMaps := make([]map[string]string, 0, len(w.state))
	for compKey, v := range w.state {
		fieldMap := make(map[string]string)

		keys := strings.Split(compKey, ",")
		for i, key := range keys {
			fieldMap[key] = con.GroupKeys[i]
		}


		fieldMap[con.Storage] = strconv.FormatFloat(v, 'f', 4, 64)
		fieldMaps = append(fieldMaps, fieldMap)
	}

	return fieldMaps
}
