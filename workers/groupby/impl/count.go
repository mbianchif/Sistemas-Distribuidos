package impl

import (
	"fmt"
	"strconv"
	"strings"
	"workers/groupby/config"
)

type Count struct {
	*Groupby
	state map[string]int
}

func NewCount(w *Groupby) GroupbyHandler {
	return &Count{w, make(map[string]int)}
}

func (w *Count) Add(fieldMap map[string]string, con *config.GroupbyConfig) error {
	keys := make([]string, 0, len(con.GroupKeys))
	for _, key := range con.GroupKeys {
		field, ok := fieldMap[key]
		if !ok {
			return fmt.Errorf("key %v was not found", key)
		}
		keys = append(keys, field)
	}

	compKey := strings.Join(keys, SEP)
	w.state[compKey] += 1
	return nil
}

func (w *Count) Result(con *config.GroupbyConfig) []map[string]string {
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
