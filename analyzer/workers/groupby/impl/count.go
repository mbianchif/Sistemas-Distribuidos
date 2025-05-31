package impl

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"analyzer/comms/middleware"
	"analyzer/comms/persistance"
	"analyzer/workers/groupby/config"
)

type Count struct {
	*GroupBy
	state map[string]int
}

func NewCount(w *GroupBy) GroupByHandler {
	return &Count{
		GroupBy: w,
		state:   make(map[string]int),
	}
}

func (w *Count) add(fieldMap map[string]string, con *config.GroupbyConfig) error {
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

func (w *Count) result(clientId int, con *config.GroupbyConfig, persistor persistance.Persistor) []map[string]string {
	persistedFiles, err := persistor.RecoverFor(clientId)
	if err != nil {
		return nil
	}

	fieldMaps := make([]map[string]string, 0)
	for pf := range persistedFiles {
		count, err := w.decode(pf.State)
		if err != nil {
			continue
		}

		fieldMap := make(map[string]string)
		keys := strings.Split(pf.FileName, SEP)
		for i, key := range keys {
			fieldMap[con.GroupKeys[i]] = key
		}

		fieldMap[con.Storage] = strconv.Itoa(count)
		fieldMaps = append(fieldMaps, fieldMap)
	}

	return fieldMaps
}

func (w *Count) encode(count int) []byte {
	return fmt.Appendf(nil, "%d\n", count)
}

func (w *Count) decode(state []byte) (int, error) {
	stateStr := string(bytes.TrimSpace(state))
	count, err := strconv.Atoi(stateStr)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (w *Count) store(id middleware.DelId, persistor *persistance.Persistor) error {
	replicaId := id.ReplicaId
	clientId := id.ClientId
	seq := id.Seq

	for k, partialCount := range w.state {
		lastReplicaId, lastSeq, state, err := persistor.Load(clientId, k)
		exists := err == nil

		if !exists {
			newState := w.encode(partialCount)
			persistor.Store(id, k, newState)
			continue
		}

		if seq <= lastSeq && replicaId == lastReplicaId {
			continue
		}

		prevCount, err := w.decode(state)
		if err != nil {
			continue
		}

		newState := w.encode(prevCount + partialCount)
		persistor.Store(id, k, newState)
	}

	w.state = make(map[string]int)
	return nil
}
