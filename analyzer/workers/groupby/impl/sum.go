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

type Sum struct {
	*GroupBy
	state map[string]int
}

func NewSum(w *GroupBy) GroupByHandler {
	return &Sum{
		GroupBy: w,
		state:   make(map[string]int),
	}
}

func (w *Sum) add(fieldMap map[string]string, con *config.GroupbyConfig) error {
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

func (w *Sum) result(clientId int, con *config.GroupbyConfig, persistor persistance.Persistor) []map[string]string {
	persistedFiles, err := persistor.RecoverFor(clientId)
	if err != nil {
		return nil
	}

	fieldMaps := make([]map[string]string, 0)
	for pf := range persistedFiles {
		sum, err := w.decode(pf.State)
		if err != nil {
			continue
		}

		fieldMap := make(map[string]string)
		keys := strings.Split(pf.FileName, SEP)
		for i, key := range keys {
			fieldMap[con.GroupKeys[i]] = key
		}

		fieldMap[con.Storage] = strconv.Itoa(sum)
		fieldMaps = append(fieldMaps, fieldMap)
	}

	return fieldMaps
}

func (w *Sum) encode(sum int) []byte {
	return fmt.Appendf(nil, "%d\n", sum)
}

func (w *Sum) decode(state []byte) (int, error) {
	stateStr := string(bytes.TrimSpace(state))
	sum, err := strconv.Atoi(stateStr)
	if err != nil {
		return 0, err
	}
	return sum, nil
}

func (w *Sum) store(id middleware.DelId, persistor *persistance.Persistor) error {
	replicaId := id.ReplicaId
	clientId := id.ClientId
	seq := id.Seq

	for k, partialSum := range w.state {
		lastReplicaId, lastSeq, state, err := persistor.Load(clientId, k)
		exists := err == nil

		if !exists {
			newState := w.encode(partialSum)
			persistor.Store(id, k, newState)
			continue
		}

		if seq <= lastSeq && replicaId == lastReplicaId {
			continue
		}

		prevSum, err := w.decode(state)
		if err != nil {
			continue
		}

		newState := w.encode(prevSum + partialSum)
		persistor.Store(id, k, newState)
	}

	w.state = make(map[string]int)
	return nil
}
