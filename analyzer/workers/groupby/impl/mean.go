package impl

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"analyzer/comms"
	"analyzer/comms/middleware"
	"analyzer/comms/persistance"
	"analyzer/workers/groupby/config"
)

type tuple struct {
	sum float64
	n   int
}

type Mean struct {
	*GroupBy
	state map[string]tuple
}

func NewMean(w *GroupBy) GroupByHandler {
	return &Mean{
		GroupBy: w,
		state:   make(map[string]tuple),
	}
}

func (w *Mean) add(shards map[string][]map[string]string, con config.GroupByConfig) error {
	for compKey, fieldMaps := range shards {
		for _, fieldMap := range fieldMaps {
			sumValueStr, ok := fieldMap[con.AggKey]
			if !ok {
				return fmt.Errorf("value %v was not found", con.AggKey)
			}

			sumValue, err := strconv.ParseFloat(sumValueStr, 64)
			if err != nil {
				return fmt.Errorf("the sum value is not numerical %v", sumValueStr)
			}

			tup := w.state[compKey]
			w.state[compKey] = tuple{tup.sum + sumValue, tup.n + 1}
		}
	}

	return nil
}

func (w *Mean) encode(sum float64, n int) []byte {
	return fmt.Appendf(nil, "%f %d\n", sum, n)
}

func (w *Mean) decode(state []byte) (float64, int, error) {
	values := strings.Split(string(bytes.TrimSpace(state)), " ")
	if len(values) != 2 {
		return 0, 0, fmt.Errorf("invalid amount of values, should be %d: %v", 2, values)
	}

	sum, err := strconv.ParseFloat(values[0], 64)
	if err != nil {
		return 0, 0, err
	}

	n, err := strconv.Atoi(values[1])
	if err != nil {
		return 0, 0, err
	}

	return sum, n, nil
}

func (w *Mean) result(clientId int, con config.GroupByConfig, persistor persistance.Persistor) ([]map[string]string, error) {
	persistedFiles, err := persistor.RecoverFor(clientId)
	if err != nil {
		return nil, err
	}

	fieldMaps := make([]map[string]string, 0)
	for pf := range persistedFiles {
		sum, n, err := w.decode(pf.State)
		if err != nil {
			continue
		}

		fieldMap := make(map[string]string)
		keys := strings.Split(pf.FileName, comms.SEP)
		for i, key := range keys {
			fieldMap[con.GroupKeys[i]] = key
		}

		fieldMap[con.Storage] = strconv.FormatFloat(sum/float64(n), 'f', 4, 64)
		fieldMaps = append(fieldMaps, fieldMap)
	}

	return fieldMaps, nil
}

func (w *Mean) store(id middleware.DelId, persistor *persistance.Persistor) error {
	defer func() { w.state = make(map[string]tuple) }()

	replicaId := id.ReplicaId
	clientId := id.ClientId
	seq := id.Seq

	for compKey, partialTup := range w.state {
		pf, err := persistor.Load(clientId, compKey)
		exists := err == nil
		if !exists {
			newState := w.encode(partialTup.sum, partialTup.n)
			persistor.Store(id, compKey, newState)
			continue
		}

		h := pf.Header
		if seq <= h.Seqs[replicaId] {
			w.Log.Infof("duplicado")
			continue
		}

		prevSum, prevCount, err := w.decode(pf.State)
		if err != nil {
			w.Log.Errorf("failed to decode state: %v", err)
			continue
		}

		newState := w.encode(prevSum+partialTup.sum, prevCount+partialTup.n)
		persistor.Store(id, compKey, newState, h)
	}

	return nil
}
