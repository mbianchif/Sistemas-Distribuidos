package impl

import (
	"fmt"
	"strconv"

	"analyzer/comms"
	"analyzer/workers"
	"analyzer/workers/minmax/config"

	"github.com/op/go-logging"
)

type tuple struct {
	fieldMap map[string]string
	value    float64
}

type MinMax struct {
	*workers.Worker
	Con  *config.MinMaxConfig
	mins map[int]tuple
	maxs map[int]tuple
}

func New(con *config.MinMaxConfig, log *logging.Logger) (*MinMax, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}

	minmax := MinMax{
		Worker: base,
		Con:    con,
		mins:   make(map[int]tuple),
		maxs:   make(map[int]tuple),
	}

	return &minmax, nil
}

func (w *MinMax) Run() error {
	return w.Worker.Run(w)
}

func (w *MinMax) Clean(clientId int) {
	w.mins[clientId] = tuple{nil, 0}
	w.maxs[clientId] = tuple{nil, 0}
}

func (w *MinMax) Batch(clientId int, data []byte) bool {
	batch, err := comms.DecodeBatch(data)
	if err != nil {
		w.Log.Fatal("failed to decode batch: %v", err)
	}

	for _, fieldMap := range batch.FieldMaps {
		err := handleMinMax(w, clientId, fieldMap)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			continue
		}
	}

	return false
}

func (w *MinMax) Eof(clientId int, data []byte) bool {
	responseFieldMaps := []map[string]string{
		w.mins[clientId].fieldMap,
		w.maxs[clientId].fieldMap,
	}

	w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
	batch := comms.NewBatch(responseFieldMaps)
	if err := w.Mailer.PublishBatch(batch, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	eof := comms.DecodeEof(data)
	if err := w.Mailer.PublishEof(eof, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	w.Clean(clientId)
	return true
}

func handleMinMax(w *MinMax, clientId int, fieldMap map[string]string) error {
	if _, ok := fieldMap[w.Con.Key]; !ok {
		return fmt.Errorf("key %v was not found in the field map", w.Con.Key)
	}

	value, err := strconv.ParseFloat(fieldMap[w.Con.Key], 64)
	if err != nil {
		return err
	}

	max := w.maxs[clientId]
	min := w.mins[clientId]

	if max.fieldMap == nil {
		max = tuple{fieldMap, value}
	}
	if min.fieldMap == nil {
		min = tuple{fieldMap, value}
	}

	if value > max.value {
		max = tuple{fieldMap, value}
	}
	if value < min.value {
		min = tuple{fieldMap, value}
	}

	w.maxs[clientId] = max
	w.mins[clientId] = min
	return nil
}
