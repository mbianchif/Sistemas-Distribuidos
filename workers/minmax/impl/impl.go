package impl

import (
	"fmt"
	"strconv"
	"workers"
	"workers/minmax/config"
	"workers/protocol"

	"github.com/op/go-logging"
)

type tuple struct {
	fieldMap map[string]string
	value    float64
}

type MinMax struct {
	*workers.Worker
	Con *config.MinMaxConfig
	min tuple
	max tuple
}

func New(con *config.MinMaxConfig, log *logging.Logger) (*MinMax, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}
	return &MinMax{base, con, tuple{nil, 0}, tuple{nil, 0}}, nil
}

func (w *MinMax) Run() error {
	return w.Worker.Run(w)
}

func (w *MinMax) Batch(data []byte) bool {
	batch, err := protocol.DecodeBatch(data)
	if err != nil {
		w.Log.Fatal("failed to decode batch: %v", err)
	}

	for _, fieldMap := range batch.FieldMaps {
		err := handleMinMax(w, fieldMap)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			continue
		}
	}

	return false
}

func (w *MinMax) Eof(data []byte) bool {
	responseFieldMaps := []map[string]string{
		w.min.fieldMap,
		w.max.fieldMap,
	}

	w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
	batch := protocol.NewBatch(responseFieldMaps)
	if err := w.PublishBatch(batch); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	eof := protocol.DecodeEof(data)
	if err := w.PublishEof(eof); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	return true
}

func (w *MinMax) Error(data []byte) bool {
	w.Log.Error("Received an ERROR message kind")
	return true
}

func handleMinMax(w *MinMax, fieldMap map[string]string) error {
	if _, ok := fieldMap[w.Con.Key]; !ok {
		return fmt.Errorf("key %v was not found in the field map", w.Con.Key)
	}

	value, err := strconv.ParseFloat(fieldMap[w.Con.Key], 64)
	if err != nil {
		return err
	}

	if w.max.fieldMap == nil {
		w.max = tuple{fieldMap, value}
	}
	if w.min.fieldMap == nil {
		w.min = tuple{fieldMap, value}
	}

	if value > w.max.value {
		w.max = tuple{fieldMap, value}
	}

	if value < w.min.value {
		w.min = tuple{fieldMap, value}
	}

	return nil
}
