package impl

import (
	"fmt"
	"strconv"

	"analyzer/workers"
	"analyzer/workers/divider/config"
	"analyzer/comms"

	"github.com/op/go-logging"
)

type Divider struct {
	*workers.Worker
	Con *config.DividerConfig
}

func New(con *config.DividerConfig, log *logging.Logger) (*Divider, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}
	return &Divider{base, con}, nil
}

func (w *Divider) Run() error {
	return w.Worker.Run(w)
}

func (w *Divider) Batch(data []byte) bool {
	batch, err := comms.DecodeBatch(data)
	if err != nil {
		w.Log.Fatal("failed to decode line: %v", err)
	}
	responseFieldMaps := make([]map[string]string, 0, len(batch.FieldMaps))

	for _, fieldMap := range batch.FieldMaps {
		responseFieldMap, err := handleDivider(fieldMap)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			continue
		}

		if responseFieldMap != nil {
			responseFieldMaps = append(responseFieldMaps, responseFieldMap)
		}
	}

	if len(responseFieldMaps) > 0 {
		w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
		batch := comms.NewBatch(responseFieldMaps)
		if err := w.PublishBatch(batch); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}

	return false
}

func handleDivider(fieldMap map[string]string) (map[string]string, error) {
	revenueStr, ok := fieldMap["revenue"]
	if !ok {
		return nil, fmt.Errorf("missing revenue field")
	}

	budgetStr, ok := fieldMap["budget"]
	if !ok {
		return nil, fmt.Errorf("missing budget field")
	}

	revenue, err := strconv.Atoi(revenueStr)
	if err != nil {
		return nil, fmt.Errorf("failed to convert revenue to int: %v", err)
	}

	budget, err := strconv.Atoi(budgetStr)
	if err != nil {
		return nil, fmt.Errorf("failed to convert budget to int: %v", err)
	}

	if revenue == 0 || budget == 0 {
		return nil, nil
	}

	rate_revenue_budget := float64(revenue) / float64(budget)
	fieldMap["rate_revenue_budget"] = strconv.FormatFloat(rate_revenue_budget, 'f', 4, 64)
	return fieldMap, nil
}

func (w *Divider) Eof(data []byte) bool {
	body := comms.DecodeEof(data)
	if err := w.PublishEof(body); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
	return true
}

func (w *Divider) Error(data []byte) bool {
	w.Log.Error("Received an ERROR message kind")
	return true
}
