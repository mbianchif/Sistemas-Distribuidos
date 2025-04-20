package impl

import (
	"fmt"
	"strconv"

	"workers"
	"workers/divider/config"
	"workers/protocol"

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
	inputQueue := w.InputQueues[0]
	recvChan, err := w.Broker.Consume(inputQueue, "")
	if err != nil {
		return err
	}

	handlers := map[int]func(*Divider, []byte) bool{
		protocol.BATCH: handleBatch,
		protocol.EOF:   handleEof,
		protocol.ERROR: handleError,
	}

	w.Log.Infof("Running")
	exit := false
	for !exit {
		select {
		case <-w.SigChan:
			exit = true

		case del := <-recvChan:
			kind, data := protocol.ReadDelivery(del)
			exit = handlers[kind](w, data)
			del.Ack(false)
		}
	}

	return nil
}

func handleBatch(w *Divider, data []byte) bool {
	batch := protocol.DecodeBatch(data)
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
		body := protocol.NewBatch(responseFieldMaps).Encode(w.Con.Select)
		if err := w.Broker.Publish(w.Con.OutputExchangeName, "", body); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}

	return false
}

func handleDivider(msg map[string]string) (map[string]string, error) {
	revenueStr, ok := msg["revenue"]
	if !ok {
		return nil, fmt.Errorf("missing revenue field")
	}

	budgetStr, ok := msg["budget"]
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
	msg["rate_revenue_budget"] = strconv.FormatFloat(rate_revenue_budget, 'f', 4, 32)

	return msg, nil
}

func handleEof(w *Divider, data []byte) bool {
	body := protocol.DecodeEof(data).Encode()
	if err := w.Broker.Publish(w.Con.OutputExchangeName, "", body); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	return true
}

func handleError(w *Divider, data []byte) bool {
	w.Log.Error("Received an ERROR message kind")
	return true
}
