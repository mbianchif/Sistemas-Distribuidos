package impl

import (
	"fmt"
	"strconv"
	"strings"

	"workers"
	"workers/filter/config"
	"workers/protocol"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Filter struct {
	*workers.Worker
	Con     *config.FilterConfig
	Handler func(*Filter, map[string]string) (map[string]string, error)
}

func New(con *config.FilterConfig, log *logging.Logger) (*Filter, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}

	handler := map[string]func(*Filter, map[string]string) (map[string]string, error){
		"range":    handleRange,
		"contains": handleContains,
		"length":   handleLength,
	}[con.Handler]

	return &Filter{base, con, handler}, nil
}

func (w *Filter) Run() error {
	inputQueue := w.InputQueues[0]
	recvChan, err := w.Broker.Consume(inputQueue, "")
	if err != nil {
		return err
	}

	handlers := map[int]func(*Filter, amqp.Delivery, []byte) bool{
		protocol.BATCH: handleBatch,
		protocol.EOF:   handleEof,
		protocol.ERROR: handleError,
	}

	w.Log.Infof("Running with handler: %v", w.Con.Handler)
	exit := false
	for !exit {
		select {
		case <-w.SigChan:
			exit = true

		case del := <-recvChan:
			kind, data := protocol.ReadDelivery(del)
			exit = handlers[kind](w, del, data)
		}
	}
	return nil
}

func handleBatch(w *Filter, del amqp.Delivery, data []byte) bool {
	batch := protocol.DecodeBatch(data)
	responseFieldMaps := make([]map[string]string, 0, len(batch.FieldMaps))

	for _, fieldMap := range batch.FieldMaps {
		responseFieldMap, err := w.Handler(w, fieldMap)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			del.Nack(false, false)
			continue
		}

		if responseFieldMap != nil {
			responseFieldMaps = append(responseFieldMaps, responseFieldMap)
		}
	}

	if len(responseFieldMaps) > 0 {
		w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
		body := protocol.NewBatch(responseFieldMaps).Encode(w.Con.Select)
		outQKey := w.Con.OutputQueueKeys[0]
		if err := w.Broker.Publish(w.Con.OutputExchangeName, outQKey, body); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}

	del.Ack(false)
	return false
}

func handleEof(w *Filter, del amqp.Delivery, data []byte) bool {
	body := protocol.DecodeEof(data).Encode()
	outQKey := w.Con.OutputQueueKeys[0]
	if err := w.Broker.Publish(w.Con.OutputExchangeName, outQKey, body); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	del.Ack(false)
	return true
}

func handleError(w *Filter, del amqp.Delivery, data []byte) bool {
	w.Log.Error("Received an ERROR message kind")
	return true
}

func handleRange(w *Filter, msg map[string]string) (map[string]string, error) {
	yearRange, err := parseMathRange(w.Con.Value)
	if err != nil {
		return nil, err
	}

	date, ok := msg[w.Con.Key]
	if !ok {
		return nil, fmt.Errorf("key %v is not in message", w.Con.Key)
	}

	year, err := strconv.Atoi(strings.Split(date, "-")[0])
	if err != nil {
		return nil, fmt.Errorf("given year is not a number")
	}

	if !yearRange.Contains(year) {
		return nil, nil
	}

	return msg, nil
}

func handleLength(w *Filter, msg map[string]string) (map[string]string, error) {
	length, err := strconv.Atoi(w.Con.Value)
	if err != nil {
		return nil, fmt.Errorf("given length is not a number")
	}

	values, ok := msg[w.Con.Key]
	if !ok {
		return nil, fmt.Errorf("key %v is not in message", w.Con.Key)
	}

	if strings.Count(values, ",")+1 != length {
		return nil, nil
	}

	return msg, nil
}

func handleContains(w *Filter, msg map[string]string) (map[string]string, error) {
	values, ok := msg[w.Con.Key]
	if !ok {
		return nil, fmt.Errorf("key %v is not in message", w.Con.Key)
	}

	valueSet := make(map[string]struct{})
	for value := range strings.SplitSeq(values, ",") {
		valueSet[value] = struct{}{}
	}

	for key := range strings.SplitSeq(w.Con.Value, ",") {
		if _, ok := valueSet[key]; !ok {
			return nil, nil
		}
	}

	return msg, nil
}
