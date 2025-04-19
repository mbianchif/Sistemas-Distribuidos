package impl

import (
	"fmt"
	"strconv"
	"strings"
	"workers"
	"workers/filter/config"
	"workers/protocol"
)

type Filter struct {
	*workers.Worker
}

func New(con *config.FilterConfig) (*Filter, error) {
	base, err := workers.New(con.Config)
	if err != nil {
		return nil, err
	}
	return &Filter{base}, nil
}

func (w *Filter) Run(con *config.FilterConfig) error {
	inputQueue := w.InputQueues[0]

	recvChan, err := w.Broker.Consume(inputQueue, "")
	if err != nil {
		return err
	}

	handlers := map[string]func(*Filter, map[string]string, string, string) (map[string]string, error){
		"range":    handleRange,
		"contains": handleContains,
		"length":   handleLength,
	}

	for msg := range recvChan {
		if len(msg.Body) == 0 {
			fmt.Println("Empty body received, rejecting message")
			msg.Nack(false, false)
			continue
		}

		decodedMsg, err := protocol.Decode(msg.Body)
		if err != nil {
			fmt.Println("Failed tod decode msg: ", err)
			continue
		}
		responseFieldMap, err := handlers[con.FilterType](w, decodedMsg, con.FilterKey, con.FilterValue)
		if err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Println("responseFieldMap: ", responseFieldMap)
		body := protocol.Encode(responseFieldMap, con.Select)
		outQKey := con.OutputQueueKeys[0] // fanout
		if err := w.Broker.Publish(con.OutputExchangeName, outQKey, body); err != nil {
			// log
		}
		msg.Ack(false)
	}

	return nil
}

func handleRange(w *Filter, msg map[string]string, filterColumn string, filterValue string) (map[string]string, error) {
	rang, err := parseMathRange(filterValue)
	if err != nil {
		return nil, err
	}

	for k, v := range msg {
		if k == filterColumn {
			year, err := strconv.Atoi(strings.Split(v, "-")[0])
			if err != nil {
				return nil, fmt.Errorf("invalid year format")
			}
			if !rang.Contains(year) {
				return nil, fmt.Errorf("value %d is not in range", year)
			} else {
				return msg, nil
			}
		}
	}

	return nil, fmt.Errorf("column %s not found in message", filterColumn)
}

func handleLength(w *Filter, msg map[string]string, filterColumn string, filterValue string) (map[string]string, error) {
	return nil, nil
}

func handleContains(w *Filter, msg map[string]string, filterColumn string, filterValue string) (map[string]string, error) {
	return nil, nil
}
