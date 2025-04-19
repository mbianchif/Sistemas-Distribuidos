package impl

import (
	"fmt"

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

	handlers := map[string]func(*Filter, map[string]string) (map[string]string, error){
		"range":    handleRange,
		"contains": handleContains,
		"length":   hanldeLength,
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
		responseFieldMap, err := handlers[con.FilterType](w, decodedMsg)
		if err != nil {
			fmt.Println(err)
			continue
		}

		body := protocol.Encode(responseFieldMap, con.Select)
		outQKey := con.OutputQueueKeys[0] // fanout
		if err := w.Broker.Publish(con.OutputExchangeName, outQKey, body); err != nil {
			// log
		}
		msg.Ack(false)
	}

	return nil
}

func hasEmptyValues(fields map[string]string) bool {
	for _, value := range fields {
		if value == "" {
			return true
		}
	}
	return false
}

func handleRange(w *Filter, msg map[string]string) (map[string]string, error) {
	fmt.Println("msg: ", msg)
	return nil, nil
}

func hanldeLength(w *Filter, msg map[string]string) (map[string]string, error) {
	return nil, nil
}

func handleContains(w *Filter, msg map[string]string) (map[string]string, error) {
	return nil, nil
}
