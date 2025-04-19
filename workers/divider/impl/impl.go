package impl

import (
	"fmt"

	"workers"
	"workers/divider/config"
	"workers/protocol"
)

type Divider struct {
	*workers.Worker
}

func New(con *config.DividerConfig) (*Divider, error) {
	base, err := workers.New(con.Config)
	if err != nil {
		return nil, err
	}
	return &Divider{base}, nil
}

func (w *Divider) Run(con *config.DividerConfig) error {
	inputQueue := w.InputQueues[0]
	recvChan, err := w.Broker.Consume(inputQueue, "")
	if err != nil {
		return err
	}

	handlers := map[string]func(*Divider, map[string]string) (map[string]string, error){
		"revenue_budget": handleRateBudget,
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
		responseFieldMap, err := handlers[con.Handler](w, decodedMsg)
		if err != nil {
			fmt.Println(err)
			continue
		}

		body := protocol.Encode(responseFieldMap, con.Select)
		outQKey := con.OutputQueueKeys[0] // direct
		if err := w.Broker.Publish(con.OutputExchangeName, outQKey, body); err != nil {
			// log
		}
	}

	return nil
}

func handleRateBudget(w *Divider, msg map[string]string) (map[string]string, error) {
	fmt.Println("msg: ", msg)
	return nil, nil
}
