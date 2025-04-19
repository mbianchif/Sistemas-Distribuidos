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
}

func New(con *config.DividerConfig) (*Divider, error) {
	base, err := workers.New(con.Config)
	if err != nil {
		return nil, err
	}
	return &Divider{base}, nil
}

func (w *Divider) Run(con *config.DividerConfig, log *logging.Logger) error {
	inputQueue := w.InputQueues[0]
	recvChan, err := w.Broker.Consume(inputQueue, "")
	if err != nil {
		return err
	}

	log.Infof("Running")
	for msg := range recvChan {
		fieldMap, err := protocol.Decode(msg.Body)
		if err != nil {
			log.Errorf("failed to decode message: %v", err)
			msg.Nack(false, false)
			continue
		}

		responseFieldMap, err := handleRateBudget(fieldMap)
		if err != nil {
			log.Errorf("failed to handle message: %v", err)
			msg.Nack(false, false)
			continue
		}

		if responseFieldMap != nil {
			body := protocol.Encode(responseFieldMap, con.Select)
			outQKey := con.OutputQueueKeys[0]
			if err := w.Broker.Publish(con.OutputExchangeName, outQKey, body); err != nil {
				log.Errorf("failed to publish message: %v", err)
			}
		}

		msg.Ack(false)
	}

	log.Info("Recv channel was closed")
	return nil
}

func handleRateBudget(msg map[string]string) (map[string]string, error) {
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
	if revenue == 0 {
		return nil, fmt.Errorf("revenue is 0")
	}

	budget, err := strconv.Atoi(budgetStr)
	if err != nil {
		return nil, fmt.Errorf("failed to convert budget to int: %v", err)
	}
	if budget == 0 {
		return nil, fmt.Errorf("budget is 0")
	}

	rate_revenue_budget := revenue / budget
	msg["rate_revenue_budget"] = strconv.Itoa(rate_revenue_budget)

	return msg, nil
}
