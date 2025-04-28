package rabbit

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Receiver struct {
	broker   *Broker
	q        amqp.Queue
}

func NewReceiver(broker *Broker, q amqp.Queue) *Receiver {
	return &Receiver{broker, q}
}

func (r *Receiver) Consume(consumer string) (<-chan amqp.Delivery, error) {
	recv, err := r.broker.Consume(r.q, consumer)
	if err != nil {
		return nil, fmt.Errorf("couldn't start consuming through receiver: %v", err)
	}
	ordered := make(chan amqp.Delivery)

	go func() {
		expecting := 0
		buf := make(map[int]amqp.Delivery)
		defer close(ordered)

		for del := range recv {
			seq := int(del.Headers["seq"].(int32))
			buf[seq] = del

			for {
				ord, ok := buf[expecting]
				if !ok {
					break
				}

				ordered <- ord
				delete(buf, expecting)
				expecting++
			}
		}
	}()

	return ordered, nil
}
