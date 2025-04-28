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

	// co-routing will always finish first than main thread
	// main thread should be always listening to returned channel
	go func() {
		expecting := 0
		buf := make(map[int]amqp.Delivery)
		defer close(ordered)

		for del := range recv {
			seq := int(del.Headers["seq"].(int32))

			kind := del.Headers["kind"].(int32)
			if query, ok := del.Headers["query"]; ok {
				fmt.Printf("query: %d, seq: %d, kind: %d\n", query, seq, kind)
			} else {
				fmt.Printf("seq: %d, kind: %d\n", seq, kind)
			}

			if _, ok := buf[seq]; !ok && seq >= expecting {
				buf[seq] = del
			}

			for {
				ord, ok := buf[expecting]
				if !ok {
					break
				}

				ordered <- ord
				delete(buf, expecting)
				expecting += 1
			}
		}
	}()

	return ordered, nil
}
