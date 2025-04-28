package rabbit

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Receiver struct {
	broker *Broker
	q      amqp.Queue
	copies int
}

func NewReceiver(broker *Broker, q amqp.Queue, copies int) *Receiver {
	return &Receiver{broker, q, copies}
}

func (r *Receiver) Consume(consumer string) (<-chan amqp.Delivery, error) {
	recv, err := r.broker.Consume(r.q, consumer)
	if err != nil {
		return nil, fmt.Errorf("couldn't start consuming through receiver: %v", err)
	}
	ordered := make(chan amqp.Delivery)

	go func() {
		defer close(ordered)
		expecting := make([]int, r.copies)
		bufs := make([]map[int]amqp.Delivery, 0)
		for range r.copies {
			bufs = append(bufs, make(map[int]amqp.Delivery))
		}

		for del := range recv {
			id := int(del.Headers["id"].(int32))
			seq := int(del.Headers["seq"].(int32))
			bufs[id][seq] = del

			for {
				ord, ok := bufs[id][expecting[id]]
				if !ok {
					break
				}

				ordered <- ord
				delete(bufs[id], expecting[id])
				expecting[id]++
			}
		}
	}()

	return ordered, nil
}
