package rabbit

import (
	"analyzer/comms"
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

	copies := r.copies
	eofsRecv := make(map[int]int)
	expecting := make([]map[int]int, copies)
	for i := range expecting {
		expecting[i] = make(map[int]int)
	}

	bufs := make([]map[int]map[int]amqp.Delivery, copies)
	for i := range bufs {
		bufs[i] = make(map[int]map[int]amqp.Delivery)
	}

	go func() {
		defer close(ordered)

		for del := range recv {
			replica := int(del.Headers["replica-id"].(int32))
			client := int(del.Headers["client-id"].(int32))
			seq := int(del.Headers["seq"].(int32))

			if _, ok := bufs[replica][client]; !ok {
				bufs[replica][client] = make(map[int]amqp.Delivery)
			}
			bufs[replica][client][seq] = del

			for {
				next, ok := bufs[replica][client][expecting[replica][client]]
				if !ok {
					break
				}

				delete(bufs[replica][client], expecting[replica][client])
				expecting[replica][client]++

				kind := int(next.Headers["kind"].(int32))
				if kind == comms.EOF {
					eofsRecv[client] += 1
					if eofsRecv[client] < copies {
						continue
					}
					delete(eofsRecv, client)
				}

				ordered <- next
			}
		}
	}()

	return ordered, nil
}
