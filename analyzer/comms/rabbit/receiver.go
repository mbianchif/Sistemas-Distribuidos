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

	eofsFiltered := make(chan amqp.Delivery)
	go func() {
		defer close(eofsFiltered)
		eofsRecv := make(map[int]int)
		copies := r.copies

		for del := range recv {
			kind := int(del.Headers["kind"].(int32))

			if kind == comms.EOF {
				client := int(del.Headers["client-id"].(int32))

				eofsRecv[client] = eofsRecv[client] + 1
				if eofsRecv[client] < copies {
					del.Ack(false)
					continue
				}
				delete(eofsRecv, client)
			}

			eofsFiltered <- del
		}
	}()

	return eofsFiltered, nil

	// ordered := make(chan amqp.Delivery)
	// go func() {
	// 	defer close(ordered)

	// 	copies := r.copies
	// 	expecting := make([]map[int]int, copies)
	// 	for i := range expecting {
	// 		expecting[i] = make(map[int]int)
	// 	}

	// 	bufs := make([]map[int]map[int]amqp.Delivery, copies)
	// 	for i := range bufs {
	// 		bufs[i] = make(map[int]map[int]amqp.Delivery)
	// 	}

	// 	for del := range eofsFiltered {
	// 		replica := int(del.Headers["replica-id"].(int32))
	// 		client := int(del.Headers["client-id"].(int32))
	// 		seq := int(del.Headers["seq"].(int32))

	// 		if _, ok := bufs[replica][client]; !ok {
	// 			bufs[replica][client] = make(map[int]amqp.Delivery)
	// 		}

	// 		if _, ok := expecting[replica][client]; !ok {
	// 			expecting[replica][client] = 0
	// 		}

	// 		if expecting[replica][client] <= seq {
	// 			bufs[replica][client][seq] = del
	// 		} else {
	// 			del.Ack(false)
	// 		}

	// 		for {
	// 			next, ok := bufs[replica][client][expecting[replica][client]]
	// 			if !ok {
	// 				break
	// 			}

	// 			delete(bufs[replica][client], expecting[replica][client])
	// 			expecting[replica][client]++
	// 			ordered <- next

	// 			kind := int(next.Headers["kind"].(int32))
	// 			if kind == comms.EOF {
	// 				delete(bufs[replica], client)
	// 				delete(expecting[replica], client)
	// 			}
	// 		}
	// 	}
	// }()

	// return ordered, nil
}
