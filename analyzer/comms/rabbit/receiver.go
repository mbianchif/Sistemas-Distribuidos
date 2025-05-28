package rabbit

import (
	"analyzer/comms"
	"bytes"
	"fmt"
	"strconv"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Receiver struct {
	broker    *Broker
	q         amqp.Queue
	copies    int
	mailer    comms.Dumpable
	expecting []map[int]int
	eofs      map[int]int
}

func NewReceiver(broker *Broker, q amqp.Queue, copies int, mailer comms.Dumpable) *Receiver {
	expecting := make([]map[int]int, copies)
	for i := range expecting {
		expecting[i] = make(map[int]int)
	}

	eofs := make(map[int]int)
	return &Receiver{broker, q, copies, mailer, expecting, eofs}
}

func (r *Receiver) Consume(consumer string) (<-chan comms.Delivery, error) {
	recv, err := r.broker.Consume(r.q, consumer)
	if err != nil {
		return nil, fmt.Errorf("couldn't start consuming through receiver: %v", err)
	}

	ordered := make(chan comms.Delivery)
	go func() {
		defer close(ordered)
		wg := new(sync.WaitGroup)

		for del := range recv {
			// Stop here until the worker has Ack'd it's last delivery
			wg.Wait()
			wg.Add(1)

			replica := int(del.Headers["replica-id"].(int32))
			clientId := int(del.Headers["client-id"].(int32))
			seq := int(del.Headers["seq"].(int32))
			kind := int(del.Headers["kind"].(int32))

			if _, ok := r.expecting[replica][clientId]; !ok {
				r.expecting[replica][clientId] = 0
			}

			if kind == comms.FLUSH {
				delete(r.expecting[replica], clientId)
			}

			if seq < r.expecting[replica][clientId] {
				del.Ack(false)
				continue
			}

			r.expecting[replica][clientId] = seq + 1
			ordered <- comms.NewDelivery(del, wg)
		}
	}()

	eofsCounted := make(chan comms.Delivery)
	go func() {
		defer close(eofsCounted)
		copies := r.copies

		for del := range ordered {
			kind := int(del.Headers["kind"].(int32))
			clientId := int(del.Headers["client-id"].(int32))

			if kind == comms.EOF {
				r.eofs[clientId]++
				if r.eofs[clientId] < copies {
					r.mailer.Dump(clientId)
					del.Ack(false)
					continue
				}
			} else if kind == comms.FLUSH {
				delete(r.eofs, clientId)
			}

			eofsCounted <- del
		}
	}()

	return eofsCounted, nil
}

func (r *Receiver) Encode(clientId int) []byte {
	init := fmt.Appendf(nil, "recv %s %d", r.q.Name, r.eofs[clientId])
	builder := bytes.NewBuffer(init)

	for replicaId := range r.expecting {
		seq := r.expecting[replicaId][clientId]
		builder.WriteRune(' ')
		builder.WriteString(strconv.Itoa(seq))
	}

	return builder.Bytes()
}
