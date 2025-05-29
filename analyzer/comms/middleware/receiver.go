package middleware

import (
	"analyzer/comms"
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Receiver struct {
	broker *Broker
	mailer Dumpable

	// Persisted
	qName     string
	copies    int
	expecting []map[int]int
	eofs      map[int]int
}

func NewReceiver(broker *Broker, q amqp.Queue, copies int, mailer Dumpable) *Receiver {
	expecting := make([]map[int]int, copies)
	for i := range expecting {
		expecting[i] = make(map[int]int)
	}

	eofs := make(map[int]int)
	return &Receiver{
		broker:    broker,
		qName:     q.Name,
		copies:    copies,
		mailer:    mailer,
		expecting: expecting,
		eofs:      eofs,
	}
}

func (r *Receiver) Consume(consumer string) (<-chan Delivery, error) {
	recv, err := r.broker.Consume(r.qName, consumer)
	if err != nil {
		return nil, fmt.Errorf("couldn't start consuming through receiver: %v", err)
	}

	ordered := make(chan Delivery)
	go func() {
		defer close(ordered)
		wg := new(sync.WaitGroup)

		for del := range recv {
			// Stop here until worker has Ack'd it's last delivery
			// so not to change `expecting` table before it got the
			// chance to persist it's state.
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
			ordered <- NewDelivery(del, wg)
		}
	}()

	eofsCounted := make(chan Delivery)
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

// Example: "recv <qName> <eofs> <seq> ... <seq>"
func (r *Receiver) Encode(clientId int) []byte {
	init := fmt.Appendf(nil, "recv %s %d", r.qName, r.eofs[clientId])
	builder := bytes.NewBuffer(init)

	for replicaId := range r.expecting {
		seq := r.expecting[replicaId][clientId]
		builder.WriteRune(' ')
		builder.WriteString(strconv.Itoa(seq))
	}

	return builder.Bytes()
}

// Example: "recv <qName> <eofs> <seq> ... <seq>"
func DecodeLineRecv(line string) (string, int, []int, error) {
	line, found := strings.CutPrefix(line, "recv ")
	if !found {
		return "", 0, nil, fmt.Errorf("Invalid recv line: %s", line)
	}

	parts := strings.Split(line, " ")
	if len(parts) < 3 {
		return "", 0, nil, fmt.Errorf("The amount of parts is not enough: %s", line)
	}

	// Read queue name
	qName := parts[0]

	// Read eof count
	eofs, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, nil, fmt.Errorf("Eof count is not a number: %s", line)
	}

	// Read sequence numbers
	seqs := make([]int, 0, len(parts)-2)
	for _, seqStr := range parts[2:] {
		seq, err := strconv.Atoi(seqStr)
		if err != nil {
			return "", 0, nil, fmt.Errorf("Seq number is not a number: %s", line)
		}
		seqs = append(seqs, seq)
	}

	return qName, eofs, seqs, nil
}
