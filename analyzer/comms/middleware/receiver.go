package middleware

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"analyzer/comms"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Receiver struct {
	broker *Broker
	mailer Dumpable
	mu     *sync.Mutex

	// Persisted
	qName     string
	copies    int
	expecting []map[int]int
	eofs      map[int]int
	flushes   map[int]int
}

func NewReceiver(broker *Broker, q amqp.Queue, copies int, mailer Dumpable, mu *sync.Mutex) *Receiver {
	expecting := make([]map[int]int, copies)
	for i := range expecting {
		expecting[i] = make(map[int]int)
	}

	return &Receiver{
		broker:    broker,
		copies:    copies,
		mailer:    mailer,
		mu:        mu,
		qName:     q.Name,
		expecting: expecting,
		eofs:      make(map[int]int),
		flushes:   make(map[int]int),
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
		copies := r.copies

		bufs := make([]map[int]map[int]amqp.Delivery, copies)
		for i := range bufs {
			bufs[i] = make(map[int]map[int]amqp.Delivery)
		}

		for del := range recv {
			replicaId := int(del.Headers["replica-id"].(int32))
			clientId := int(del.Headers["client-id"].(int32))
			seq := int(del.Headers["seq"].(int32))

			if _, ok := bufs[replicaId][clientId]; !ok {
				bufs[replicaId][clientId] = make(map[int]amqp.Delivery)
			}

			if seq < r.expecting[replicaId][clientId] {
				del.Ack(false)
				continue
			}

			bufs[replicaId][clientId][seq] = del
			for {
				expected := r.expecting[replicaId][clientId]
				next, ok := bufs[replicaId][clientId][expected]
				if !ok {
					break
				}

				delete(bufs[replicaId][clientId], expected)
				kind := int(next.Headers["kind"].(int32))

				// Stop here until worker has Ack'd it's last delivery
				// so not to change `expecting` table before it got the
				// chance to persist it's state.
				r.mu.Lock()

				if kind == comms.FLUSH {
					delete(r.expecting[replicaId], clientId)
				} else if kind == comms.PURGE {
					for i := range r.expecting {
						r.expecting[i] = make(map[int]int)
					}
				} else {
					r.expecting[replicaId][clientId]++
				}

				ordered <- NewDelivery(next, r.mu)
			}
		}
	}()

	counted := make(chan Delivery)
	go func() {
		defer close(counted)
		copies := r.copies

		for del := range ordered {
			kind := del.Headers.Kind
			clientId := del.Headers.ClientId

			if kind == comms.EOF {
				r.eofs[clientId]++
				if r.eofs[clientId] < copies {
					r.mailer.Dump(clientId)
					del.Ack(false)
					continue
				}
			} else if kind == comms.FLUSH {
				r.flushes[clientId]++
				if r.flushes[clientId] < copies {
					r.mailer.Dump(clientId)
					del.Ack(false)
					continue
				}
				delete(r.eofs, clientId)
				delete(r.flushes, clientId)
			}

			counted <- del
		}
	}()

	return counted, nil
}

// Example: "recv <qName> <eofs> <flushes> <seq> ... <seq>"
func (r *Receiver) Encode(clientId int) []byte {
	init := fmt.Appendf(nil, "recv %s %d %d", r.qName, r.eofs[clientId], r.flushes[clientId])
	builder := bytes.NewBuffer(init)

	for replicaId := range r.expecting {
		seq := r.expecting[replicaId][clientId]
		builder.WriteRune(' ')
		builder.WriteString(strconv.Itoa(seq))
	}

	return builder.Bytes()
}

// Example: "recv <qName> <eofs> <flushes> <seq> ... <seq>"
func DecodeLineRecv(line string) (string, int, int, []int, error) {
	line, _ = strings.CutPrefix(line, "recv ")

	parts := strings.Split(line, " ")
	if len(parts) < 5 {
		return "", 0, 0, nil, fmt.Errorf("the amount of parts is not enough: %s", line)
	}

	// Read queue name
	qName := parts[0]

	// Read eof count
	eofs, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, 0, nil, fmt.Errorf("Eof count is not a number: %s", line)
	}

	// Read flush count
	flushes, err := strconv.Atoi(parts[2])
	if err != nil {
		return "", 0, 0, nil, fmt.Errorf("Flush count is not a number: %s", line)
	}

	// Read sequence numbers
	seqs := make([]int, 0, len(parts)-3)
	for _, seqStr := range parts[3:] {
		seq, err := strconv.Atoi(seqStr)
		if err != nil {
			return "", 0, 0, nil, fmt.Errorf("seq number is not a number: %s", line)
		}
		seqs = append(seqs, seq)
	}

	return qName, eofs, flushes, seqs, nil
}

func (r *Receiver) SetState(clientId, eofs, flushes int, seqs []int) error {
	if len(seqs) != r.copies {
		return fmt.Errorf("expected %d sequence numbers, got %d for client %d in receiver %s", r.copies, len(seqs), clientId, r.qName)
	}

	r.eofs[clientId] = eofs
	r.flushes[clientId] = flushes

	for replicaId, seq := range seqs {
		r.expecting[replicaId][clientId] = seq
	}

	return nil
}
