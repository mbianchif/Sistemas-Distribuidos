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
	mu     *sync.Mutex

	// Persisted
	qName     string
	copies    int
	expecting []map[int]int
	eofs      map[int]int
}

func NewReceiver(broker *Broker, q amqp.Queue, copies int, mailer Dumpable, mu *sync.Mutex) *Receiver {
	expecting := make([]map[int]int, copies)
	for i := range expecting {
		expecting[i] = make(map[int]int)
	}

	eofs := make(map[int]int)
	return &Receiver{
		broker:    broker,
		copies:    copies,
		mailer:    mailer,
		mu:        mu,
		qName:     q.Name,
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

		for rabbitDel := range recv {
			// Stop here until worker has Ack'd it's last delivery
			// so not to change `expecting` table before it got the
			// chance to persist it's state.
			r.mu.Lock()

			del := NewDelivery(rabbitDel, r.mu)
			replicaId := del.Headers.ReplicaId
			clientId := del.Headers.ClientId
			seq := del.Headers.Seq
			kind := del.Headers.Kind

			if _, ok := r.expecting[replicaId][clientId]; !ok {
				r.expecting[replicaId][clientId] = seq
			}

			if kind == comms.FLUSH {
				delete(r.expecting[replicaId], clientId)
			}

			if seq < r.expecting[replicaId][clientId] {
				del.Ack(false)
				continue
			}

			r.expecting[replicaId][clientId] = seq + 1
			ordered <- del
		}
	}()

	eofsCounted := make(chan Delivery)
	go func() {
		defer close(eofsCounted)
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
	line, _ = strings.CutPrefix(line, "recv ")

	parts := strings.Split(line, " ")
	if len(parts) < 3 {
		return "", 0, nil, fmt.Errorf("the amount of parts is not enough: %s", line)
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
			return "", 0, nil, fmt.Errorf("seq number is not a number: %s", line)
		}
		seqs = append(seqs, seq)
	}

	return qName, eofs, seqs, nil
}

func (r *Receiver) SetState(clientId int, eofs int, seqs []int) error {
	if len(seqs) != r.copies {
		return fmt.Errorf("expected %d sequence numbers, got %d for client %d in receiver %s", r.copies, len(seqs), clientId, r.qName)
	}

	r.eofs[clientId] = eofs

	for replicaId, seq := range seqs {
		r.expecting[replicaId][clientId] = seq
	}

	return nil
}
