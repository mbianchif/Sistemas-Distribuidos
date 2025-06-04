package middleware

import (
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Table = amqp.Table

// Id to unequivocally identify a delivery
type DelId struct {
	ReplicaId int
	Seq       int
	ClientId  int
}

// The headers of all deliveries
type Headers struct {
	ReplicaId int
	ClientId  int
	Seq       int
	Query     int
	Kind      int
}

// Middleware delivery imlpementation
type Delivery struct {
	Headers Headers
	Body    []byte
	del     amqp.Delivery
	mu      *sync.Mutex
}

// Creates a new delivery, the mutex will be unlocked when the delivery is acked
func NewDelivery(del amqp.Delivery, mu *sync.Mutex) Delivery {
	headers := Headers{
		ReplicaId: int(del.Headers["replica-id"].(int32)),
		ClientId:  int(del.Headers["client-id"].(int32)),
		Seq:       int(del.Headers["seq"].(int32)),
		Kind:      int(del.Headers["kind"].(int32)),
	}

	if query, ok := del.Headers["query"]; ok {
		headers.Query = int(query.(int32))
	} else {
		headers.Query = -1
	}

	return Delivery{
		Headers: headers,
		Body:    del.Body,
		del:     del,
		mu:      mu,
	}
}

// Returns the id that corresponds with this delivery
func (d Delivery) Id() DelId {
	return DelId{
		ReplicaId: d.Headers.ReplicaId,
		Seq:       d.Headers.Seq,
		ClientId:  d.Headers.ClientId,
	}
}

// Sends an ack to the delivery's sender, waits till confirmation
func (d Delivery) Ack(multiple bool) error {
	if err := d.del.Ack(multiple); err != nil {
		return err
	}

	d.mu.Unlock()
	return nil
}
