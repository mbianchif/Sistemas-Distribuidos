package middleware

import (
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Table = amqp.Table

type DelId struct {
	ReplicaId int
	Seq       int
	ClientId  int
}

type Headers struct {
	ReplicaId int
	ClientId  int
	Seq       int
	Query     int
	Kind      int
}

type Delivery struct {
	Headers Headers
	Body    []byte
	del     amqp.Delivery
	wg      *sync.WaitGroup
}

func NewDelivery(del amqp.Delivery, wg *sync.WaitGroup) Delivery {
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
		wg:      wg,
	}
}

func (d Delivery) Id() DelId {
	return DelId{
		ReplicaId: d.Headers.ReplicaId,
		Seq:       d.Headers.Seq,
		ClientId:  d.Headers.ClientId,
	}
}

func (d Delivery) Ack(multiple bool) error {
	// aca hay que asegurarse de que el ack le llega a rabbit, despues correr d.acker.Done()
	d.wg.Done()
	return d.del.Ack(multiple)
}
