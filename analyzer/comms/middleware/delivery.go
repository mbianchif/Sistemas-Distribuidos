package middleware

import (
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Delivery struct {
	Headers amqp.Table
	Body    []byte
	del     amqp.Delivery
	acker   *sync.WaitGroup
}

func NewDelivery(del amqp.Delivery, acker *sync.WaitGroup) Delivery {
	return Delivery{
		Headers: del.Headers,
		Body:    del.Body,
		del:     del,
		acker:   acker,
	}
}

func (d Delivery) Ack(multiple bool) error {
	// aca hay que asegurarse de que el ack le llega a rabbit, despues correr d.acker.Done()
	d.acker.Done()
	return d.del.Ack(multiple)
}
