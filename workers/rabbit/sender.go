package rabbit

import (
	"workers/protocol"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Sender interface {
	Batch(protocol.Batch, map[string]struct{}) error
	Broadcast([]byte, amqp.Table) error
}
