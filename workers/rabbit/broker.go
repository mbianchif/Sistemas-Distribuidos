package rabbit

import (
	"workers/config"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Broker struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func New(addr string) (*Broker, error) {
	conn, err := amqp.Dial(addr)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &Broker{conn, ch}, nil
}

func (b *Broker) Init(con *config.Config) ([]amqp.Queue, []amqp.Queue, error) {
	inputQueues, err := b.declareSide(con.InputExchangeName, con.InputExchangeType, con.InputQueues, con.InputQueueKeys)
	if err != nil {
		return nil, nil, err
	}

	outputQueues, err := b.declareSide(con.OutputExchangeName, con.OutputExchangeType, con.OutputQueues, con.OutputQueueKeys)
	if err != nil {
		return nil, nil, err
	}

	return inputQueues, outputQueues, nil
}

func (b *Broker) DeInit() {
	b.conn.Close()
	b.ch.Close()
}

func (b *Broker) declareSide(exchangeName string, exchangeType string, qNames []string, qKeys []string) ([]amqp.Queue, error) {
	qs := make([]amqp.Queue, 0)
	if err := b.exchangeDeclare(exchangeName, exchangeType); err != nil {
		return qs, err
	}

	for i, name := range qNames {
		q, err := b.queueDeclare(name)
		if err != nil {
			return qs, err
		}

		if err := b.queueBind(q, qKeys[i], exchangeName); err != nil {
			return qs, err
		}

		qs = append(qs, q)
	}

	return qs, nil
}

func (b *Broker) exchangeDeclare(name string, kind string) error {
	return b.ch.ExchangeDeclare(
		name,
		kind,
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
}

func (b *Broker) queueDeclare(name string) (amqp.Queue, error) {
	return b.ch.QueueDeclare(
		name,
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
}

func (b *Broker) queueBind(q amqp.Queue, key string, exchangeName string) error {
	return b.ch.QueueBind(
		q.Name,
		key,
		exchangeName,
		false, // no-wait
		nil,   // args
	)
}

func (b *Broker) Consume(q amqp.Queue, consumer string) (<-chan amqp.Delivery, error) {
	return b.ch.Consume(
		q.Name,
		consumer,
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
}

func (b *Broker) Publish(exchangeName string, key string, body []byte) error {
	return b.ch.Publish(
		exchangeName,
		key,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/octet-stream",
			Body:        body,
		})
}
