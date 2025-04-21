package rabbit

import (
	"fmt"
	"workers/config"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Broker struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	con  *config.Config
}

func New(con *config.Config) (*Broker, error) {
	conn, err := amqp.Dial(con.Url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &Broker{conn, ch, con}, nil
}

func (b *Broker) Init() error {
	if err := b.declareInput(); err != nil {
		return err
	}
	if err := b.declareOutput(); err != nil {
		return err
	}
	return nil
}

func (b *Broker) DeInit() {
	if !b.conn.IsClosed() {
		b.conn.Close()
	}
	if !b.ch.IsClosed() {
		b.ch.Close()
	}
}

func (b *Broker) declareInput() error {
	exchangeNames := b.con.InputExchangeNames
	exchangeTypes := b.con.InputExchangeTypes
	qName := b.con.InputQueueName
	qKey := b.con.InputQueueKey

	if len(exchangeNames) != len(exchangeTypes) {
		return fmt.Errorf("config failed to check len(InputExchangeNames) == len(InputExchangeTypes)")
	}

	q, err := b.queueDeclare(qName)
	if err != nil {
		return err
	}

	for i := range exchangeNames {
		if err := b.exchangeDeclare(exchangeNames[i], exchangeTypes[i]); err != nil {
			return err
		}

		if err := b.queueBind(q, qKey, exchangeNames[i]); err != nil {
			return err
		}
	}

	return nil
}

func (b *Broker) declareOutput() error {
	exchangeName := b.con.OutputExchangeName
	exchangeType := b.con.OutputExchangeType
	qNames := b.con.OutputQueueNames
	qKeys := b.con.OutputQueueKeys

	if len(qNames) != len(qKeys) {
		return fmt.Errorf("config failed to check len(OutputQueueNames) == len(OutputQueueKeys)")
	}

	if err := b.exchangeDeclare(exchangeName, exchangeType); err != nil {
		return err
	}

	for i := range qNames {
		q, err := b.queueDeclare(qNames[i])
		if err != nil {
			return err
		}

		if err := b.queueBind(q, qKeys[i], exchangeName); err != nil {
			return err
		}
	}

	return nil
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

func (b *Broker) Consume(consumer string) (<-chan amqp.Delivery, error) {
	return b.ch.Consume(
		b.con.InputQueueName,
		consumer,
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
}

func (b *Broker) Publish(key string, body []byte) error {
	return b.ch.Publish(
		b.con.OutputExchangeName,
		key,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/octet-stream",
			Body:        body,
			Headers: amqp.Table {
				"producer": b.con.Producer,
			},
		})
}
