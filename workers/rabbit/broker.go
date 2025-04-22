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

func (b *Broker) Init() ([]amqp.Queue, error) {
	inputQueues, err := b.declareInput()
	if err != nil {
		return nil, err
	}
	if err := b.declareOutput(); err != nil {
		return nil, err
	}
	return inputQueues, nil
}

func (b *Broker) DeInit() {
	if !b.conn.IsClosed() {
		b.conn.Close()
	}
	if !b.ch.IsClosed() {
		b.ch.Close()
	}
}

func (b *Broker) declareInput() ([]amqp.Queue, error) {
	exchangeNames := b.con.InputExchangeNames
	exchangeTypes := b.con.InputExchangeTypes
	qNames := b.con.InputQueueNames
	qKeys := b.con.InputQueueKeys

	if len(exchangeNames) != len(exchangeTypes) {
		return nil, fmt.Errorf("config failed to check len(InputExchangeNames) == len(InputExchangeTypes)")
	}

	if len(qNames) != len(qKeys) {
		return nil, fmt.Errorf("config failed to check len(InputQueueNames) == len(InputQueueKeys)")
	}

	if len(exchangeNames) != len(qNames) {
		return nil, fmt.Errorf("config failed to check len(InputExchangeNames) == len(InputQueueNames)")
	}

	inputQueues := make([]amqp.Queue, 0, len(qNames))
	for i := range exchangeNames {
		if err := b.exchangeDeclare(exchangeNames[i], exchangeTypes[i]); err != nil {
			return nil, err
		}

		q, err := b.queueDeclare(qNames[i])
		if err != nil {
			return nil, err
		}

		if err := b.queueBind(q, qKeys[i], exchangeNames[i]); err != nil {
			return nil, err
		}

		inputQueues = append(inputQueues, q)
	}

	return inputQueues, nil
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

func (b *Broker) Publish(key string, body []byte) error {
	return b.ch.Publish(
		b.con.OutputExchangeName,
		key,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/octet-stream",
			Body:        body,
		})
}

func (b *Broker) PublishWithHeaders(key string, body []byte, headers amqp.Table) error {
	return b.ch.Publish(
		b.con.OutputExchangeName,
		key,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/octet-stream",
			Body:        body,
			Headers:     headers,
		})
}
