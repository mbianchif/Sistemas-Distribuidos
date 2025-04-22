package rabbit

import (
	"workers/config"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Broker struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	con  *config.Config
	log  *logging.Logger
}

func NewBroker(con *config.Config, log *logging.Logger) (*Broker, error) {
	conn, err := amqp.Dial(con.Url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &Broker{conn, ch, con, log}, nil
}

func (b *Broker) Init() ([]amqp.Queue, error) {
	inputQs, err := b.initInput()
	if err != nil {
		return nil, err
	}

	return inputQs, nil
}

func (b *Broker) DeInit() {
	if !b.conn.IsClosed() {
		b.conn.Close()
	}
	if !b.ch.IsClosed() {
		b.ch.Close()
	}
}

func (b *Broker) initInputExchange() error {
	for _, exchangeName := range b.con.InputExchangeNames {
		if err := b.exchangeDeclare(exchangeName, "direct"); err != nil {
			return err
		}
	}
	return nil
}

func (b *Broker) initInputQueues() ([]amqp.Queue, error) {
	qs := make([]amqp.Queue, 0, len(b.con.InputQueueNames))

	for _, qName := range b.con.InputQueueNames {
		nameFmt := qName + "-%d"
		q, err := b.queueDeclare(nameFmt)
		if err != nil {
			return nil, err
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
