package rabbit

import (
	"fmt"
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

func (b *Broker) Init() ([]amqp.Queue, []string, error) {
	inputQs, err := b.initInput()
	if err != nil {
		return nil, nil, err
	}

	outputQFmts, err := b.initOutput()
	if err != nil {
		return inputQs, nil, err
	}

	return inputQs, outputQFmts, nil
}

func (b *Broker) DeInit() {
	if !b.conn.IsClosed() {
		b.conn.Close()
	}
	if !b.ch.IsClosed() {
		b.ch.Close()
	}
}

func (b *Broker) initInput() ([]amqp.Queue, error) {
	exchangeNames := b.con.InputExchangeNames
	qNames := b.con.InputQueueNames

	qs := make([]amqp.Queue, 0, len(b.con.InputQueueNames))
	for i := range b.con.InputExchangeNames {
		if err := b.exchangeDeclare(exchangeNames[i], "direct"); err != nil {
			return nil, err
		}

		// Build queue name
		nameFmt := qNames[i] + "-%d"
		qName := fmt.Sprintf(nameFmt, b.con.Id)

		q, err := b.queueDeclare(qName)
		if err != nil {
			return nil, err
		}

		if err := b.queueBind(q, q.Name, exchangeNames[i]); err != nil {
			return nil, err
		}

		qs = append(qs, q)
	}

	return qs, nil
}

// Returns the format of output queue names
func (b *Broker) initOutput() ([]string, error) {
	exchangeName := b.con.OutputExchangeName
	qNames := b.con.OutputQueueNames
	qCopies := b.con.OutputCopies

	if err := b.exchangeDeclare(exchangeName, "direct"); err != nil {
		return nil, err
	}

	fmtQNames := make([]string, 0, len(qNames)*len(qCopies))
	for i := range qNames {
		nameFmt := qNames[i] + "-%d"
		fmtQNames = append(fmtQNames, nameFmt)

		for id := range qCopies[i] {
			qName := fmt.Sprintf(nameFmt, id)

			q, err := b.queueDeclare(qName)
			if err != nil {
				return nil, err
			}

			if err := b.queueBind(q, q.Name, exchangeName); err != nil {
				return nil, err
			}
		}
	}

	return fmtQNames, nil
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
