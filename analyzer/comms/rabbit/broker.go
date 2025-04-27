package rabbit

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Broker struct {
	conn               *amqp.Connection
	ch                 *amqp.Channel
	outputExchangeName string
}

func NewBroker(url string) (*Broker, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &Broker{
		conn:               conn,
		ch:                 ch,
		outputExchangeName: "",
	}, nil
}

func (b *Broker) Init(id int, inExchNames []string, inQNames []string, outExchName string, outQNames []string, outCopies []int) ([]amqp.Queue, []string, error) {
	inputQs, err := b.initInput(id, inExchNames, inQNames)
	if err != nil {
		return nil, nil, err
	}

	outputQFmts, err := b.initOutput(outExchName, outQNames, outCopies)
	if err != nil {
		return inputQs, nil, err
	}

	b.outputExchangeName = outExchName
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

func (b *Broker) initInput(id int, exchangeNames []string, qNames []string) ([]amqp.Queue, error) {
	qs := make([]amqp.Queue, 0, len(qNames))
	for i := range exchangeNames {
		if err := b.exchangeDeclare(exchangeNames[i], "direct"); err != nil {
			return nil, err
		}

		// Build queue name
		nameFmt := qNames[i] + "-%d"
		qName := fmt.Sprintf(nameFmt, id)

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
func (b *Broker) initOutput(exchangeName string, qNames []string, qCopies []int) ([]string, error) {
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
		b.outputExchangeName,
		key,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/octet-stream",
			Body:        body,
		})
}
