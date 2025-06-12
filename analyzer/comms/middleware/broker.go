package middleware

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Broker struct {
	rxConn             *amqp.Connection
	txConn             *amqp.Connection
	rxCh               *amqp.Channel
	txCh               *amqp.Channel
	outputExchangeName string
}

// Creates a `Broker` and sets the connections to it
func NewBroker(url string) (*Broker, error) {
	rxConn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	txConn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	rxCh, err := rxConn.Channel()
	if err != nil {
		return nil, err
	}
	// if err := rxCh.Qos(4096, 0, false); err != nil {
	// 	return nil, err
	// }

	txCh, err := txConn.Channel()
	if err != nil {
		return nil, err
	}
	if err := txCh.Confirm(false); err != nil {
		return nil, err
	}

	return &Broker{
		rxConn:             rxConn,
		txConn:             txConn,
		rxCh:               rxCh,
		txCh:               txCh,
		outputExchangeName: "",
	}, nil
}

// Creates all the broker's infrastructure the node needs to communicate with the broker
func (b *Broker) Init(id int, inExchNames []string, inQNames []string, outExchName string, outQNames []string, outCopies []int) ([]Queue, error) {
	inputQs, err := b.InitInput(id, inExchNames, inQNames)
	if err != nil {
		return nil, err
	}

	if err := b.InitOutput(outExchName, outQNames, outCopies); err != nil {
		return nil, err
	}

	return inputQs, nil
}

// Releases the used external resources
func (b *Broker) DeInit() {
	if !b.rxCh.IsClosed() {
		b.rxCh.Close()
	}
	if !b.txCh.IsClosed() {
		b.txCh.Close()
	}

	if !b.rxConn.IsClosed() {
		b.rxConn.Close()
	}
	if !b.txConn.IsClosed() {
		b.txConn.Close()
	}
}

// Creates the broker's infrastructure for the input of this particular node
func (b *Broker) InitInput(id int, exchangeNames []string, qNames []string) ([]Queue, error) {
	qs := make([]Queue, 0)
	for i := range exchangeNames {
		if err := b.exchangeDeclare(exchangeNames[i], "direct"); err != nil {
			return nil, err
		}

		qName := fmt.Sprintf("%s-%d", qNames[i], id)
		q, err := b.queueDeclare(qName)
		if err != nil {
			return nil, err
		}

		qs = append(qs, q)
		if err := b.queueBind(q.Name, q.Name, exchangeNames[i]); err != nil {
			return nil, err
		}
	}

	return qs, nil
}

// Creates the broker's infrastructure for the input of this particular node
func (b *Broker) InitOutput(exchangeName string, qNames []string, qCopies []int) error {
	if err := b.exchangeDeclare(exchangeName, "direct"); err != nil {
		return err
	}

	for i := range qNames {
		nameFmt := qNames[i] + "-%d"

		for id := range qCopies[i] {
			qName := fmt.Sprintf(nameFmt, id)

			q, err := b.queueDeclare(qName)
			if err != nil {
				return err
			}

			if err := b.queueBind(q.Name, q.Name, exchangeName); err != nil {
				return err
			}
		}
	}

	b.outputExchangeName = exchangeName
	return nil
}

// Declares an exchange with the given name and kind
func (b *Broker) exchangeDeclare(name string, kind string) error {
	return b.txCh.ExchangeDeclare(
		name,
		kind,
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
}

// Declares a queue ith the given name and returns it
func (b *Broker) queueDeclare(name string) (Queue, error) {
	_, err := b.txCh.QueueDeclare(
		name,
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)

	if err != nil {
		return Queue{}, err
	}

	return NewQueue(name), nil
}

// Declares a queue binding from the given queue to the given exchange using `key`
func (b *Broker) queueBind(qName, key, exchangeName string) error {
	return b.txCh.QueueBind(
		qName,
		key,
		exchangeName,
		false, // no-wait
		nil,   // args
	)
}

// Returns a channel used to consume deliveries sent by the broker server
func (b *Broker) Consume(q Queue, consumer string) (<-chan amqp.Delivery, error) {
	return b.rxCh.Consume(
		q.Name,
		consumer,
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
}

// Publishes a message to the broker server using the given key, will wait for server confirmation
func (b *Broker) Publish(key string, body []byte, headers amqp.Table) error {
	waiter, err := b.txCh.PublishWithDeferredConfirm(
		b.outputExchangeName,
		key,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/octet-stream",
			Body:        body,
			Headers:     headers,
		})

	if err != nil {
		return err
	}

	if !waiter.Wait() {
		return fmt.Errorf("failed to wait on publishment")
	}

	return nil
}

// Clears the messages of a given queue
func (b *Broker) Purge(q Queue) error {
	_, err := b.rxCh.QueuePurge(q.Name, false)
	return err
}
