package rabbit

import (
	"workers/config"
	"workers/protocol"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Mailer struct {
	senders []Sender
	broker  *Broker
}

func NewMailer(con *config.Config, log *logging.Logger) (*Mailer, error) {
	broker, err := NewBroker(con, log)
	if err != nil {
		return nil, err
	}

	return &Mailer{nil, broker}, inputQs, nil
}

func (m *Mailer) Init() ([][]amqp.Queue, error) {
	inputQs, outputQs, err := m.broker.Init()
	if err != nil {
		return nil, err
	}

	m.senders = m.initSenders(outputQs)
	return inputQs, nil
}

func (m *Mailer) initSenders(outputQs []amqp.Queue) []Sender {
	delTypes := m.broker.con.OutputDeliveryTypes
	senders := make([]Sender, 0, len(delTypes))

	return senders
}

func (m *Mailer) DeInit() {
	m.broker.DeInit()
}

func (m *Mailer) Consume(q amqp.Queue) (<-chan amqp.Delivery, error) {
	return m.broker.Consume(q, "")
}

func (m *Mailer) PublishBatch(batch protocol.Batch, filterCols map[string]struct{}) error {
	for _, sender := range m.senders {
		if err := sender.Batch(batch, filterCols); err != nil {
			return err
		}
	}
	return nil
}

func (m *Mailer) PublishEof(eof protocol.Eof) error {
	body := eof.Encode()
	headers := amqp.Table{
		"type": protocol.EOF,
	}

	return m.broadcast(body, headers)
}

func (m *Mailer) PublishError(erro protocol.Error) error {
	body := erro.Encode()
	headers := amqp.Table{
		"type": protocol.ERROR,
	}
	return m.broadcast(body, headers)
}

func (m *Mailer) broadcast(body []byte, headers amqp.Table) error {
	for _, sender := range m.senders {
		if err := sender.Broadcast(body, headers); err != nil {
			return err
		}
	}
	return nil
}
