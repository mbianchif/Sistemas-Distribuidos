package rabbit

import (
	"strings"

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

	return &Mailer{nil, broker}, nil
}

func (m *Mailer) Init() ([]amqp.Queue, error) {
	inputQs, outputQFmts, err := m.broker.Init()
	if err != nil {
		return nil, err
	}

	m.senders = m.initSenders(outputQFmts)
	m.broker.log.Infof("inputQs: %v", inputQs)
	m.broker.log.Infof("outputQs: %v", m.senders)
	return inputQs, nil
}

func (m *Mailer) initSenders(outputQFmts []string) []Sender {
	delTypes := m.broker.con.OutputDeliveryTypes
	qCopies := m.broker.con.OutputCopies
	senders := make([]Sender, 0, len(delTypes))

	for i := range outputQFmts {
		var sender Sender
		if delTypes[i] == "robin" {
			sender = NewRobin(m.broker, outputQFmts[i], qCopies[i], m.broker.log)
		} else {
			parts := strings.Split(delTypes[i], ":")
			key := parts[1]
			sender = NewShard(m.broker, outputQFmts[i], key, qCopies[i], m.broker.log)
		}

		senders = append(senders, sender)
	}

	return senders
}

func (m *Mailer) DeInit() {
	m.broker.DeInit()
}

func (m *Mailer) Consume(q amqp.Queue) (<-chan amqp.Delivery, error) {
	return m.broker.Consume(q, "")
}

func (m *Mailer) PublishBatch(batch protocol.Batch) error {
	for _, sender := range m.senders {
		if err := sender.Batch(batch, m.broker.con.Select); err != nil {
			return err
		}
	}
	return nil
}

func (m *Mailer) PublishBatchWithQuery(batch protocol.Batch, query int) error {
	for _, sender := range m.senders {
		if err := sender.BatchWithQuery(batch, m.broker.con.Select, query); err != nil {
			return err
		}
	}
	return nil
}

func (m *Mailer) PublishEof(eof protocol.Eof) error {
	for _, sender := range m.senders {
		if err := sender.Eof(eof); err != nil {
			return err
		}
	}
	return nil
}

func (m *Mailer) PublishError(erro protocol.Error) error {
	for _, sender := range m.senders {
		if err := sender.Error(erro); err != nil {
			return err
		}
	}
	return nil
}
