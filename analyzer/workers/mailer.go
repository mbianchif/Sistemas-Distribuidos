package workers

import (
	"strings"

	"analyzer/comms"
	"analyzer/comms/rabbit"
	"analyzer/workers/config"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Mailer struct {
	senders []rabbit.Sender
	broker  *rabbit.Broker
	con     *config.Config
	Log     *logging.Logger
}

func NewMailer(con *config.Config, log *logging.Logger) (*Mailer, error) {
	broker, err := rabbit.NewBroker(con.Url)
	if err != nil {
		return nil, err
	}
	return &Mailer{nil, broker, con, log}, nil
}

func (m *Mailer) Init() ([]amqp.Queue, error) {
	inExchNames := m.con.InputExchangeNames
	inQNames := m.con.InputQueueNames
	outExchName := m.con.OutputExchangeName
	outQNames := m.con.OutputQueueNames
	outCopies := m.con.OutputCopies

	inputQs, outputQFmts, err := m.broker.Init(m.con.Id, inExchNames, inQNames, outExchName, outQNames, outCopies)
	if err != nil {
		return nil, err
	}

	m.senders = m.initSenders(outputQFmts)
	return inputQs, nil
}

func (m *Mailer) initSenders(outputQFmts []string) []rabbit.Sender {
	delTypes := m.con.OutputDeliveryTypes
	outputQCopies := m.con.OutputCopies
	senders := make([]rabbit.Sender, 0, len(delTypes))

	for i := range outputQFmts {
		var sender rabbit.Sender
		if delTypes[i] == "robin" {
			sender = rabbit.NewRobin(m.broker, outputQFmts[i], outputQCopies[i])
		} else {
			parts := strings.Split(delTypes[i], ":")
			key := parts[1]
			sender = rabbit.NewShard(m.broker, outputQFmts[i], key, outputQCopies[i], m.Log)
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

func (m *Mailer) PublishBatch(batch comms.Batch) error {
	for _, sender := range m.senders {
		if err := sender.Batch(batch, m.con.Select); err != nil {
			return err
		}
	}
	return nil
}

func (m *Mailer) PublishBatchWithQuery(batch comms.Batch, query int) error {
	for _, sender := range m.senders {
		if err := sender.BatchWithQuery(batch, m.con.Select, query); err != nil {
			return err
		}
	}
	return nil
}

func (m *Mailer) PublishEof(eof comms.Eof) error {
	for _, sender := range m.senders {
		if err := sender.Eof(eof); err != nil {
			return err
		}
	}
	return nil
}

func (m *Mailer) PublishEofWithQuery(eof comms.Eof, query int) error {
	for _, sender := range m.senders {
		if err := sender.EofWithQuery(eof, query); err != nil {
			return err
		}
	}
	return nil
}

func (m *Mailer) PublishError(erro comms.Error) error {
	for _, sender := range m.senders {
		if err := sender.Error(erro); err != nil {
			return err
		}
	}
	return nil
}
