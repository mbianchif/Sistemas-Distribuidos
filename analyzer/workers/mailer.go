package workers

import (
	"maps"
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

func mergeHeaders(base amqp.Table, headers []amqp.Table) amqp.Table {
	for _, h := range headers {
		maps.Copy(base, h)
	}
	return base
}

func (m *Mailer) PublishBatch(batch comms.Batch, headers ...amqp.Table) error {
	baseHeaders := amqp.Table{
		"kind": comms.BATCH,
	}
	merged := mergeHeaders(baseHeaders, headers)

	for _, sender := range m.senders {
		if err := sender.Batch(batch, m.con.Select, merged); err != nil {
			return err
		}
	}
	return nil
}

func (m *Mailer) PublishEof(eof comms.Eof, headers ...amqp.Table) error {
	baseHeaders := amqp.Table{
		"kind": comms.EOF,
	}
	merged := mergeHeaders(baseHeaders, headers)

	for _, sender := range m.senders {
		if err := sender.Eof(eof, merged); err != nil {
			return err
		}
	}
	return nil
}
