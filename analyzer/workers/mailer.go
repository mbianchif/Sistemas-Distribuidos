package workers

import (
	"bytes"
	"fmt"
	"maps"
	"strings"

	"analyzer/comms"
	"analyzer/comms/rabbit"
	"analyzer/workers/config"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Mailer struct {
	senders   []rabbit.Sender
	receivers map[string]*rabbit.Receiver
	broker    *rabbit.Broker
	con       *config.Config
	Log       *logging.Logger
}

func NewMailer(con *config.Config, log *logging.Logger) (*Mailer, error) {
	broker, err := rabbit.NewBroker(con.Url)
	if err != nil {
		return nil, err
	}
	return &Mailer{nil, nil, broker, con, log}, nil
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

	inputCopies := m.con.InputCopies
	m.senders = m.initSenders(outputQFmts)
	m.receivers = m.initReceivers(inputQs, inputCopies)
	return inputQs, nil
}

func (m *Mailer) initSenders(outputQFmts []string) []rabbit.Sender {
	// TODO: check recovery file, else run default behaviour

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

func (m *Mailer) initReceivers(inputQs []amqp.Queue, inputCopies []int) map[string]*rabbit.Receiver {
	// TODO: check recovery file, else run default behaviour

	receivers := make(map[string]*rabbit.Receiver, len(inputQs))

	for i := range inputQs {
		recv := rabbit.NewReceiver(m.broker, inputQs[i], inputCopies[i], m)
		receivers[inputQs[i].Name] = recv
	}

	return receivers
}

func (m *Mailer) DeInit() {
	m.broker.DeInit()
}

func (m *Mailer) Consume(q amqp.Queue) (<-chan comms.Delivery, error) {
	recv, ok := m.receivers[q.Name]
	if !ok {
		return nil, fmt.Errorf("no receivers matches this queue name: %s", q.Name)
	}

	return recv.Consume("")
}

func mergeHeaders(base amqp.Table, headers []amqp.Table) amqp.Table {
	for _, h := range headers {
		maps.Copy(base, h)
	}

	return base
}

func (m *Mailer) PublishBatch(batch comms.Batch, clientId int, headers ...amqp.Table) error {
	baseHeaders := amqp.Table{
		"kind":       comms.BATCH,
		"replica-id": m.con.Id,
		"client-id":  int32(clientId),
	}
	merged := mergeHeaders(baseHeaders, headers)

	for _, sender := range m.senders {
		if err := sender.Batch(batch, m.con.Select, merged); err != nil {
			return err
		}
	}

	return nil
}

func (m *Mailer) PublishEof(eof comms.Eof, clientId int, headers ...amqp.Table) error {
	baseHeaders := amqp.Table{
		"kind":       comms.EOF,
		"replica-id": m.con.Id,
		"client-id":  int32(clientId),
	}
	merged := mergeHeaders(baseHeaders, headers)

	for _, sender := range m.senders {
		if err := sender.Eof(eof, merged); err != nil {
			return err
		}
	}

	return nil
}

func (m *Mailer) PublishFlush(flush comms.Flush, clientId int, headers ...amqp.Table) error {
	baseHeaders := amqp.Table{
		"kind":       comms.FLUSH,
		"replica-id": m.con.Id,
		"client-id":  int32(clientId),
	}
	merged := mergeHeaders(baseHeaders, headers)

	for _, sender := range m.senders {
		if err := sender.Flush(flush, merged); err != nil {
			return err
		}
	}

	return nil
}

func (m *Mailer) Dump(clientId int) error {
	buf := bytes.NewBuffer(nil)

	// 1. Write receivers' data
	for _, receiver := range m.receivers {
		encoded := receiver.Encode(clientId)
		buf.Write(encoded)
		buf.WriteByte('\n')

	}

	// 2. Write senders' data
	for _, sender := range m.senders {
		encoded := sender.Encode(clientId)
		buf.Write(encoded)
		buf.WriteByte('\n')
	}

	// 3. Atomic write
	dirPath := fmt.Sprintf("/mailer/%d", clientId)
	fileName := "state"
	return comms.AtomicWrite(dirPath, fileName, buf.Bytes())
}
