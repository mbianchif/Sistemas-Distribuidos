package workers

import (
	"bufio"
	"bytes"
	"fmt"
	"maps"
	"os"
	"strconv"
	"strings"
	"sync"

	"analyzer/comms"
	"analyzer/comms/middleware"
	"analyzer/workers/config"

	"github.com/op/go-logging"
)

const (
	PERSISTANCE_DIRNAME  = "mailer"
	PERSISTANCE_FILENAME = "state"
)

type Mailer struct {
	con *config.Config
	log *logging.Logger

	// Need Init
	broker    *middleware.Broker
	senders   []middleware.Sender
	receivers map[string]*middleware.Receiver
	inputQs   []middleware.Queue
}

func NewMailer(con *config.Config, log *logging.Logger) (*Mailer, error) {
	broker, err := middleware.NewBroker(con.Url)
	if err != nil {
		return nil, err
	}

	return &Mailer{
		broker:    broker,
		con:       con,
		log:       log,
		senders:   nil,
		receivers: nil,
		inputQs:   nil,
	}, nil
}

func (m *Mailer) tryRecover(inputQs []middleware.Queue) (map[string]*middleware.Receiver, []middleware.Sender) {
	m.inputQs = inputQs
	receivers := m.initReceivers(inputQs, m.con.InputCopies)
	senders := m.initSenders()

	dirPath := fmt.Sprintf("/%s", PERSISTANCE_DIRNAME)
	mailerDir, err := os.ReadDir(dirPath)
	if err != nil {
		return receivers, senders
	}

	for _, clientDir := range mailerDir {
		clientIdStr := clientDir.Name()
		clientId, err := strconv.Atoi(clientIdStr)
		if err != nil {
			m.log.Errorf("Failed to parse clientId for client %s: ", clientIdStr, err)
			continue
		}

		dirPath := fmt.Sprintf("/%s/%d", PERSISTANCE_DIRNAME, clientId)
		statePath := fmt.Sprintf("%s/%s", dirPath, PERSISTANCE_FILENAME)
		fp, err := os.Open(statePath)
		if err != nil {
			m.log.Errorf("Failed to recover client %s mailer's state: %v", clientId, err)
			continue
		}

		reader := bufio.NewReader(fp)
		sendIdx := 0
		for {
			lineBytes, err := reader.ReadBytes('\n')
			if err != nil {
				break
			}
			line := strings.TrimSpace(string(lineBytes))

			if strings.HasPrefix(line, "recv") {
				qName, eofs, flushes, seqs, err := middleware.DecodeLineRecv(line)
				if err != nil {
					m.log.Errorf("Failed to decode line for client-%d's receiver: %v", clientId, err)
					continue
				}
				receivers[qName].SetState(clientId, eofs, flushes, seqs)
			} else if strings.HasPrefix(line, "robin") {
				cur, seqs, err := middleware.DecodeLineRobin(line)
				if err != nil {
					m.log.Errorf("failed to decode line for client-%d's robin sender: %v", clientId, err)
					continue
				}
				senders[sendIdx].(*middleware.SenderRobin).SetState(clientId, cur, seqs)
				sendIdx++
			} else if strings.HasPrefix(line, "shard") {
				seqs, err := middleware.DecodeLineShard(line)
				if err != nil {
					m.log.Errorf("failed to decode line for client-%d's shard sender: %v", clientId, err)
					continue
				}
				senders[sendIdx].(*middleware.SenderShard).SetState(clientId, seqs)
				sendIdx++
			} else {
				m.log.Errorf("Unknown line format for client-%d's mailer state: %s", clientId, line)
				continue
			}
		}
	}

	m.log.Infof("Mailer recovered successfully")
	return receivers, senders
}

func (m *Mailer) Init() ([]middleware.Queue, error) {
	inExchNames := m.con.InputExchangeNames
	inQNames := m.con.InputQueueNames
	outExchName := m.con.OutputExchangeName
	outQNames := m.con.OutputQueueNames
	outCopies := m.con.OutputCopies

	inputQs, err := m.broker.Init(m.con.Id, inExchNames, inQNames, outExchName, outQNames, outCopies)
	if err != nil {
		return nil, err
	}

	m.receivers, m.senders = m.tryRecover(inputQs)
	return inputQs, nil
}

func (m *Mailer) initSenders() []middleware.Sender {
	delTypes := m.con.OutputDeliveryTypes
	outputQNames := m.con.OutputQueueNames
	outputQCopies := m.con.OutputCopies
	senders := make([]middleware.Sender, 0, len(delTypes))

	for i, name := range outputQNames {
		qNameFmt := name + "-%d"

		var sender middleware.Sender
		if delTypes[i] == "robin" {
			sender = middleware.NewRobin(m.broker, qNameFmt, outputQCopies[i])
		} else {
			parts := strings.Split(delTypes[i], ":")
			keys := strings.Split(parts[1], ";")
			sender = middleware.NewShard(m.broker, qNameFmt, keys, outputQCopies[i], m.log)
		}

		senders = append(senders, sender)
	}

	return senders
}

func (m *Mailer) initReceivers(inputQs []middleware.Queue, inputCopies []int) map[string]*middleware.Receiver {
	receivers := make(map[string]*middleware.Receiver, len(inputQs))
	mu := new(sync.Mutex)

	for i := range inputQs {
		recv := middleware.NewReceiver(m.broker, inputQs[i], inputCopies[i], m, mu)
		receivers[inputQs[i].Name] = recv
	}

	return receivers
}

func (m *Mailer) DeInit() {
	m.broker.DeInit()
}

func (m *Mailer) Consume(q middleware.Queue) (<-chan middleware.Delivery, error) {
	recv, ok := m.receivers[q.Name]
	if !ok {
		return nil, fmt.Errorf("no receivers matches this queue name: %s", q.Name)
	}

	return recv.Consume("")
}

func mergeHeaders(base middleware.Table, headers []middleware.Table) middleware.Table {
	for _, h := range headers {
		maps.Copy(base, h)
	}

	return base
}

func (m *Mailer) PublishBatch(batch comms.Batch, clientId int, headers ...middleware.Table) error {
	baseHeaders := middleware.Table{
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

func (m *Mailer) PublishEof(eof comms.Eof, clientId int, headers ...middleware.Table) error {
	baseHeaders := middleware.Table{
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

func (m *Mailer) PublishFlush(flush comms.Flush, clientId int, headers ...middleware.Table) error {
	baseHeaders := middleware.Table{
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

func (m *Mailer) PublishPurge(purge comms.Purge, headers ...middleware.Table) error {
	baseHeaders := middleware.Table{
		"kind":       comms.PURGE,
		"replica-id": m.con.Id,
		"client-id":  int32(-1),
	}
	merged := mergeHeaders(baseHeaders, headers)

	for _, sender := range m.senders {
		if err := sender.Purge(purge, merged); err != nil {
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
	dirPath := fmt.Sprintf("%s/%d", PERSISTANCE_DIRNAME, clientId)
	return comms.AtomicWrite(dirPath, PERSISTANCE_FILENAME, buf.Bytes())
}

func (m *Mailer) Flush(clientId int) error {
	dirPath := fmt.Sprintf("/%s/%d", PERSISTANCE_DIRNAME, clientId)
	return os.RemoveAll(dirPath)
}

func (m *Mailer) Purge() error {
	for _, q := range m.inputQs {
		if err := m.broker.Purge(q); err != nil {
			return fmt.Errorf("failed to purge queue %s: %v", q.Name, err)
		}

	}

	dirPath := fmt.Sprintf("/%s", PERSISTANCE_DIRNAME)
	return os.RemoveAll(dirPath)
}
