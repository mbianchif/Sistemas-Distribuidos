package protocol

import (
	"analyzer/comms"
	"analyzer/comms/middleware"
	"analyzer/gateway/config"

	"github.com/op/go-logging"
)

type TxMailer struct {
	con config.Config
	log *logging.Logger

	// Need Init
	broker      *middleware.Broker
	senders     []*middleware.SenderRobin
	filename2Id map[string]int
}

func NewTxMailer(con config.Config, log *logging.Logger) (*TxMailer, error) {
	broker, err := middleware.NewBroker(con.Url)
	if err != nil {
		return nil, err
	}

	return &TxMailer{
		con:    con,
		log:    log,
		broker: broker,
		filename2Id: map[string]int{
			"movies":  0,
			"credits": 1,
			"ratings": 2,
		},
	}, nil
}

func (s *TxMailer) initSenders() []*middleware.SenderRobin {
	outputQNames := s.con.OutputQueueNames
	outputQCopies := s.con.OutputCopies

	senders := make([]*middleware.SenderRobin, 0, len(outputQNames))
	for i, name := range outputQNames {
		qNameFmt := name + "-%d"
		sender := middleware.NewRobin(s.broker, qNameFmt, outputQCopies[i])
		senders = append(senders, sender)
	}

	return senders
}

func (s *TxMailer) Init() error {
	outExchName := s.con.OutputExchangeName
	outQNames := s.con.OutputQueueNames
	outCopies := s.con.OutputCopies

	err := s.broker.InitOutput(outExchName, outQNames, outCopies)
	if err != nil {
		return err
	}

	s.senders = s.initSenders()
	return nil
}

func (s *TxMailer) PublishBatch(fileName string, clientId int, body []byte) error {
	baseHeaders := middleware.Table{
		"kind":       comms.BATCH,
		"replica-id": s.con.Id,
		"client-id":  int32(clientId),
	}
	return s.senders[s.filename2Id[fileName]].Direct(body, baseHeaders)
}

func (s *TxMailer) PublishEof(fileName string, clientId int, body []byte) error {
	baseHeaders := middleware.Table{
		"kind":       comms.EOF,
		"replica-id": s.con.Id,
		"client-id":  int32(clientId),
	}
	return s.senders[s.filename2Id[fileName]].Broadcast(body, baseHeaders)
}

func (s *TxMailer) PublishFlush(clientId int, body []byte) error {
	baseHeaders := middleware.Table{
		"kind":       comms.FLUSH,
		"replica-id": s.con.Id,
		"client-id":  int32(clientId),
	}

	for _, sender := range s.senders {
		if err := sender.Broadcast(body, baseHeaders); err != nil {
			return err
		}
	}

	return nil
}

func (s *TxMailer) PublishPurge(body []byte) error {
	baseHeaders := middleware.Table{
		"kind":       comms.PURGE,
		"replica-id": s.con.Id,
		"client-id":  int32(-1),
	}

	for _, sender := range s.senders {
		if err := sender.Broadcast(body, baseHeaders); err != nil {
			return err
		}
	}

	return nil
}

func (s *TxMailer) DeInit() {
	s.broker.DeInit()
}
