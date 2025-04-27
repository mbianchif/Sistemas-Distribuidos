package protocol

import (
	"analyzer/comms/rabbit"
	"analyzer/gateway/config"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SanitizeMailer struct {
	broker      *rabbit.Broker
	senders     []*rabbit.SenderRobin
	con         *config.Config
	filename2Id map[string]int
}

func NewSanitizeMailer(con *config.Config, log *logging.Logger) (*SanitizeMailer, error) {
	broker, err := rabbit.NewBroker(con.Url)
	if err != nil {
		return nil, err
	}

	return &SanitizeMailer{
		broker:  broker,
		senders: nil,
		con:     con,
		filename2Id: map[string]int{
			"movies":  0,
			"credits": 1,
			"ratings": 2,
		},
	}, nil
}

func (s *SanitizeMailer) initSenders(outputQFmts []string) []*rabbit.SenderRobin {
	outputQCopies := s.con.OutputCopies
	senders := make([]*rabbit.SenderRobin, 0, len(outputQCopies))

	for i := range outputQFmts {
		sender := rabbit.NewRobin(s.broker, outputQFmts[i], outputQCopies[i])
		senders = append(senders, sender)
	}

	return senders
}

func (s *SanitizeMailer) Init() (amqp.Queue, map[int]int, error) {
	inExchName := s.con.InputExchangeName
	inQName := s.con.InputQueueName
	outExchName := s.con.OutputExchangeName
	outQNames := s.con.OutputQueueNames
	outCopies := s.con.OutputCopies

	inputQs, outputQFmts, err := s.broker.Init(s.con.Id, []string{inExchName}, []string{inQName}, outExchName, outQNames, outCopies)
	if err != nil {
		return amqp.Queue{}, nil, err
	}

	s.senders = s.initSenders(outputQFmts)

	inputCopies := make(map[int]int, 0)
	for i, copies := range s.con.InputCopies {
		inputCopies[i+1] = copies
	}

	return inputQs[0], inputCopies, nil
}

func (s *SanitizeMailer) Consume(q amqp.Queue) (<-chan amqp.Delivery, error) {
	return s.broker.Consume(q, "")
}

func (s *SanitizeMailer) PublishBatch(fileName string, body []byte) error {
	return s.senders[s.filename2Id[fileName]].Direct(body)
}

func (s *SanitizeMailer) PublishEof(fileName string, body []byte) error {
	return s.senders[s.filename2Id[fileName]].Broadcast(body)
}

func (s *SanitizeMailer) DeInit() {
	s.broker.DeInit()
}
