package protocol

import (
	"analyzer/comms"
	"analyzer/comms/middleware"
	"analyzer/gateway/config"
	"fmt"
	"reflect"
	"sync"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SanitizeMailer struct {
	broker      *middleware.Broker
	con         *config.Config
	senders     []*middleware.SenderRobin
	receivers   []*middleware.Receiver
	filename2Id map[string]int
	log         *logging.Logger
}

func NewSanitizeMailer(con *config.Config, log *logging.Logger) (*SanitizeMailer, error) {
	broker, err := middleware.NewBroker(con.Url)
	if err != nil {
		return nil, err
	}

	return &SanitizeMailer{
		broker:  broker,
		senders: nil,
		con:     con,
		log:     log,
		filename2Id: map[string]int{
			"movies":  0,
			"credits": 1,
			"ratings": 2,
		},
	}, nil
}

func (s *SanitizeMailer) initSenders(outputQFmts []string) []*middleware.SenderRobin {
	outputQCopies := s.con.OutputCopies
	senders := make([]*middleware.SenderRobin, 0, len(outputQCopies))

	for i := range outputQFmts {
		sender := middleware.NewRobin(s.broker, outputQFmts[i], outputQCopies[i])
		senders = append(senders, sender)
	}

	return senders
}

func (s *SanitizeMailer) initReceivers(inputQs []amqp.Queue, inputCopies []int) []*middleware.Receiver {
	receivers := make([]*middleware.Receiver, 0, len(inputQs))
	mu := new(sync.Mutex)

	for i := range inputQs {
		recv := middleware.NewReceiver(s.broker, inputQs[i], inputCopies[i], nil, mu)
		receivers = append(receivers, recv)
	}

	return receivers
}

func (s *SanitizeMailer) Init() error {
	inExchNames := s.con.InputExchangeNames
	inQNames := s.con.InputQueueNames
	outExchName := s.con.OutputExchangeName
	outQNames := s.con.OutputQueueNames
	outCopies := s.con.OutputCopies

	inputQs, outputQFmts, err := s.broker.Init(s.con.Id, inExchNames, inQNames, outExchName, outQNames, outCopies)
	if err != nil {
		return err
	}

	inputCopies := s.con.InputCopies
	s.senders = s.initSenders(outputQFmts)
	s.receivers = s.initReceivers(inputQs, inputCopies)

	return nil
}

func (s *SanitizeMailer) Consume() (<-chan middleware.Delivery, error) {
	out := make(chan middleware.Delivery)

	cases := make([]reflect.SelectCase, len(s.receivers))
	for i, recv := range s.receivers {
		ch, err := recv.Consume("")
		if err != nil {
			return nil, fmt.Errorf("couldn't start consuming: error with chan %d: %v", i, err)
		}

		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		}
	}

	go func() {
		defer close(out)

		for len(cases) > 0 {
			chosen, recv, ok := reflect.Select(cases)
			if !ok {
				cases[chosen] = cases[len(cases)-1]
				cases = cases[:len(cases)-1]
				continue
			}

			out <- recv.Interface().(middleware.Delivery)
		}
	}()

	return out, nil
}

func (s *SanitizeMailer) PublishBatch(fileName string, clientId int, body []byte) error {
	baseHeaders := middleware.Table{
		"kind":       comms.BATCH,
		"replica-id": s.con.Id,
		"client-id":  int32(clientId),
	}
	return s.senders[s.filename2Id[fileName]].Direct(body, baseHeaders)
}

func (s *SanitizeMailer) PublishEof(fileName string, clientId int, body []byte) error {
	baseHeaders := middleware.Table{
		"kind":       comms.EOF,
		"replica-id": s.con.Id,
		"client-id":  int32(clientId),
	}
	return s.senders[s.filename2Id[fileName]].Broadcast(body, baseHeaders)
}

func (s *SanitizeMailer) PublishFlush(clientId int, body []byte) error {
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

func (s *SanitizeMailer) DeInit() {
	s.broker.DeInit()
}
