package protocol

import (
	"analyzer/comms/middleware"
	"analyzer/gateway/config"
	"fmt"
	"reflect"
	"sync"

	"github.com/op/go-logging"
)

type RxMailer struct {
	con *config.Config
	log *logging.Logger

	// Need Init
	broker      *middleware.Broker
	receivers   []*middleware.Receiver
	inputQueues []middleware.Queue
}

func NewRxMailer(con *config.Config, log *logging.Logger) (*RxMailer, error) {
	broker, err := middleware.NewBroker(con.Url)
	if err != nil {
		return nil, err
	}

	return &RxMailer{
		broker: broker,
		con:    con,
		log:    log,
	}, nil
}

func (m *RxMailer) initReceivers(inputQs []middleware.Queue, inputCopies []int) []*middleware.Receiver {
	receivers := make([]*middleware.Receiver, 0, len(inputQs))
	mu := new(sync.Mutex)

	for i := range inputQs {
		recv := middleware.NewReceiver(m.broker, inputQs[i], inputCopies[i], m, mu)
		receivers = append(receivers, recv)
	}

	return receivers
}

func (m *RxMailer) Init() error {
	id := m.con.Id
	inExchNames := m.con.InputExchangeNames
	inQNames := m.con.InputQueueNames
	inputCopies := m.con.InputCopies

	inputQs, err := m.broker.InitInput(id, inExchNames, inQNames)
	if err != nil {
		return err
	}

	m.inputQueues = inputQs
	m.receivers = m.initReceivers(inputQs, inputCopies)
	return nil
}

func (m *RxMailer) Consume() (<-chan middleware.Delivery, error) {
	out := make(chan middleware.Delivery)

	cases := make([]reflect.SelectCase, len(m.receivers))
	for i, recv := range m.receivers {
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

func (m *RxMailer) Purge() error {
	for _, q := range m.inputQueues {
		if err := m.broker.Purge(q); err != nil {
			return err
		}
	}

	return nil
}

func (m *RxMailer) Dump(_ int) error {
	return nil
}

func (m *RxMailer) DeInit() {
	m.broker.DeInit()
}
