package workers

import (
	"os"
	"os/signal"
	"syscall"
	"workers/config"
	"workers/rabbit"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Worker struct {
	Broker       *rabbit.Broker
	InputQueues  []amqp.Queue
	OutputQueues []amqp.Queue
	SigChan      chan os.Signal
}

func New(con *config.Config) (*Worker, error) {
	broker, err := rabbit.New(con.Url)
	if err != nil {
		return nil, err
	}

	inputQueues, outputQueues, err := broker.Init(con)
	if err != nil {
		return nil, err
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

	return &Worker{
		broker,
		inputQueues,
		outputQueues,
		sigs,
	}, nil
}

func (w *Worker) Close() {
	w.Broker.DeInit()
	close(w.SigChan)
}
