package workers

import (
	"workers/config"
	"workers/rabbit"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Worker struct {
	Broker       *rabbit.Broker
	InputQueues  []amqp.Queue
	OutputQueues []amqp.Queue
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

	return &Worker{
		broker,
		inputQueues,
		outputQueues,
	}, nil
}

func (w *Worker) Close() {
	w.Broker.DeInit()
}
