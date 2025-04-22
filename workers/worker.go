package workers

import (
	"os"
	"os/signal"
	"syscall"

	"workers/config"
	"workers/protocol"
	"workers/rabbit"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type IWorker interface {
	Batch([]byte) bool
	Eof([]byte) bool
	Error([]byte) bool
}

type Worker struct {
	Mailer      *rabbit.Mailer
	SigChan     chan os.Signal
	inputQueues []amqp.Queue
	Log         *logging.Logger
}

func New(con *config.Config, log *logging.Logger) (*Worker, error) {
	mailer, err := rabbit.NewMailer(con, log)
	if err != nil {
		return nil, err
	}

	inputQueues, err := mailer.Init()
	if err != nil {
		return nil, err
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

	return &Worker{
		mailer,
		sigs,
		inputQueues,
		log,
	}, nil
}

func (base *Worker) Run(w IWorker) error {
	handlers := map[int]func([]byte) bool{
		protocol.BATCH: w.Batch,
		protocol.EOF:   w.Eof,
		protocol.ERROR: w.Error,
	}

	base.Log.Infof("Running...")
	for _, q := range base.inputQueues {
		ch, err := base.Mailer.Consume(q)
		if err != nil {
			return err
		}

		exit := false
		for !exit {
			select {
			case <-base.SigChan:
				base.Log.Info("received SIGTERM")
				return nil

			case del, ok := <-ch:
				if !ok {
					base.Log.Warning("delivery channel was closed unexpectedly")
					exit = true
					break
				}

				kind, data := protocol.ReadDelivery(del)
				handle, ok := handlers[kind]
				if !ok {
					base.Log.Errorf("received an unknown message type %v", kind)
				} else {
					exit = handle(data)
				}

				del.Ack(false)
			}
		}
	}

	return nil
}

func (base *Worker) PublishBatch(batch protocol.Batch) error {
	return base.Mailer.PublishBatch(batch)
}

func (base *Worker) PublishBatchWithQuery(batch protocol.Batch, query int) error {
	return base.Mailer.PublishBatchWithQuery(batch, query)
}

func (base *Worker) PublishEof(eof protocol.Eof) error {
	return base.Mailer.PublishEof(eof)
}

func (base *Worker) PublishError(erro protocol.Error) error {
	return base.Mailer.PublishError(erro)
}

func (w *Worker) Close() {
	w.Mailer.DeInit()
	close(w.SigChan)
}
