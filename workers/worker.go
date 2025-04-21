package workers

import (
	"os"
	"os/signal"
	"syscall"
	"workers/config"
	"workers/protocol"
	"workers/rabbit"

	"github.com/op/go-logging"
)

type IWorker interface {
	Batch(string, []byte) bool
	Eof(string, []byte) bool
	Error(string, []byte) bool
}

type Worker struct {
	Broker  *rabbit.Broker
	SigChan chan os.Signal
	Log     *logging.Logger
}

func New(con *config.Config, log *logging.Logger) (*Worker, error) {
	broker, err := rabbit.New(con)
	if err != nil {
		return nil, err
	}

	if err := broker.Init(); err != nil {
		return nil, err
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

	return &Worker{
		broker,
		sigs,
		log,
	}, nil
}

func (base *Worker) Run(w IWorker) error {
	ch, err := base.Broker.Consume("")
	if err != nil {
		return err
	}

	handlers := map[int]func(string, []byte) bool {
		protocol.BATCH: w.Batch,
		protocol.EOF:   w.Eof,
		protocol.ERROR: w.Error,
	}

	base.Log.Infof("Running...")
	exit := false
	for !exit {
		select {
		case <-base.SigChan:
			base.Log.Info("received SIGTERM")
			exit = true

		case del, ok := <-ch:
			if !ok {
				base.Log.Warning("delivery channel was closed unexpectedly")
				exit = true
				break
			}

			kind, producer, data := protocol.ReadDelivery(del)
			handle, ok := handlers[kind]
			if !ok {
				base.Log.Errorf("received an unknown message type %v", kind)
			} else {
				exit = handle(producer, data)
			}

			del.Ack(false)
		}
	}

	return nil
}

func (w *Worker) Close() {
	w.Broker.DeInit()
	close(w.SigChan)
}
