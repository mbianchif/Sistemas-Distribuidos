package workers

import (
	"os"
	"os/signal"
	"syscall"

	"analyzer/comms"
	"analyzer/workers/config"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type IWorker interface {
	Batch([]byte) bool
	Eof([]byte) bool
	Error([]byte) bool
}

type Worker struct {
	Log         *logging.Logger
	mailer      *Mailer
	sigChan     chan os.Signal
	inputQueues []amqp.Queue
	eofsRecv    int
	con         *config.Config
}

func New(con *config.Config, log *logging.Logger) (*Worker, error) {
	mailer, err := NewMailer(con, log)
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
		Log:         log,
		mailer:      mailer,
		sigChan:     sigs,
		inputQueues: inputQueues,
		con:         con,
	}, nil
}

func (base *Worker) Run(w IWorker) error {

	base.Log.Infof("Running...")
	for i, q := range base.inputQueues {
		handlers := map[int]func([]byte) bool{
			comms.BATCH: w.Batch,
			comms.ERROR: w.Error,
			comms.EOF: func(data []byte) bool {
				base.eofsRecv += 1

				if base.eofsRecv < base.con.InputCopies[i] {
					return false
				}

				base.eofsRecv = 0
				return w.Eof(data)
			},
		}

		ch, err := base.mailer.Consume(q)
		if err != nil {
			return err
		}

		exit := false
		for !exit {
			select {
			case <-base.sigChan:
				base.Log.Info("received SIGTERM")
				return nil

			case del, ok := <-ch:
				if !ok {
					base.Log.Warning("delivery channel was closed unexpectedly")
					exit = true
					break
				}

				kind, data := comms.ReadDelivery(del)
				handle, ok := handlers[kind]
				if !ok {
					base.Log.Errorf("received an unknown message type %v", kind)
				} else {
					exit = handle(data)
				}
			}
		}
	}

	return nil
}

func (base *Worker) PublishBatch(batch comms.Batch) error {
	return base.mailer.PublishBatch(batch)
}

func (base *Worker) PublishBatchWithQuery(batch comms.Batch, query int) error {
	return base.mailer.PublishBatchWithQuery(batch, query)
}

func (base *Worker) PublishEof(eof comms.Eof) error {
	return base.mailer.PublishEof(eof)
}

func (base *Worker) PublishEofWithQuery(eof comms.Eof, query int) error {
	return base.mailer.PublishEofWithQuery(eof, query)
}

func (base *Worker) PublishError(erro comms.Error) error {
	return base.mailer.PublishError(erro)
}

func (w *Worker) Close() {
	w.mailer.DeInit()
	close(w.sigChan)
}
