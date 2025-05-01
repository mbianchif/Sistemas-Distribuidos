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
}

type Worker struct {
	Log         *logging.Logger
	Mailer      *Mailer
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
		Mailer:      mailer,
		sigChan:     sigs,
		inputQueues: inputQueues,
		con:         con,
	}, nil
}

func (base *Worker) Run(w IWorker) error {
	base.Log.Infof("Running...")
	for _, q := range base.inputQueues {
		ch, err := base.Mailer.Consume(q)
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

				kind := int(del.Headers["kind"].(int32))
				body := del.Body

				switch kind {
				case comms.BATCH:
					exit = w.Batch(body)
				case comms.EOF:
					exit = w.Eof(body)
				default:
					base.Log.Errorf("received an unknown message type %v", kind)
				}
				
				del.Ack(false)
			}
		}
	}

	return nil
}

func (w *Worker) Close() {
	w.Mailer.DeInit()
	close(w.sigChan)
}
