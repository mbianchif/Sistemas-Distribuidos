package workers

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"analyzer/comms"
	"analyzer/workers/config"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type IWorker interface {
	Batch(int, []byte) bool
	Eof(int, []byte) bool
}

type Worker struct {
	Log        *logging.Logger
	Mailer     *Mailer
	sigChan    chan os.Signal
	inputChans []<-chan amqp.Delivery
	con        *config.Config
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

	inputChans := make([]<-chan amqp.Delivery, 0, len(inputQueues))
	for _, q := range inputQueues {
		ch, err := mailer.Consume(q)
		if err != nil {
			return nil, err
		}
		inputChans = append(inputChans, ch)
	}

	return &Worker{
		Log:        log,
		Mailer:     mailer,
		sigChan:    sigs,
		inputChans: inputChans,
		con:        con,
	}, nil
}

func (base *Worker) Run(w IWorker) error {
	base.Log.Infof("Running...")
	for {
		for _, ch := range base.inputChans {
			exit := false
			for !exit {
				select {
				case <-base.sigChan:
					base.Log.Info("received SIGTERM")
					return nil

				case del, ok := <-ch:
					if !ok {
						return fmt.Errorf("delivery channel was closed unexpectedly")
					}
					defer del.Ack(false)

					kind := int(del.Headers["kind"].(int32))
					client := int(del.Headers["client-id"].(int32))
					body := del.Body

					switch kind {
					case comms.BATCH:
						exit = w.Batch(client, body)
					case comms.EOF:
						exit = w.Eof(client, body)
					default:
						base.Log.Errorf("received an unknown message type %v", kind)
					}
				}
			}
		}
	}
}

func (w *Worker) Close() {
	w.Mailer.DeInit()
	close(w.sigChan)
}
