package rabbit

import (
	"workers/protocol"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Mailer struct {
	senders []rabbit.Sender
}

func (m *Mailer) PublishBatch(batch protocol.Batch, filterCols map[string]struct{}) error {
	for _, sender := range m.senders {
		if err := sender.Batch(batch, filterCols); err != nil {
			return err
		}
	}
	return nil
}

func (m *Mailer) PublishEof(eof protocol.Eof) error {
	body := eof.Encode()
	headers := amqp.Table{
		"type": protocol.EOF,
	}

	return m.broadcast(body, headers)
}

func (m *Mailer) PublishError(erro protocol.Error) error {
	body := erro.Encode()
	headers := amqp.Table{
		"type": protocol.ERROR,
	}
	return m.broadcast(body, headers)
}

func (m *Mailer) broadcast(body []byte, headers amqp.Table) error {
	for _, sender := range m.senders {
		if err := sender.Broadcast(body, headers); err != nil {
			return err
		}
	}
	return nil
}


