package protocol

import (
	"analyzer/comms"
	"analyzer/gateway/config"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type tuple struct {
	fileName string
	clientId int
	kind     int
	body     []byte
}

type SyncMailer struct {
	mailer *SanitizeMailer
	ch     chan tuple
	log    *logging.Logger
}

func NewSyncMailer(con *config.Config, log *logging.Logger) (*SyncMailer, error) {
	sanitize, err := NewSanitizeMailer(con, log)
	if err != nil {
		return nil, err
	}

	ch := make(chan tuple)
	return &SyncMailer{mailer: sanitize, ch: ch, log: log}, nil
}

func (s *SyncMailer) Init() error {
	go func() {
		for tup := range s.ch {
			switch tup.kind {
			case comms.BATCH:
				if err := s.mailer.PublishBatch(tup.fileName, tup.clientId, tup.body); err != nil {
					s.log.Errorf("Error publishing batch: %v", err)
				}
			case comms.EOF:
				if err := s.mailer.PublishEof(tup.fileName, tup.clientId, tup.body); err != nil {
					s.log.Errorf("Error publishing EOF: %v", err)
				}
			}
		}
	}()

	return s.mailer.Init()
}

func (s *SyncMailer) Consume() (<-chan amqp.Delivery, error) {
	return s.mailer.Consume()
}

func (s *SyncMailer) PublishBatch(fileName string, clientId int, body []byte) {
	s.ch <- tuple{fileName: fileName, clientId: clientId, kind: comms.BATCH, body: body}
}

func (s *SyncMailer) PublishEof(fileName string, clientId int, body []byte) {
	s.ch <- tuple{fileName: fileName, clientId: clientId, kind: comms.EOF, body: body}
}

func (s *SyncMailer) DeInit() {
	s.mailer.DeInit()
}
