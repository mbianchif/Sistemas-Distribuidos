package protocol

import (
	"analyzer/comms"
	"analyzer/gateway/config"

	"github.com/op/go-logging"
)

type tuple struct {
	fileName string
	clientId int
	body     []byte
	kind     int
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
			case comms.FLUSH:
				if err := s.mailer.PublishFlush(tup.clientId, tup.body); err != nil {
					s.log.Errorf("Error publishing flush: %v", err)
				}
			default:
				s.log.Errorf("got an unexpected message kind in sync mailer: %d", tup.kind)
			}
		}
	}()

	return s.mailer.Init()
}

func (s *SyncMailer) Consume() (<-chan comms.Delivery, error) {
	return s.mailer.Consume()
}

func (s *SyncMailer) PublishBatch(fileName string, clientId int, body []byte) {
	s.ch <- tuple{fileName, clientId, body, comms.BATCH}
}

func (s *SyncMailer) PublishEof(fileName string, clientId int, body []byte) {
	s.ch <- tuple{fileName, clientId, body, comms.EOF}
}

func (s *SyncMailer) PublishFlush(clientId int, body []byte) {
	s.ch <- tuple{"", clientId, body, comms.FLUSH}
}

func (s *SyncMailer) DeInit() {
	close(s.ch)
	s.mailer.DeInit()
}
