package protocol

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"analyzer/comms"
	"analyzer/gateway/config"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Server struct {
	lis         *CsvTransferListener
	con         *config.Config
	mailer      *SanitizeMailer
	inputCopies map[int]int
	log         *logging.Logger
	inputQueue  amqp.Queue
}

func NewServer(config *config.Config, log *logging.Logger) (*Server, error) {
	mailer, err := NewSanitizeMailer(config, log)
	if err != nil {
		return nil, err
	}

	inputQ, inputCopies, err := mailer.Init()
	if err != nil {
		return nil, err
	}

	lis, err := Bind(config.Host, config.Port, config.Backlog)
	if err != nil {
		return nil, err
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

	return &Server{
		lis:         lis,
		con:         config,
		mailer:      mailer,
		inputQueue:  inputQ,
		inputCopies: inputCopies,
		log:         log,
	}, nil
}

func (s *Server) acceptNewConn() (*CsvTransferStream, error) {
	s.log.Infof("Waiting for connections...")
	conn, addr, err := s.lis.Accept()
	if err != nil {
		return nil, fmt.Errorf("couldn't accept new connection: %v", err)
	}

	if conn == nil {
		return nil, nil
	}

	s.log.Infof("Got a new connection from %v", addr)
	return conn, nil
}

func (s *Server) clientHandler(conn *CsvTransferStream) error {
	for range 3 {
		fileName, err := conn.Resource()
		if err != nil {
			return err
		}
		s.log.Infof("Receiving %s", fileName)

		for {
			msg, err := conn.Recv()
			if err != nil {
				return err
			}

			if msg.Kind == MSG_EOF {
				s.log.Infof("%s was successfully received", fileName)
				s.sendEof(fileName)
				break

			} else if msg.Kind == MSG_BATCH {
				s.sendBatch(fileName, msg.Data)

			} else if msg.Kind == MSG_ERR {
				s.log.Criticalf("an error was received from the client, exiting...")
				return nil
			} else {
				return fmt.Errorf("an unknown msg kind was received: %d", msg.Kind)
			}
		}
	}

	return nil
}

func (s *Server) sendBatch(fileName string, records [][]byte) error {
	body := make([]byte, 1, 24000)
	body[0] = comms.BATCH
	first := true

	for _, record := range records {
		if !first {
			body = append(body, '\n')
		}

		first = false
		body = append(body, record...)
	}

	return s.mailer.PublishBatch(fileName, body)
}

func (s *Server) sendEof(fileName string) error {
	body := []byte{comms.EOF}
	return s.mailer.PublishEof(fileName, body)
}

func (s *Server) recvResults(conn *CsvTransferStream) error {
	recvChan, err := s.mailer.Consume(s.inputQueue)
	if err != nil {
		return fmt.Errorf("couldn't consume")
	}

	eofsRecv := make(map[int]int, 0)
	for del := range recvChan {
		kind := int(del.Headers["kind"].(int32))
		query := int(del.Headers["query"].(int32))
		body := del.Body

		if kind == comms.BATCH {
			batch, err := comms.DecodeBatch(body)
			if err != nil {
				return fmt.Errorf("failed to decode batch from query %d", query)
			}
			result := batch.ToResult(query)
			conn.Send(result)

		} else if kind == MSG_EOF {
			eofsRecv[query] += 1
			if eofsRecv[query] >= s.inputCopies[query] {
				eof := comms.DecodeEof(body)
				result := eof.ToResult(query)
				conn.Send(result)
				s.log.Infof("Query %d has been successfully processed", query)
			}
		} else {
			s.log.Errorf("Received an unknown data kind %d", kind)
		}

		del.Ack(false)
	}

	return nil
}

func (s *Server) Run() error {
	conn, err := s.acceptNewConn()
	if err != nil {
		return err
	}
	if conn == nil {
		return nil
	}
	defer s.mailer.DeInit()
	defer conn.Close()

	s.lis.Close()
	go func() {
		if err := s.clientHandler(conn); err != nil {
			s.log.Errorf("error while handling client: %v", err)
		}
	}()

	return s.recvResults(conn)
}
