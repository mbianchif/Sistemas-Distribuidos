package impl

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"workers"
	"workers/protocol"
	"workers/sanitize/config"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Sanitize struct {
	*workers.Worker
	Con     *config.SanitizeConfig
	Handler func(*Sanitize, []string) (map[string]string, error)
}

func New(con *config.SanitizeConfig, log *logging.Logger) (*Sanitize, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}

	handler := map[string]func(*Sanitize, []string) (map[string]string, error){
		"movies":  handleMovie,
		"credits": handleCredit,
		"ratings": handleRating,
	}[con.Handler]

	return &Sanitize{base, con, handler}, nil
}

func (w *Sanitize) Run() error {
	inputQueue := w.InputQueues[0]
	recvChan, err := w.Broker.Consume(inputQueue, "")
	if err != nil {
		return err
	}

	handlers := map[int]func(*Sanitize, amqp.Delivery, []byte) bool{
		protocol.BATCH: handleBatch,
		protocol.EOF:   handleEof,
		protocol.ERROR: handleError,
	}

	w.Log.Infof("Running with handler: %v", w.Con.Handler)
	exit := false
	for !exit {
		select {
		case <-w.SigChan:
			exit = true

		case del := <-recvChan:
			kind, data := protocol.ReadDelivery(del)
			exit = handlers[kind](w, del, data)
		}
	}

	return nil
}

func handleBatch(w *Sanitize, del amqp.Delivery, data []byte) bool {
	reader := csv.NewReader(strings.NewReader(string(del.Body)))
	responseFieldMaps := make([]map[string]string, 0)
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			w.Log.Errorf("failed to decode message: %v", err)
			continue
		}

		responseFieldMap, err := w.Handler(w, line)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			continue
		}

		if responseFieldMap != nil {
			responseFieldMaps = append(responseFieldMaps, responseFieldMap)
		}
	}

	if len(responseFieldMaps) > 0 {
		w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
		body := protocol.NewBatch(responseFieldMaps).Encode(w.Con.Select)
		outQKey := w.Con.OutputQueueKeys[0]
		if err := w.Broker.Publish(w.Con.OutputExchangeName, outQKey, body); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}

	del.Ack(false)
	return false
}

func parseNamesFromJson(field string) (names []string) {
	handlePanic := func() {
		if recover() != nil {
			names = nil
		}
	}
	defer handlePanic()

	type Named struct {
		Name string `json:"name"`
	}

	field = strings.ReplaceAll(field, "'", "\"")

	var values []Named
	if err := json.Unmarshal([]byte(field), &values); err != nil {
		return nil
	}

	names = make([]string, 0, len(values))
	for _, named := range values {
		names = append(names, named.Name)
	}

	return names
}

func isValidRow(fields map[string]string) bool {
	for _, value := range fields {
		if len(value) == 0 {
			return false
		}
	}
	return true
}

func handleMovie(w *Sanitize, line []string) (map[string]string, error) {
	if len(line) != 24 {
		return nil, fmt.Errorf("invalid size for csv register, is %v", len(line))
	}

	genres := parseNamesFromJson(line[3])
	if genres == nil {
		return nil, nil
	}
	prodCountries := parseNamesFromJson(line[13])
	if prodCountries == nil {
		return nil, nil
	}
	spokLangs := parseNamesFromJson(line[17])
	if spokLangs == nil {
		return nil, nil
	}

	fields := map[string]string{
		"id":                   strings.TrimSpace(line[5]),
		"title":                strings.TrimSpace(line[20]),
		"release_date":         strings.TrimSpace(line[14]),
		"overview":             strings.TrimSpace(line[9]),
		"budget":               strings.TrimSpace(line[2]),
		"revenue":              strings.TrimSpace(line[15]),
		"genres":               strings.Join(genres, ","),
		"production_countries": strings.Join(prodCountries, ","),
		"spoken_languages":     strings.Join(spokLangs, ","),
	}

	if !isValidRow(fields) {
		return nil, nil
	}

	return fields, nil
}

func parseTimestamp(timestamp string) (string, error) {
	unixEpoc, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return "", err
	}

	t := time.Unix(unixEpoc, 0)
	return t.Format("2006-01-02 15:04:05"), nil
}

func handleRating(w *Sanitize, line []string) (map[string]string, error) {
	if len(line) != 4 {
		return nil, fmt.Errorf("invalid size for csv register, is %v", len(line))
	}

	timestamp, err := parseTimestamp(line[3])
	if err != nil {
		return nil, err
	}

	fields := map[string]string{
		"movieId":   strings.TrimSpace(line[1]),
		"rating":    strings.TrimSpace(line[2]),
		"timestamp": timestamp,
	}

	if !isValidRow(fields) {
		return nil, nil
	}

	return fields, nil
}

func handleCredit(w *Sanitize, line []string) (map[string]string, error) {
	if len(line) != 3 {
		return nil, fmt.Errorf("invalid size for csv register, is %v", len(line))
	}

	cast := parseNamesFromJson(line[0])
	if cast == nil {
		return nil, nil
	}

	fields := map[string]string{
		"id":   strings.TrimSpace(line[2]),
		"cast": strings.Join(cast, ","),
	}

	if !isValidRow(fields) {
		return nil, nil
	}

	return fields, nil
}

func handleEof(w *Sanitize, del amqp.Delivery, data []byte) bool {
	body := protocol.DecodeEof(data).Encode()
	outQKey := w.Con.OutputQueueKeys[0]
	if err := w.Broker.Publish(w.Con.OutputExchangeName, outQKey, body); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	del.Ack(false)
	return true
}

func handleError(w *Sanitize, del amqp.Delivery, data []byte) bool {
	w.Log.Error("Received an ERROR message kind")
	return true
}
