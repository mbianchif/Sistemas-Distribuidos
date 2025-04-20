package impl

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"workers"
	"workers/protocol"
	"workers/sanitize/config"

	"github.com/op/go-logging"
)

type Sanitize struct {
	*workers.Worker
}

func New(con *config.SanitizeConfig) (*Sanitize, error) {
	base, err := workers.New(con.Config)
	if err != nil {
		return nil, err
	}
	return &Sanitize{base}, nil
}

func (w *Sanitize) Run(con *config.SanitizeConfig, log *logging.Logger) error {
	inputQueue := w.InputQueues[0]
	recvChan, err := w.Broker.Consume(inputQueue, "")
	if err != nil {
		return err
	}

	handler := map[string]func(*Sanitize, []string) (map[string]string, error){
		"movies":  handleMovie,
		"credits": handleCredit,
		"ratings": handleRating,
	}[con.Handler]

	log.Infof("Running with handler: %v", con.Handler)
	exit := false
	for !exit {
		select {
		case <-w.SigChan:
			exit = true

		case msg := <-recvChan:
			reader := csv.NewReader(strings.NewReader(string(msg.Body)))
			line, err := reader.Read()
			if err != nil {
				log.Errorf("failed to decode message: %v", err)
				msg.Nack(false, false)
				continue
			}

			responseFieldMap, err := handler(w, line)
			if err != nil {
				log.Errorf("failed to handle message: %v", err)
				msg.Nack(false, false)
				continue
			}

			if responseFieldMap != nil {
				log.Debugf("fieldMap: %v", responseFieldMap)
				body := protocol.Encode(responseFieldMap, con.Select)
				outQKey := con.OutputQueueKeys[0]
				if err := w.Broker.Publish(con.OutputExchangeName, outQKey, body); err != nil {
					log.Errorf("failed to publish message: %v", err)
				}
			}

			msg.Ack(false)
		}
	}

	return nil
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
