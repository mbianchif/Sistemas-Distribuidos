package main

import (
	"os"

	"client/config"
	"client/protocol"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func main() {
	con, err := config.Create()
	if err != nil {
		log.Fatalf("Couldn't read envars: %v", err)
	}

	skt, err := protocol.Connect(con.GatewayHost, con.GatewayPort)
	if err != nil {
		skt.Close()
		log.Fatalf("Can't connect with gateway: %v", err)
	}
	defer skt.Close()

	files := []string{"movies_metadata.csv", "credits.csv", "ratings.csv"}
	for id, filename := range files {
		path := con.DataPath + "/" + filename
		fp, err := os.Open(path)
		if err != nil {
			log.Criticalf("Can't open file %s: %v", path, err)
			break
		}
		defer fp.Close()

		log.Debugf("Sending file %s", filename)
		if err = skt.SendFile(fp, uint8(id), con.BatchSize); err != nil {
			log.Criticalf("Couldn't send batch of file %s: %v", filename, err)
			break
		}
	}

	// log.Infof("Waiting for results")
	// for range 5 {
	// 	id, err := skt.RecvQueryResult(con.Storage)
	// 	if err != nil {
	// 		log.Errorf("There was an error receiving query result %d: %v", id, err)
	// 		continue
	// 	}

	// 	log.Infof("Query #%d has been successfully received", id)
	// }
}
