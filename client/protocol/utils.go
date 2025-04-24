package protocol

import (
	"os"

	"client/config"

	"github.com/op/go-logging"
)

func SendFiles(skt *CsvTransferStream, con *config.Config, log *logging.Logger, files []string) error {
	for id, filename := range files {
		path := con.DataPath + "/" + filename
		fp, err := os.Open(path)
		if err != nil {
			log.Fatalf("Can't open file %s: %v", path, err)
		}
		defer fp.Close()

		log.Debugf("Sending %s", filename)
		if err = skt.SendFile(fp, uint8(id), con.BatchSize); err != nil {
			log.Criticalf("Couldn't send batch of file %s: %v", filename, err)
			skt.Error()
			return err
		}

		if err = skt.Confirm(); err != nil {
			log.Criticalf("Couldn't send confirm fo file %s: %v", filename, err)
		}
	}

	log.Infof("Every file was sent successfully")
	return nil
}
